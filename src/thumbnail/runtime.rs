use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context, Result};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, warn};

use crate::{
    bambu::PrinterStatus,
    cloud::CloudSession,
    devices::DeviceRegistry,
    mqtt::{MqttDeviceState, MqttRuntime},
};

use super::{cloud, error_chain, local, trimmed, ThumbnailStatus};

const LOADING_RETRY_DELAY: Duration = Duration::from_secs(2);
const MISSING_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(crate) struct ThumbnailRuntime {
    inner: Arc<ThumbnailInner>,
}

struct ThumbnailInner {
    mqtt: MqttRuntime,
    cloud: Option<CloudSession>,
    registry: DeviceRegistry,
    cache: RwLock<HashMap<String, ThumbnailEntry>>,
    fetch_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

#[derive(Debug, Clone)]
struct ThumbnailEntry {
    task: TaskKey,
    status: ThumbnailStatus,
    retry_after: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskKey(String);

impl ThumbnailRuntime {
    pub(crate) fn new(
        mqtt: MqttRuntime,
        cloud: Option<CloudSession>,
        registry: DeviceRegistry,
    ) -> Self {
        Self {
            inner: Arc::new(ThumbnailInner {
                mqtt,
                cloud,
                registry,
                cache: RwLock::new(HashMap::new()),
                fetch_locks: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub(crate) async fn thumbnail(
        &self,
        requested_device_id: Option<&str>,
        _requested_task: Option<&str>,
    ) -> Result<ThumbnailStatus> {
        let Some(device_id) = self.select_device_id(requested_device_id).await? else {
            return Ok(ThumbnailStatus::Missing("no device selected".to_owned()));
        };

        self.refresh_device(&device_id).await?;
        Ok(self.cached_status(&device_id).await)
    }

    pub(crate) async fn watch_task_changes(&self) {
        let mut changes = self.inner.mqtt.subscribe();
        self.refresh_changed_devices().await;
        loop {
            if changes.recv().await.is_err() {
                changes = self.inner.mqtt.subscribe();
            }
            self.refresh_changed_devices().await;
        }
    }

    async fn refresh_changed_devices(&self) {
        let states = self.inner.mqtt.live_states().await;
        for device in self.inner.registry.devices() {
            let device_id = device.id.as_str();
            let Some(state) = states.get(device_id) else {
                self.clear_device(device_id).await;
                continue;
            };
            let Some(task) = TaskKey::from_state(state) else {
                self.clear_device(device_id).await;
                continue;
            };
            if self.cache_matches(device_id, &task).await {
                continue;
            }
            if let Err(error) = self.fetch_and_cache(device_id, &state.report, task).await {
                warn!(
                    device_id,
                    error = %error_chain(&error),
                    "failed to refresh print thumbnail"
                );
            }
        }
    }

    async fn refresh_device(&self, device_id: &str) -> Result<()> {
        let states = self.inner.mqtt.live_states().await;
        let Some(state) = states.get(device_id) else {
            self.clear_device(device_id).await;
            return Ok(());
        };
        let Some(task) = TaskKey::from_state(state) else {
            self.clear_device(device_id).await;
            return Ok(());
        };
        if self.cache_matches(device_id, &task).await {
            return Ok(());
        }
        self.fetch_and_cache(device_id, &state.report, task).await
    }

    async fn fetch_and_cache(
        &self,
        device_id: &str,
        report: &PrinterStatus,
        task: TaskKey,
    ) -> Result<()> {
        let fetch_lock = self.fetch_lock(device_id).await;
        let _guard = fetch_lock.lock().await;

        if self.cache_matches(device_id, &task).await {
            return Ok(());
        }

        let (status, retry_after) = match self.fetch_thumbnail(device_id, report).await {
            Ok(ThumbnailStatus::Ready(image)) => {
                debug!(device_id, "cached print thumbnail");
                (ThumbnailStatus::Ready(image), None)
            }
            Ok(ThumbnailStatus::Loading(message)) => {
                debug!(device_id, message, "print thumbnail is not ready yet");
                (
                    ThumbnailStatus::Loading(message),
                    Some(Instant::now() + LOADING_RETRY_DELAY),
                )
            }
            Ok(ThumbnailStatus::Missing(message)) => (
                ThumbnailStatus::Missing(message),
                Some(Instant::now() + MISSING_RETRY_DELAY),
            ),
            Err(error) => {
                let message = error_chain(&error);
                warn!(
                    device_id,
                    error = %message,
                    "print thumbnail is unavailable"
                );
                (
                    ThumbnailStatus::Missing(message),
                    Some(Instant::now() + MISSING_RETRY_DELAY),
                )
            }
        };

        self.inner.cache.write().await.insert(
            device_id.to_owned(),
            ThumbnailEntry {
                task,
                status,
                retry_after,
            },
        );
        Ok(())
    }

    async fn fetch_thumbnail(
        &self,
        device_id: &str,
        report: &PrinterStatus,
    ) -> Result<ThumbnailStatus> {
        let entry = self
            .inner
            .registry
            .get(device_id)
            .with_context(|| format!("device `{device_id}` is not known"))?;

        if let Some(local) = entry.local() {
            return local::fetch_thumbnail(device_id, local, report).await;
        }
        if entry.has_cloud_mqtt() {
            return cloud::fetch_thumbnail(self.inner.cloud.as_ref(), device_id, report)
                .await
                .map(ThumbnailStatus::Ready);
        }

        anyhow::bail!("device `{device_id}` has no thumbnail data source")
    }

    async fn select_device_id(&self, requested_device_id: Option<&str>) -> Result<Option<String>> {
        let requested_device_id = requested_device_id
            .map(str::trim)
            .filter(|device_id| !device_id.is_empty());
        if let Some(device_id) = requested_device_id {
            ensure!(
                self.inner.registry.get(device_id).is_some(),
                "device `{device_id}` is not known"
            );
            return Ok(Some(device_id.to_owned()));
        }

        Ok(self
            .inner
            .registry
            .first()
            .map(|entry| entry.id().to_owned()))
    }

    async fn cached_status(&self, device_id: &str) -> ThumbnailStatus {
        let cache = self.inner.cache.read().await;
        match cache.get(device_id).map(|entry| &entry.status) {
            Some(status @ ThumbnailStatus::Ready(_)) => status.clone(),
            Some(status @ ThumbnailStatus::Loading(error)) => {
                debug!(device_id, error, "thumbnail is loading");
                status.clone()
            }
            Some(status @ ThumbnailStatus::Missing(error)) => {
                debug!(device_id, error, "thumbnail is unavailable");
                status.clone()
            }
            None => ThumbnailStatus::Missing("thumbnail is not available".to_owned()),
        }
    }

    async fn cache_matches(&self, device_id: &str, task: &TaskKey) -> bool {
        let cache = self.inner.cache.read().await;
        let Some(entry) = cache.get(device_id) else {
            return false;
        };
        if entry.task != *task {
            return false;
        }
        match entry.status {
            ThumbnailStatus::Ready(_) => true,
            ThumbnailStatus::Loading(_) | ThumbnailStatus::Missing(_) => entry
                .retry_after
                .is_some_and(|retry_after| retry_after > Instant::now()),
        }
    }

    async fn clear_device(&self, device_id: &str) {
        self.inner.cache.write().await.remove(device_id);
    }

    async fn fetch_lock(&self, device_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.inner.fetch_locks.lock().await;
        locks
            .entry(device_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

impl TaskKey {
    fn from_state(state: &MqttDeviceState) -> Option<Self> {
        state
            .is_active_task()
            .then(|| Self::from_report(&state.report))
            .flatten()
    }

    fn from_report(report: &PrinterStatus) -> Option<Self> {
        let task_id = trimmed(report.task_id.as_deref());
        let filename = trimmed(report.filename.as_deref());
        let task_name = trimmed(report.task_name.as_deref());
        if task_id.is_none() && filename.is_none() && task_name.is_none() {
            return None;
        }

        Some(Self(format!(
            "{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}",
            task_id.unwrap_or_default(),
            filename.unwrap_or_default(),
            task_name.unwrap_or_default(),
            trimmed(report.start_time.as_deref()).unwrap_or_default(),
            trimmed(report.print_type.as_deref()).unwrap_or_default()
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::{
        bambu::{CloudDevice, PrinterStatus},
        devices::DeviceRegistry,
        mqtt::{MqttConnectionStatus, MqttDeviceConnection, MqttDeviceState, MqttRuntime},
    };

    use super::{TaskKey, ThumbnailEntry, ThumbnailRuntime};
    use crate::thumbnail::ThumbnailStatus;

    #[test]
    fn task_key_tracks_the_active_print_identity() {
        let report = PrinterStatus {
            task_id: Some("task-1".to_owned()),
            filename: Some("cube.3mf".to_owned()),
            task_name: Some("Cube".to_owned()),
            start_time: Some("2026-01-01".to_owned()),
            ..PrinterStatus::default()
        };

        assert!(TaskKey::from_report(&report).is_some());
        assert_eq!(TaskKey::from_report(&PrinterStatus::default()), None);
    }

    #[test]
    fn task_key_ignores_inactive_live_state() {
        let state = MqttDeviceState::from_report(PrinterStatus {
            status: Some("FINISH".to_owned()),
            task_id: Some("task-1".to_owned()),
            filename: Some("cube.3mf".to_owned()),
            task_name: Some("Cube".to_owned()),
            ..PrinterStatus::default()
        });

        assert_eq!(TaskKey::from_state(&state), None);
    }

    #[test]
    fn task_key_ignores_stale_live_state() {
        let state = MqttDeviceState::from_snapshot(
            PrinterStatus {
                status: Some("RUNNING".to_owned()),
                task_id: Some("task-1".to_owned()),
                filename: Some("cube.3mf".to_owned()),
                task_name: Some("Cube".to_owned()),
                ..PrinterStatus::default()
            },
            None,
            MqttDeviceConnection {
                key: Some("printer-a".to_owned()),
                status: MqttConnectionStatus::Disconnected,
                error: Some("disconnected".to_owned()),
            },
        );

        assert_eq!(TaskKey::from_state(&state), None);
    }

    #[tokio::test]
    async fn missing_thumbnail_cache_throttles_until_retry_time() {
        let runtime = ThumbnailRuntime::new(
            MqttRuntime::new(),
            None,
            DeviceRegistry::new(
                vec![CloudDevice {
                    id: Some("printer-a".to_owned()),
                    ..CloudDevice::default()
                }],
                Vec::new(),
            ),
        );
        let task = TaskKey("task".to_owned());
        runtime.inner.cache.write().await.insert(
            "printer-a".to_owned(),
            ThumbnailEntry {
                task: task.clone(),
                status: ThumbnailStatus::Missing("missing".to_owned()),
                retry_after: Some(Instant::now() + Duration::from_secs(30)),
            },
        );

        assert!(runtime.cache_matches("printer-a", &task).await);
    }
}
