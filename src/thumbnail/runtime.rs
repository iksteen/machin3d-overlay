use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context, Result};
use tracing::{debug, warn};

use crate::{
    bambu::PrinterStatus, cloud::CloudSession, devices::DeviceRegistry, mqtt::MqttRuntime,
    service::ShutdownReceiver,
};

use super::{
    cache::{TaskKey, ThumbnailCache},
    cloud, error_chain, local, ThumbnailStatus,
};

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
    cache: ThumbnailCache,
}

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
                cache: ThumbnailCache::new(),
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
        Ok(self.inner.cache.status(&device_id).await)
    }

    pub(crate) async fn watch_task_changes(&self, mut shutdown: ShutdownReceiver) {
        let mut changes = self.inner.mqtt.subscribe();
        self.refresh_changed_devices().await;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                received = changes.recv() => {
                    if received.is_err() {
                        changes = self.inner.mqtt.subscribe();
                    }
                }
            }
            self.refresh_changed_devices().await;
        }
    }

    async fn refresh_changed_devices(&self) {
        let states = self.inner.mqtt.live_states().await;
        for device in self.inner.registry.devices() {
            let device_id = device.id.as_str();
            let Some(state) = states.get(device_id) else {
                self.inner.cache.clear(device_id).await;
                continue;
            };
            let Some(task) = TaskKey::from_state(state) else {
                self.inner.cache.clear(device_id).await;
                continue;
            };
            if self.inner.cache.matches(device_id, &task).await {
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
            self.inner.cache.clear(device_id).await;
            return Ok(());
        };
        let Some(task) = TaskKey::from_state(state) else {
            self.inner.cache.clear(device_id).await;
            return Ok(());
        };
        if self.inner.cache.matches(device_id, &task).await {
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
        let fetch_lock = self.inner.cache.fetch_lock(device_id).await;
        let _guard = fetch_lock.lock().await;

        if self.inner.cache.matches(device_id, &task).await {
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

        self.inner
            .cache
            .store(device_id, task, status, retry_after)
            .await;
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
}
