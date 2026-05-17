use std::{collections::HashMap, sync::Arc, time::Instant};

use tokio::sync::{Mutex, RwLock};
use tracing::debug;

use crate::{bambu::PrinterStatus, mqtt::MqttDeviceState};

use super::{trimmed, ThumbnailStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TaskKey(String);

#[derive(Debug, Clone)]
struct ThumbnailEntry {
    task: TaskKey,
    status: ThumbnailStatus,
    retry_after: Option<Instant>,
}

pub(super) struct ThumbnailCache {
    entries: RwLock<HashMap<String, ThumbnailEntry>>,
    fetch_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl ThumbnailCache {
    pub(super) fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            fetch_locks: Mutex::new(HashMap::new()),
        }
    }

    pub(super) async fn status(&self, device_id: &str) -> ThumbnailStatus {
        let cache = self.entries.read().await;
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

    pub(super) async fn store(
        &self,
        device_id: &str,
        task: TaskKey,
        status: ThumbnailStatus,
        retry_after: Option<Instant>,
    ) {
        self.entries.write().await.insert(
            device_id.to_owned(),
            ThumbnailEntry {
                task,
                status,
                retry_after,
            },
        );
    }

    pub(super) async fn clear(&self, device_id: &str) {
        self.entries.write().await.remove(device_id);
    }

    pub(super) async fn matches(&self, device_id: &str, task: &TaskKey) -> bool {
        let cache = self.entries.read().await;
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

    pub(super) async fn fetch_lock(&self, device_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.fetch_locks.lock().await;
        locks
            .entry(device_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    #[cfg(test)]
    async fn insert_for_test(
        &self,
        device_id: &str,
        task: TaskKey,
        status: ThumbnailStatus,
        retry_delay: Option<std::time::Duration>,
    ) {
        self.store(
            device_id,
            task,
            status,
            retry_delay.map(|delay| Instant::now() + delay),
        )
        .await;
    }
}

impl TaskKey {
    pub(super) fn from_state(state: &MqttDeviceState) -> Option<Self> {
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
    use std::time::Duration;

    use crate::{
        bambu::PrinterStatus,
        mqtt::{MqttConnectionStatus, MqttDeviceConnection, MqttDeviceState},
    };

    use super::{TaskKey, ThumbnailCache};
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
        let cache = ThumbnailCache::new();
        let task = TaskKey("task".to_owned());
        cache
            .insert_for_test(
                "printer-a",
                task.clone(),
                ThumbnailStatus::Missing("missing".to_owned()),
                Some(Duration::from_secs(30)),
            )
            .await;

        assert!(cache.matches("printer-a", &task).await);
    }
}
