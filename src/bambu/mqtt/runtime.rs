//! Bambu MQTT runtime: tracks per-broker connection state, accumulates Bambu
//! `PrinterStatus` reports for the (still Bambu-only) thumbnail service, and
//! republishes vendor-neutral [`PrinterReport`] / [`DeviceConnection`] into the
//! shared [`LiveStateStore`].
//!
//! Each broker connect/disconnect fans out per-device connection updates to the
//! store so a snapshot freshness check is the same shape for every backend:
//! a device is fresh iff its `connection.status == Connected`.

use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use crate::{
    bambu::{printer_status_to_live, PrinterStatus},
    live::{ConnectionStatus, DeviceConnection, LiveStateStore},
};

use super::MqttDeviceState;

#[derive(Clone)]
pub struct MqttRuntime {
    inner: Arc<RwLock<MqttState>>,
    store: LiveStateStore,
}

#[derive(Default)]
struct MqttState {
    revision: u64,
    devices: HashMap<String, StoredDeviceState>,
    connections: HashMap<String, MqttConnectionState>,
}

#[derive(Default)]
struct StoredDeviceState {
    report: Option<MqttReportState>,
}

struct MqttReportState {
    report: PrinterStatus,
    last_report_at: DateTime<Utc>,
}

#[derive(Default)]
struct MqttConnectionState {
    status: MqttTransportStatus,
    error: Option<String>,
    device_ids: Vec<String>,
}

#[derive(Default, Clone, Copy)]
enum MqttTransportStatus {
    #[default]
    Disconnected,
    Connecting,
    Connected,
}

/// Bambu-specific snapshot consumed by the thumbnail service: it needs the
/// raw `PrinterStatus` to drive cloud-task lookup and 3MF download.
#[derive(Debug, Clone)]
pub(crate) struct MqttSnapshot {
    pub(crate) revision: u64,
    pub(crate) devices: HashMap<String, MqttDeviceState>,
}

impl MqttRuntime {
    pub fn new(store: LiveStateStore) -> Self {
        Self {
            inner: Arc::new(RwLock::new(MqttState::default())),
            store,
        }
    }

    pub(crate) async fn snapshot(&self) -> MqttSnapshot {
        let state = self.inner.read().await;
        let devices = state
            .devices
            .iter()
            .filter_map(|(device_id, device)| {
                let report = device.report.as_ref()?;
                Some((
                    device_id.clone(),
                    bambu_device_state(device_id, report, &state.connections),
                ))
            })
            .collect();
        MqttSnapshot {
            revision: state.revision,
            devices,
        }
    }

    pub(crate) async fn register_connection(
        &self,
        key: impl Into<String>,
        device_ids: Vec<String>,
    ) {
        let key = key.into();
        let device_ids = normalized_device_ids(device_ids);

        let (dropped_devices, transferred) = self
            .mutate_state(|state| {
                let previous_device_ids = state
                    .connections
                    .get(&key)
                    .map(|connection| connection.device_ids.clone())
                    .unwrap_or_default();
                remove_devices_from_other_connections(&mut state.connections, &key, &device_ids);
                state.connections.entry(key.clone()).or_default().device_ids = device_ids.clone();
                remove_unregistered_reports(state, previous_device_ids.clone());
                let dropped = dropped_device_ids(state, previous_device_ids);
                let transferred = device_ids
                    .iter()
                    .map(|device_id| (device_id.clone(), broker_connection(state, &key)))
                    .collect::<Vec<_>>();
                (dropped, transferred)
            })
            .await;
        for device_id in dropped_devices {
            self.store.remove_device(&device_id).await;
        }
        for (device_id, connection) in transferred {
            self.store
                .set_device_connection(&device_id, connection)
                .await;
        }
    }

    pub(crate) async fn set_connection_connecting(&self, key: impl Into<String>) {
        let key = key.into();
        let device_ids = self
            .mutate_state(|state| {
                let connection = state.connections.entry(key.clone()).or_default();
                connection.status = MqttTransportStatus::Connecting;
                connection.error = None;
                connection.device_ids.clone()
            })
            .await;
        for device_id in device_ids {
            self.store
                .set_device_connection(
                    &device_id,
                    DeviceConnection {
                        key: Some(key.clone()),
                        status: ConnectionStatus::Connecting,
                        error: None,
                    },
                )
                .await;
        }
    }

    pub(crate) async fn set_connection_connected(&self, key: impl Into<String>) {
        let key = key.into();
        let device_ids = self
            .mutate_state(|state| {
                let connection = state.connections.entry(key.clone()).or_default();
                connection.status = MqttTransportStatus::Connected;
                connection.error = None;
                connection.device_ids.clone()
            })
            .await;
        // The broker is up but no fresh per-device report has arrived yet;
        // surface as `Connecting` until each device actually reports.
        for device_id in device_ids {
            self.store
                .set_device_connection(
                    &device_id,
                    DeviceConnection {
                        key: Some(key.clone()),
                        status: ConnectionStatus::Connecting,
                        error: None,
                    },
                )
                .await;
        }
    }

    pub(crate) async fn set_connection_disconnected(&self, key: impl Into<String>) {
        let key = key.into();
        let device_ids = self
            .mutate_state(|state| {
                let connection = state.connections.entry(key.clone()).or_default();
                connection.status = MqttTransportStatus::Disconnected;
                let device_ids = connection.device_ids.clone();
                clear_reports_for_devices(state, &device_ids);
                device_ids
            })
            .await;
        for device_id in device_ids {
            self.store
                .set_device_connection(
                    &device_id,
                    DeviceConnection {
                        key: Some(key.clone()),
                        status: ConnectionStatus::Disconnected,
                        error: None,
                    },
                )
                .await;
        }
    }

    pub(crate) async fn set_connection_error(
        &self,
        key: impl Into<String>,
        error: impl Into<String>,
    ) {
        let key = key.into();
        let error = error.into();
        let device_ids = self
            .mutate_state(|state| {
                let connection = state.connections.entry(key.clone()).or_default();
                connection.status = MqttTransportStatus::Disconnected;
                connection.error = Some(error.clone());
                let device_ids = connection.device_ids.clone();
                clear_reports_for_devices(state, &device_ids);
                device_ids
            })
            .await;
        for device_id in device_ids {
            self.store
                .set_device_connection(
                    &device_id,
                    DeviceConnection {
                        key: Some(key.clone()),
                        status: ConnectionStatus::Disconnected,
                        error: Some(error.clone()),
                    },
                )
                .await;
        }
    }

    pub(crate) async fn merge_report(&self, device_id: &str, report: PrinterStatus) {
        let merged = self
            .mutate_state(|state| {
                let now = Utc::now();
                let device = state.devices.entry(device_id.to_owned()).or_default();
                if let Some(report_state) = &mut device.report {
                    report_state.report.merge(report);
                    report_state.last_report_at = now;
                    Some(report_state.report.clone())
                } else {
                    let report_state = device.report.insert(MqttReportState {
                        report,
                        last_report_at: now,
                    });
                    Some(report_state.report.clone())
                }
            })
            .await;
        if let Some(merged) = merged {
            let connection_key = self.device_connection_key(device_id).await;
            self.store
                .publish_report(
                    device_id,
                    printer_status_to_live(&merged),
                    DeviceConnection {
                        key: connection_key,
                        status: ConnectionStatus::Connected,
                        error: None,
                    },
                )
                .await;
        }
    }

    async fn device_connection_key(&self, device_id: &str) -> Option<String> {
        let state = self.inner.read().await;
        device_connections(&state.connections)
            .get(device_id)
            .cloned()
    }

    async fn mutate_state<R>(&self, mutation: impl FnOnce(&mut MqttState) -> R) -> R {
        let mut state = self.inner.write().await;
        let result = mutation(&mut state);
        state.revision = state.revision.saturating_add(1);
        result
    }
}

fn broker_connection(state: &MqttState, key: &str) -> DeviceConnection {
    let connection = state.connections.get(key);
    let status = match connection.map(|connection| connection.status) {
        Some(MqttTransportStatus::Connected) => ConnectionStatus::Connecting,
        Some(MqttTransportStatus::Connecting) => ConnectionStatus::Connecting,
        Some(MqttTransportStatus::Disconnected) | None => ConnectionStatus::Disconnected,
    };
    DeviceConnection {
        key: Some(key.to_owned()),
        status,
        error: connection.and_then(|connection| connection.error.clone()),
    }
}

fn bambu_device_state(
    device_id: &str,
    report: &MqttReportState,
    connections: &HashMap<String, MqttConnectionState>,
) -> MqttDeviceState {
    let connection = device_connection_for_report(device_id, report.last_report_at, connections);
    MqttDeviceState::from_snapshot(
        report.report.clone(),
        Some(report.last_report_at),
        connection,
    )
}

fn device_connection_for_report(
    device_id: &str,
    _last_report_at: DateTime<Utc>,
    connections: &HashMap<String, MqttConnectionState>,
) -> DeviceConnection {
    if let Some((key, connection)) = connections
        .iter()
        .find(|(_, connection)| connection.device_ids.iter().any(|id| id == device_id))
    {
        let status = match connection.status {
            MqttTransportStatus::Disconnected => ConnectionStatus::Disconnected,
            MqttTransportStatus::Connecting => ConnectionStatus::Connecting,
            MqttTransportStatus::Connected => ConnectionStatus::Connected,
        };
        DeviceConnection {
            key: Some(key.clone()),
            status,
            error: connection.error.clone(),
        }
    } else {
        DeviceConnection {
            key: None,
            status: ConnectionStatus::Disconnected,
            error: Some("MQTT connection has not been registered".to_owned()),
        }
    }
}

fn clear_reports_for_devices(state: &mut MqttState, device_ids: &[String]) {
    for device_id in device_ids {
        if let Some(device) = state.devices.get_mut(device_id) {
            device.report = None;
        }
    }
}

fn device_connections(
    connections: &HashMap<String, MqttConnectionState>,
) -> HashMap<String, String> {
    let mut devices = HashMap::new();
    for (key, connection) in connections {
        for device_id in &connection.device_ids {
            devices.insert(device_id.clone(), key.clone());
        }
    }
    devices
}

fn normalized_device_ids(device_ids: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::new();
    for device_id in device_ids {
        let device_id = device_id.trim();
        if !device_id.is_empty() && !normalized.iter().any(|known| known == device_id) {
            normalized.push(device_id.to_owned());
        }
    }
    normalized
}

fn remove_devices_from_other_connections(
    connections: &mut HashMap<String, MqttConnectionState>,
    owning_key: &str,
    device_ids: &[String],
) {
    for (key, connection) in connections {
        if key == owning_key {
            continue;
        }
        connection
            .device_ids
            .retain(|device_id| !device_ids.iter().any(|owned| owned == device_id));
    }
}

fn remove_unregistered_reports(state: &mut MqttState, device_ids: Vec<String>) {
    let registered = device_connections(&state.connections);
    for device_id in device_ids {
        if !registered.contains_key(&device_id) {
            state.devices.remove(&device_id);
        }
    }
}

fn dropped_device_ids(state: &MqttState, previous_device_ids: Vec<String>) -> Vec<String> {
    let registered = device_connections(&state.connections);
    previous_device_ids
        .into_iter()
        .filter(|device_id| !registered.contains_key(device_id))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        bambu::PrinterStatus,
        live::{ConnectionStatus, LiveStateStore},
    };

    use super::MqttRuntime;

    fn runtime() -> (MqttRuntime, LiveStateStore) {
        let store = LiveStateStore::new();
        let runtime = MqttRuntime::new(store.clone());
        (runtime, store)
    }

    #[tokio::test]
    async fn register_connection_notifies_subscribers() {
        let (runtime, store) = runtime();
        let mut changes = store.subscribe();

        runtime
            .register_connection("printer-a", vec!["printer-a".to_owned()])
            .await;

        // register_connection only mutates broker tracking; nothing fanned out to the store yet.
        runtime.set_connection_connecting("printer-a").await;

        tokio::time::timeout(Duration::from_secs(1), changes.recv())
            .await
            .expect("registration should notify subscribers")
            .expect("change channel should stay open");
    }

    #[tokio::test]
    async fn snapshot_marks_reports_fresh_only_after_a_report_arrives() {
        let (runtime, store) = runtime();
        runtime
            .register_connection("printer-a", vec!["printer-a".to_owned()])
            .await;
        runtime.set_connection_connecting("printer-a").await;

        let snapshot = store.snapshot().await;
        let connection = snapshot.connections.get("printer-a").unwrap();
        assert_eq!(connection.status, ConnectionStatus::Connecting);
        assert!(!snapshot.devices.contains_key("printer-a"));

        runtime.set_connection_connected("printer-a").await;

        let snapshot = store.snapshot().await;
        let connection = snapshot.connections.get("printer-a").unwrap();
        assert_eq!(connection.status, ConnectionStatus::Connecting);
        assert!(!snapshot.devices.contains_key("printer-a"));

        runtime
            .merge_report(
                "printer-a",
                PrinterStatus {
                    status: Some("RUNNING".to_owned()),
                    ..PrinterStatus::default()
                },
            )
            .await;

        let snapshot = store.snapshot().await;
        let state = snapshot.devices.get("printer-a").unwrap();
        let connection = snapshot.connections.get("printer-a").unwrap();
        assert!(state.is_fresh());
        assert!(state.is_active_task());
        assert_eq!(connection.status, ConnectionStatus::Connected);
        assert!(state.last_report_at.is_some());
        assert_eq!(state.connection.key.as_deref(), Some("printer-a"));

        runtime.set_connection_disconnected("printer-a").await;

        let snapshot = store.snapshot().await;
        let connection = snapshot.connections.get("printer-a").unwrap();
        assert!(!snapshot.devices.contains_key("printer-a"));
        assert_eq!(connection.status, ConnectionStatus::Disconnected);
        assert!(!snapshot.status.any_connected);
    }

    #[tokio::test]
    async fn report_is_cleared_on_disconnect_and_replaced_after_reconnect() {
        let (runtime, store) = runtime();
        runtime
            .register_connection("printer-a", vec!["printer-a".to_owned()])
            .await;
        runtime.set_connection_connected("printer-a").await;
        runtime
            .merge_report(
                "printer-a",
                PrinterStatus {
                    task_name: Some("old".to_owned()),
                    ..PrinterStatus::default()
                },
            )
            .await;

        runtime.set_connection_disconnected("printer-a").await;

        let disconnected = store.snapshot().await;
        assert!(!disconnected.devices.contains_key("printer-a"));

        runtime.set_connection_connected("printer-a").await;
        runtime
            .merge_report(
                "printer-a",
                PrinterStatus {
                    task_name: Some("new".to_owned()),
                    ..PrinterStatus::default()
                },
            )
            .await;

        let snapshot = store.snapshot().await;
        let state = snapshot.devices.get("printer-a").unwrap();
        assert_eq!(state.report.task_name.as_deref(), Some("new"));
    }

    #[tokio::test]
    async fn connection_device_ids_are_the_membership_authority() {
        let (runtime, store) = runtime();
        runtime
            .register_connection("cloud", vec!["printer-a".to_owned()])
            .await;
        runtime.set_connection_connected("cloud").await;
        runtime
            .merge_report(
                "printer-a",
                PrinterStatus {
                    task_name: Some("cloud report".to_owned()),
                    ..PrinterStatus::default()
                },
            )
            .await;

        runtime
            .register_connection("local", vec!["printer-a".to_owned()])
            .await;

        let snapshot = store.snapshot().await;
        assert_eq!(
            snapshot
                .connections
                .get("printer-a")
                .and_then(|connection| connection.key.as_deref()),
            Some("local")
        );
    }

    #[tokio::test]
    async fn reregistering_connection_drops_reports_for_removed_devices() {
        let (runtime, store) = runtime();
        runtime
            .register_connection("cloud", vec!["printer-a".to_owned()])
            .await;
        runtime.set_connection_connected("cloud").await;
        runtime
            .merge_report(
                "printer-a",
                PrinterStatus {
                    task_name: Some("old".to_owned()),
                    ..PrinterStatus::default()
                },
            )
            .await;

        runtime
            .register_connection("cloud", vec!["printer-b".to_owned()])
            .await;

        let snapshot = store.snapshot().await;
        assert!(!snapshot.devices.contains_key("printer-a"));
        assert!(!snapshot.connections.contains_key("printer-a"));
    }

    #[tokio::test]
    async fn snapshot_revision_increments_once_per_state_mutation() {
        let (runtime, _store) = runtime();
        assert_eq!(runtime.snapshot().await.revision, 0);

        runtime
            .register_connection("printer-a", vec!["printer-a".to_owned()])
            .await;
        assert_eq!(runtime.snapshot().await.revision, 1);

        runtime.set_connection_connected("printer-a").await;
        assert_eq!(runtime.snapshot().await.revision, 2);

        runtime
            .merge_report(
                "printer-a",
                PrinterStatus {
                    status: Some("RUNNING".to_owned()),
                    ..PrinterStatus::default()
                },
            )
            .await;
        assert_eq!(runtime.snapshot().await.revision, 3);

        runtime.set_connection_disconnected("printer-a").await;
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.revision, 4);
        assert!(!snapshot.devices.contains_key("printer-a"));
    }
}
