use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::{broadcast, RwLock};

use crate::bambu::PrinterStatus;

use super::{MqttConnectionStatus, MqttDeviceConnection, MqttDeviceState, PrintActivity};

#[derive(Clone)]
pub struct MqttRuntime {
    inner: Arc<RwLock<MqttState>>,
    changes: broadcast::Sender<()>,
}

#[derive(Default)]
struct MqttState {
    devices: HashMap<String, DeviceLiveState>,
    connections: HashMap<String, MqttConnectionState>,
    connected: bool,
    error: Option<String>,
    updated_at: Option<String>,
}

#[derive(Default)]
struct DeviceLiveState {
    connection_key: Option<String>,
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

#[derive(Default)]
enum MqttTransportStatus {
    #[default]
    Disconnected,
    Connecting,
    Connected {
        connected_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MqttStatusPayload {
    pub connected: bool,
    pub error: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct MqttSnapshot {
    pub(crate) devices: HashMap<String, MqttDeviceState>,
    pub(crate) connections: HashMap<String, MqttDeviceConnection>,
    pub(crate) status: MqttStatusPayload,
}

impl MqttRuntime {
    pub fn new() -> Self {
        let (changes, _) = broadcast::channel(128);
        Self {
            inner: Arc::new(RwLock::new(MqttState::default())),
            changes,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.changes.subscribe()
    }

    pub(crate) async fn snapshot(&self) -> MqttSnapshot {
        let state = self.inner.read().await;
        let connections = state
            .devices
            .iter()
            .filter_map(|(device_id, device)| {
                let connection_key = device.connection_key.clone()?;
                Some((
                    device_id.clone(),
                    connection_state(Some(connection_key), &state.connections),
                ))
            })
            .collect::<HashMap<_, _>>();
        let devices = state
            .devices
            .iter()
            .filter_map(|(device_id, device)| {
                let report = device.report.as_ref()?;
                Some((
                    device_id.clone(),
                    device_state(device, report, &state.connections),
                ))
            })
            .collect();

        MqttSnapshot {
            devices,
            connections,
            status: status_payload(&state),
        }
    }

    pub(crate) async fn live_states(&self) -> HashMap<String, MqttDeviceState> {
        self.snapshot().await.devices
    }

    pub async fn status(&self) -> MqttStatusPayload {
        let state = self.inner.read().await;
        status_payload(&state)
    }

    pub(crate) async fn register_connection(
        &self,
        key: impl Into<String>,
        device_ids: Vec<String>,
    ) {
        let key = key.into();
        let mut state = self.inner.write().await;
        let previous_device_ids = state
            .connections
            .entry(key.clone())
            .or_default()
            .device_ids
            .clone();
        let mut remove_devices = Vec::new();
        for device_id in previous_device_ids {
            if let Some(device) = state.devices.get_mut(&device_id) {
                if device.connection_key.as_deref() == Some(key.as_str()) {
                    device.connection_key = None;
                    if device.report.is_none() {
                        remove_devices.push(device_id);
                    }
                }
            }
        }
        for device_id in remove_devices {
            state.devices.remove(&device_id);
        }
        for device_id in &device_ids {
            state
                .devices
                .entry(device_id.clone())
                .or_default()
                .connection_key = Some(key.clone());
        }
        state.connections.entry(key).or_default().device_ids = device_ids;
    }

    pub(crate) async fn set_connection_connecting(&self, key: impl Into<String>) {
        let mut state = self.inner.write().await;
        let connection = state.connections.entry(key.into()).or_default();
        connection.status = MqttTransportStatus::Connecting;
        connection.error = None;
        refresh_status(&mut state);
        drop(state);
        self.notify();
    }

    pub(crate) async fn set_connection_connected(&self, key: impl Into<String>) {
        let mut state = self.inner.write().await;
        let connection = state.connections.entry(key.into()).or_default();
        connection.status = MqttTransportStatus::Connected {
            connected_at: Utc::now(),
        };
        connection.error = None;
        refresh_status(&mut state);
        drop(state);
        self.notify();
    }

    pub(crate) async fn set_connection_disconnected(&self, key: impl Into<String>) {
        let mut state = self.inner.write().await;
        let connection = state.connections.entry(key.into()).or_default();
        connection.status = MqttTransportStatus::Disconnected;
        refresh_status(&mut state);
        drop(state);
        self.notify();
    }

    pub(crate) async fn set_connection_error(
        &self,
        key: impl Into<String>,
        error: impl Into<String>,
    ) {
        let mut state = self.inner.write().await;
        let connection = state.connections.entry(key.into()).or_default();
        connection.status = MqttTransportStatus::Disconnected;
        connection.error = Some(error.into());
        refresh_status(&mut state);
        drop(state);
        self.notify();
    }

    pub(crate) async fn merge_report(&self, device_id: &str, report: PrinterStatus) {
        let mut state = self.inner.write().await;
        let now = Utc::now();
        let device = state.devices.entry(device_id.to_owned()).or_default();
        let activity = if let Some(report_state) = &mut device.report {
            report_state.report.merge(report);
            report_state.last_report_at = now;
            PrintActivity::from_report(&report_state.report)
        } else {
            let report_state = device.report.insert(MqttReportState {
                report,
                last_report_at: now,
            });
            PrintActivity::from_report(&report_state.report)
        };
        if let PrintActivity::Unknown(gcode_state) = activity {
            tracing::debug!(
                device_id,
                gcode_state,
                "unknown MQTT printer gcode_state; treating task as inactive"
            );
        }
        state.updated_at = Some(now.to_rfc3339());
        refresh_status(&mut state);
        drop(state);
        self.notify();
    }

    fn notify(&self) {
        let _ = self.changes.send(());
    }
}

fn device_state(
    device: &DeviceLiveState,
    report: &MqttReportState,
    connections: &HashMap<String, MqttConnectionState>,
) -> MqttDeviceState {
    let connection = connection_state_for_report(
        device.connection_key.clone(),
        Some(report.last_report_at),
        connections,
    );

    MqttDeviceState::from_snapshot(
        report.report.clone(),
        Some(report.last_report_at),
        connection,
    )
}

fn connection_state(
    key: Option<String>,
    connections: &HashMap<String, MqttConnectionState>,
) -> MqttDeviceConnection {
    connection_state_for_report(key, None, connections)
}

fn connection_state_for_report(
    key: Option<String>,
    last_report_at: Option<DateTime<Utc>>,
    connections: &HashMap<String, MqttConnectionState>,
) -> MqttDeviceConnection {
    key.as_deref()
        .and_then(|key| connections.get(key))
        .map(|connection| MqttDeviceConnection {
            key: key.clone(),
            status: device_connection_status(connection, last_report_at),
            error: connection.error.clone(),
        })
        .unwrap_or_else(|| MqttDeviceConnection {
            key,
            status: MqttConnectionStatus::Disconnected,
            error: Some("MQTT connection has not been registered".to_owned()),
        })
}

fn device_connection_status(
    connection: &MqttConnectionState,
    last_report_at: Option<DateTime<Utc>>,
) -> MqttConnectionStatus {
    match connection.status {
        MqttTransportStatus::Disconnected => MqttConnectionStatus::Disconnected,
        MqttTransportStatus::Connecting => MqttConnectionStatus::Connecting,
        MqttTransportStatus::Connected { connected_at } => {
            if last_report_at.is_some_and(|last_report_at| last_report_at >= connected_at) {
                MqttConnectionStatus::Connected
            } else {
                MqttConnectionStatus::Connecting
            }
        }
    }
}

fn status_payload(state: &MqttState) -> MqttStatusPayload {
    MqttStatusPayload {
        connected: state.connected,
        error: state.error.clone(),
        updated_at: state.updated_at.clone(),
    }
}

fn refresh_status(state: &mut MqttState) {
    state.connected = state
        .connections
        .values()
        .any(|connection| matches!(connection.status, MqttTransportStatus::Connected { .. }));
    let mut errors = state
        .connections
        .iter()
        .filter_map(|(key, connection)| {
            connection
                .error
                .as_ref()
                .map(|error| format!("{key}: {error}"))
        })
        .collect::<Vec<_>>();
    errors.sort();
    state.error = (!errors.is_empty()).then(|| errors.join("; "));
}

impl Default for MqttRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::{bambu::PrinterStatus, mqtt::MqttConnectionStatus};

    use super::MqttRuntime;

    #[tokio::test]
    async fn snapshot_marks_reports_fresh_only_while_connection_is_connected() {
        let runtime = MqttRuntime::new();
        runtime
            .register_connection("printer-a", vec!["printer-a".to_owned()])
            .await;
        runtime.set_connection_connecting("printer-a").await;

        let snapshot = runtime.snapshot().await;
        let connection = snapshot.connections.get("printer-a").unwrap();
        assert_eq!(connection.status, MqttConnectionStatus::Connecting);

        runtime.set_connection_connected("printer-a").await;

        let snapshot = runtime.snapshot().await;
        let connection = snapshot.connections.get("printer-a").unwrap();
        assert_eq!(connection.status, MqttConnectionStatus::Connecting);

        runtime
            .merge_report(
                "printer-a",
                PrinterStatus {
                    status: Some("RUNNING".to_owned()),
                    ..PrinterStatus::default()
                },
            )
            .await;

        let snapshot = runtime.snapshot().await;
        let state = snapshot.devices.get("printer-a").unwrap();
        let connection = snapshot.connections.get("printer-a").unwrap();
        assert!(state.is_fresh());
        assert!(state.is_active_task());
        assert_eq!(state.connection.status, MqttConnectionStatus::Connected);
        assert_eq!(connection.status, MqttConnectionStatus::Connecting);
        assert!(state.last_report_at.is_some());
        assert_eq!(state.connection.key.as_deref(), Some("printer-a"));

        runtime.set_connection_disconnected("printer-a").await;

        let snapshot = runtime.snapshot().await;
        let state = snapshot.devices.get("printer-a").unwrap();
        let connection = snapshot.connections.get("printer-a").unwrap();
        assert!(!state.is_fresh());
        assert!(!state.is_active_task());
        assert_eq!(state.connection.status, MqttConnectionStatus::Disconnected);
        assert_eq!(connection.status, MqttConnectionStatus::Disconnected);
        assert!(!snapshot.status.connected);

        runtime.set_connection_connected("printer-a").await;

        let snapshot = runtime.snapshot().await;
        let state = snapshot.devices.get("printer-a").unwrap();
        let connection = snapshot.connections.get("printer-a").unwrap();
        assert!(!state.is_fresh());
        assert!(!state.is_active_task());
        assert_eq!(state.connection.status, MqttConnectionStatus::Connecting);
        assert_eq!(connection.status, MqttConnectionStatus::Connecting);

        runtime
            .merge_report(
                "printer-a",
                PrinterStatus {
                    status: Some("RUNNING".to_owned()),
                    ..PrinterStatus::default()
                },
            )
            .await;

        let snapshot = runtime.snapshot().await;
        let state = snapshot.devices.get("printer-a").unwrap();
        assert!(state.is_fresh());
        assert!(state.is_active_task());
        assert_eq!(state.connection.status, MqttConnectionStatus::Connected);
    }
}
