//! Per-device live-state store shared by every backend.
//!
//! Each backend (Bambu MQTT today, Snapmaker Moonraker next) translates its
//! wire protocol into vendor-neutral [`PrinterReport`] + [`DeviceConnection`]
//! and publishes via [`LiveStateStore::publish_report`] or
//! [`LiveStateStore::set_device_connection`]. The summary/web layer reads via
//! [`LiveStateStore::snapshot`].
//!
//! A device is considered "fresh" only while its `connection.status` is
//! `Connected` — disconnecting clears the report so a stale snapshot can't
//! survive a reconnect.

use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::{broadcast, RwLock};

use super::{ConnectionStatus, DeviceConnection, DeviceLiveState, PrinterReport};

const CHANGE_CHANNEL_CAPACITY: usize = 128;

#[derive(Clone, Default)]
pub(crate) struct LiveStateStore {
    inner: Arc<LiveStateInner>,
}

struct LiveStateInner {
    state: RwLock<StoreState>,
    changes: broadcast::Sender<()>,
}

#[derive(Default)]
struct StoreState {
    devices: HashMap<String, StoredDeviceState>,
    updated_at: Option<String>,
}

#[derive(Default, Clone)]
struct StoredDeviceState {
    report: Option<PrinterReport>,
    last_report_at: Option<DateTime<Utc>>,
    connection: DeviceConnection,
}

#[derive(Debug, Clone)]
pub(crate) struct LiveSnapshot {
    pub(crate) devices: HashMap<String, DeviceLiveState>,
    pub(crate) connections: HashMap<String, DeviceConnection>,
    pub(crate) status: LiveStatusPayload,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveStatusPayload {
    /// `true` if at least one device is currently `Connected`. Per-device
    /// status lives in `LiveSnapshot.connections`.
    pub any_connected: bool,
    pub error: Option<String>,
    pub updated_at: Option<String>,
}

impl LiveStateStore {
    pub(crate) fn new() -> Self {
        let (changes, _) = broadcast::channel(CHANGE_CHANNEL_CAPACITY);
        Self {
            inner: Arc::new(LiveStateInner {
                state: RwLock::new(StoreState::default()),
                changes,
            }),
        }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<()> {
        self.inner.changes.subscribe()
    }

    pub(crate) async fn snapshot(&self) -> LiveSnapshot {
        let state = self.inner.state.read().await;
        let devices = state
            .devices
            .iter()
            .filter_map(|(id, record)| {
                let report = record.report.clone()?;
                Some((
                    id.clone(),
                    DeviceLiveState::from_snapshot(
                        report,
                        record.last_report_at,
                        record.connection.clone(),
                    ),
                ))
            })
            .collect();
        let connections = state
            .devices
            .iter()
            .map(|(id, record)| (id.clone(), record.connection.clone()))
            .collect();
        LiveSnapshot {
            devices,
            connections,
            status: status_payload(&state),
        }
    }

    pub(crate) async fn status(&self) -> LiveStatusPayload {
        let state = self.inner.state.read().await;
        status_payload(&state)
    }

    /// Replace the device's report and mark it `Connected`. Updates the
    /// `last_report_at` timestamp and notifies subscribers.
    pub(crate) async fn publish_report(
        &self,
        device_id: &str,
        report: PrinterReport,
        connection: DeviceConnection,
    ) {
        self.mutate(|state| {
            let now = Utc::now();
            let record = state.devices.entry(device_id.to_owned()).or_default();
            record.report = Some(report);
            record.last_report_at = Some(now);
            record.connection = connection;
            state.updated_at = Some(now.to_rfc3339());
        })
        .await;
    }

    /// Update the device's connection without changing its report. If the new
    /// status is not `Connected`, the stored report is cleared so a stale
    /// snapshot cannot leak across a reconnect.
    pub(crate) async fn set_device_connection(
        &self,
        device_id: &str,
        connection: DeviceConnection,
    ) {
        self.mutate(|state| {
            let record = state.devices.entry(device_id.to_owned()).or_default();
            record.connection = connection;
            if record.connection.status != ConnectionStatus::Connected {
                record.report = None;
                record.last_report_at = None;
            }
            state.updated_at = Some(Utc::now().to_rfc3339());
        })
        .await;
    }

    /// Forget a device entirely (e.g. when the registry no longer lists it).
    pub(crate) async fn remove_device(&self, device_id: &str) {
        self.mutate(|state| {
            state.devices.remove(device_id);
        })
        .await;
    }

    async fn mutate(&self, f: impl FnOnce(&mut StoreState)) {
        let mut state = self.inner.state.write().await;
        f(&mut state);
        drop(state);
        let _ = self.inner.changes.send(());
    }
}

impl Default for LiveStateInner {
    fn default() -> Self {
        let (changes, _) = broadcast::channel(CHANGE_CHANNEL_CAPACITY);
        Self {
            state: RwLock::new(StoreState::default()),
            changes,
        }
    }
}

fn status_payload(state: &StoreState) -> LiveStatusPayload {
    let any_connected = state
        .devices
        .values()
        .any(|record| record.connection.status == ConnectionStatus::Connected);
    let mut errors: Vec<String> = state
        .devices
        .iter()
        .filter_map(|(id, record)| {
            record
                .connection
                .error
                .as_ref()
                .map(|err| format!("{id}: {err}"))
        })
        .collect();
    errors.sort();
    LiveStatusPayload {
        any_connected,
        error: (!errors.is_empty()).then(|| errors.join("; ")),
        updated_at: state.updated_at.clone(),
    }
}
