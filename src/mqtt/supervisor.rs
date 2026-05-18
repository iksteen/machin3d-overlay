use std::{collections::HashSet, time::Duration};

use anyhow::Result;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::{bambu::PrinterStatus, service::ShutdownReceiver};

use super::{
    session::{ReportEvent, ReportSession},
    MqttRuntime, MqttTarget,
};

#[derive(Deserialize)]
#[serde(untagged)]
enum ReportPayload {
    Wrapped { print: PrinterStatus },
    Bare(PrinterStatus),
}

impl ReportPayload {
    fn into_report(self) -> PrinterStatus {
        match self {
            ReportPayload::Wrapped { print } => print,
            ReportPayload::Bare(report) => report,
        }
    }
}

pub(crate) async fn supervise_target(
    runtime: MqttRuntime,
    target: MqttTarget,
    mut shutdown: ShutdownReceiver,
) {
    runtime
        .register_connection(target.connection_key(), target.device_ids())
        .await;
    let mut delay = Duration::from_secs(2);
    loop {
        match run_runtime_once(&runtime, &target, &mut shutdown).await {
            Ok(RunResult::Disconnected) => delay = Duration::from_secs(2),
            Ok(RunResult::Shutdown) => return,
            Err(error) => {
                runtime
                    .set_connection_error(target.connection_key(), error.to_string())
                    .await;
                target.warn_disconnect(&error, "MQTT disconnected");
                if sleep_or_shutdown(delay, &mut shutdown).await {
                    return;
                }
                delay = (delay + delay / 2).min(Duration::from_secs(30));
            }
        }
    }
}

enum RunResult {
    Disconnected,
    Shutdown,
}

async fn run_runtime_once(
    runtime: &MqttRuntime,
    target: &MqttTarget,
    shutdown: &mut ShutdownReceiver,
) -> Result<RunResult> {
    let connection_key = target.connection_key();
    let allowed_device_ids = target.device_ids().into_iter().collect::<HashSet<_>>();
    runtime
        .set_connection_connecting(connection_key.clone())
        .await;
    let mut session = tokio::select! {
        session = ReportSession::connect(target) => session?,
        _ = shutdown.cancelled() => {
            runtime.set_connection_disconnected(connection_key).await;
            return Ok(RunResult::Shutdown);
        }
    };
    runtime
        .set_connection_connected(connection_key.clone())
        .await;
    loop {
        let event = tokio::select! {
            event = session.next() => event?,
            _ = shutdown.cancelled() => {
                runtime.set_connection_disconnected(connection_key).await;
                return Ok(RunResult::Shutdown);
            }
        };
        let Some(event) = event else {
            break;
        };
        handle_publish(runtime, &allowed_device_ids, event).await;
    }
    runtime.set_connection_disconnected(connection_key).await;
    Ok(RunResult::Disconnected)
}

async fn sleep_or_shutdown(delay: Duration, shutdown: &mut ShutdownReceiver) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        _ = shutdown.cancelled() => true,
    }
}

async fn handle_publish(
    runtime: &MqttRuntime,
    allowed_device_ids: &HashSet<String>,
    event: ReportEvent,
) {
    let parts = event.topic.split('/').collect::<Vec<_>>();
    if parts.len() != 3 || parts[0] != "device" || parts[2] != "report" {
        debug!(topic = %event.topic, "ignoring unexpected MQTT topic");
        return;
    }
    if !allowed_device_ids.contains(parts[1]) {
        debug!(
            topic = %event.topic,
            device_id = parts[1],
            "ignoring MQTT report for unregistered device"
        );
        return;
    }
    if event.retained {
        debug!(
            topic = %event.topic,
            device_id = parts[1],
            "ignoring retained MQTT report"
        );
        return;
    }
    let Ok(report) = serde_json::from_slice::<ReportPayload>(&event.payload) else {
        warn!(
            topic = %event.topic,
            payload = %payload_preview(&event.payload),
            "ignoring MQTT report with unexpected JSON shape"
        );
        return;
    };
    runtime.merge_report(parts[1], report.into_report()).await;
}

fn payload_preview(payload: &[u8]) -> String {
    let limit = payload.len().min(300);
    let mut preview = String::from_utf8_lossy(&payload[..limit]).into_owned();
    if payload.len() > limit {
        preview.push_str("...");
    }
    preview
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::mqtt::{MqttConnectionStatus, MqttRuntime};

    use super::{handle_publish, ReportEvent};

    fn report_event(retained: bool) -> ReportEvent {
        ReportEvent {
            topic: "device/printer-a/report".to_owned(),
            payload: br#"{"print":{"gcode_state":"RUNNING","mc_percent":42}}"#.to_vec(),
            retained,
        }
    }

    fn allowed(device_ids: &[&str]) -> HashSet<String> {
        device_ids
            .iter()
            .map(|device_id| (*device_id).to_owned())
            .collect()
    }

    #[tokio::test]
    async fn retained_reports_do_not_mark_a_device_connected() {
        let runtime = MqttRuntime::new();
        runtime
            .register_connection("printer-a", vec!["printer-a".to_owned()])
            .await;
        runtime.set_connection_connected("printer-a").await;

        handle_publish(&runtime, &allowed(&["printer-a"]), report_event(true)).await;

        let snapshot = runtime.snapshot().await;
        assert!(!snapshot.devices.contains_key("printer-a"));
        assert_eq!(
            snapshot.connections.get("printer-a").unwrap().status,
            MqttConnectionStatus::Connecting
        );

        handle_publish(&runtime, &allowed(&["printer-a"]), report_event(false)).await;

        let snapshot = runtime.snapshot().await;
        let state = snapshot.devices.get("printer-a").unwrap();
        assert_eq!(state.connection.status, MqttConnectionStatus::Connected);
        assert!(state.is_active_task());
    }

    #[tokio::test]
    async fn reports_for_unregistered_devices_are_ignored() {
        let runtime = MqttRuntime::new();
        runtime
            .register_connection("printer-a", vec!["printer-a".to_owned()])
            .await;
        runtime.set_connection_connected("printer-a").await;

        handle_publish(
            &runtime,
            &allowed(&["printer-a"]),
            ReportEvent {
                topic: "device/printer-b/report".to_owned(),
                payload: br#"{"print":{"gcode_state":"RUNNING","mc_percent":42}}"#.to_vec(),
                retained: false,
            },
        )
        .await;

        let snapshot = runtime.snapshot().await;
        assert!(!snapshot.devices.contains_key("printer-b"));
    }
}
