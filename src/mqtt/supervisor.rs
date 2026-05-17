use std::time::Duration;

use anyhow::Result;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::bambu::PrinterStatus;

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

pub(crate) async fn supervise_target(runtime: MqttRuntime, target: MqttTarget) {
    runtime
        .register_connection(target.connection_key(), target.device_ids())
        .await;
    let mut delay = Duration::from_secs(2);
    loop {
        match run_runtime_once(&runtime, &target).await {
            Ok(()) => delay = Duration::from_secs(2),
            Err(error) => {
                runtime
                    .set_connection_error(target.connection_key(), error.to_string())
                    .await;
                target.warn_disconnect(&error, "MQTT disconnected");
                tokio::time::sleep(delay).await;
                delay = (delay + delay / 2).min(Duration::from_secs(30));
            }
        }
    }
}

async fn run_runtime_once(runtime: &MqttRuntime, target: &MqttTarget) -> Result<()> {
    let connection_key = target.connection_key();
    runtime
        .set_connection_connecting(connection_key.clone())
        .await;
    let mut session = ReportSession::connect(target).await?;
    runtime
        .set_connection_connected(connection_key.clone())
        .await;
    while let Some(event) = session.next().await? {
        handle_publish(runtime, event).await;
    }
    runtime.set_connection_disconnected(connection_key).await;
    Ok(())
}

async fn handle_publish(runtime: &MqttRuntime, event: ReportEvent) {
    let parts = event.topic.split('/').collect::<Vec<_>>();
    if parts.len() != 3 || parts[0] != "device" || parts[2] != "report" {
        debug!(topic = %event.topic, "ignoring unexpected MQTT topic");
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
    use crate::mqtt::{MqttConnectionStatus, MqttRuntime};

    use super::{handle_publish, ReportEvent};

    fn report_event(retained: bool) -> ReportEvent {
        ReportEvent {
            topic: "device/printer-a/report".to_owned(),
            payload: br#"{"print":{"gcode_state":"RUNNING","mc_percent":42}}"#.to_vec(),
            retained,
        }
    }

    #[tokio::test]
    async fn retained_reports_do_not_mark_a_device_connected() {
        let runtime = MqttRuntime::new();
        runtime
            .register_connection("printer-a", vec!["printer-a".to_owned()])
            .await;
        runtime.set_connection_connected("printer-a").await;

        handle_publish(&runtime, report_event(true)).await;

        let snapshot = runtime.snapshot().await;
        assert!(snapshot.devices.get("printer-a").is_none());
        assert_eq!(
            snapshot.connections.get("printer-a").unwrap().status,
            MqttConnectionStatus::Connecting
        );

        handle_publish(&runtime, report_event(false)).await;

        let snapshot = runtime.snapshot().await;
        let state = snapshot.devices.get("printer-a").unwrap();
        assert_eq!(state.connection.status, MqttConnectionStatus::Connected);
        assert!(state.is_active_task());
    }
}
