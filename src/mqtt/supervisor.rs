use std::time::Duration;

use anyhow::Result;
use serde::Deserialize;
use tracing::{debug, error, warn};

use crate::{bambu::PrinterStatus, local::LocalDevice};

use super::{session::ReportSession, MqttRuntime, MqttTarget};

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

pub(crate) fn start_local_supervisors(runtime: MqttRuntime, devices: Vec<LocalDevice>) {
    for device in devices {
        start_local_supervisor(runtime.clone(), MqttTarget::local(device));
    }
}

fn start_local_supervisor(runtime: MqttRuntime, target: MqttTarget) {
    let device_id = target.connection_key();
    let mqtt_status = runtime.clone();
    let supervisor = tokio::spawn(supervise_target(runtime, target));
    tokio::spawn(async move {
        match supervisor.await {
            Ok(()) => {
                warn!(
                    device_id = %device_id,
                    "local MQTT supervisor exited unexpectedly"
                );
                mqtt_status
                    .set_connection_error(device_id, "local MQTT supervisor exited unexpectedly")
                    .await;
            }
            Err(error) => {
                error!(
                    device_id = %device_id,
                    error = %error,
                    "local MQTT supervisor task failed"
                );
                mqtt_status
                    .set_connection_error(
                        device_id,
                        format!("local MQTT supervisor task failed: {error}"),
                    )
                    .await;
            }
        }
    });
}

pub(crate) async fn supervise_target(runtime: MqttRuntime, target: MqttTarget) {
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
    let mut session = ReportSession::connect(target).await?;
    let connection_key = target.connection_key();
    runtime
        .set_connection_connected(connection_key.clone(), true)
        .await;
    while let Some(event) = session.next().await? {
        handle_publish(runtime, event.topic, event.payload).await;
    }
    runtime
        .set_connection_connected(connection_key, false)
        .await;
    Ok(())
}

async fn handle_publish(runtime: &MqttRuntime, topic: String, payload: Vec<u8>) {
    let parts = topic.split('/').collect::<Vec<_>>();
    if parts.len() != 3 || parts[0] != "device" || parts[2] != "report" {
        debug!(%topic, "ignoring unexpected MQTT topic");
        return;
    }
    let Ok(report) = serde_json::from_slice::<ReportPayload>(&payload) else {
        warn!(
            topic = %topic,
            payload = %payload_preview(&payload),
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
