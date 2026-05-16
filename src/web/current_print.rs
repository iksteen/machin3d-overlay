use std::{convert::Infallible, time::Duration};

use anyhow::Result;
use async_stream::stream;
use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Json,
};
use chrono::Utc;
use serde::Serialize;
use serde_json::json;
use url::form_urlencoded;

use crate::{
    devices::DeviceRegistry,
    mqtt::{MqttRuntime, MqttStatusPayload},
    overlay::{summarize_devices, DeviceSummary, Spool, TaskSource},
};

use super::AppState;

#[derive(Clone)]
pub(super) struct CurrentPrintService {
    registry: DeviceRegistry,
    mqtt: MqttRuntime,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OverlayPayload {
    ok: bool,
    updated_at: String,
    mqtt: MqttStatusPayload,
    devices: Vec<OverlayDevice>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ErrorPayload {
    ok: bool,
    error: String,
    updated_at: String,
    mqtt: MqttStatusPayload,
    devices: Vec<OverlayDevice>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct OverlayDevice {
    id: String,
    name: String,
    online: bool,
    is_printing: bool,
    title: Option<String>,
    filename: Option<String>,
    task_name: Option<String>,
    task_status: Option<String>,
    task_source: &'static str,
    mode: Option<String>,
    progress: Option<f64>,
    progress_source: Option<String>,
    total_print_time: Option<String>,
    weight: Option<String>,
    layer_current: Option<i64>,
    layer_total: Option<i64>,
    time_remaining: Option<String>,
    toolhead_temp: Option<String>,
    bed_temp: Option<String>,
    fan_speed: Option<String>,
    started: Option<String>,
    plate: Option<String>,
    ams_spools: Vec<OverlaySpool>,
    external_spool: Option<OverlaySpool>,
    thumbnail: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct OverlaySpool {
    label: String,
    material: String,
    color: String,
    active: bool,
}

impl CurrentPrintService {
    pub(super) fn new(registry: DeviceRegistry, mqtt: MqttRuntime) -> Self {
        Self { registry, mqtt }
    }

    pub(super) async fn payload(&self) -> Result<OverlayPayload> {
        let reports = self.mqtt.reports().await;
        let status = self.mqtt.status().await;
        let devices = summarize_devices(self.registry.devices(), &reports)
            .into_iter()
            .map(overlay_device)
            .collect();

        Ok(OverlayPayload {
            ok: true,
            updated_at: Utc::now().to_rfc3339(),
            mqtt: status,
            devices,
        })
    }
}

pub(super) async fn current_print(State(state): State<AppState>) -> Response {
    match state.current_print.payload().await {
        Ok(payload) => Json(payload).into_response(),
        Err(error) => {
            let payload = error_payload(error.to_string(), state.mqtt.status().await);
            (StatusCode::BAD_GATEWAY, Json(payload)).into_response()
        }
    }
}

pub(super) async fn current_print_events(
    State(state): State<AppState>,
) -> Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>> {
    let mut changes = state.mqtt.subscribe();
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let stream = stream! {
        yield Ok(current_print_event(&state).await);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                received = changes.recv() => {
                    if received.is_err() {
                        changes = state.mqtt.subscribe();
                    }
                }
            }
            yield Ok(current_print_event(&state).await);
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn current_print_event(state: &AppState) -> Event {
    let payload = match state.current_print.payload().await {
        Ok(payload) => serde_json::to_string(&payload),
        Err(error) => {
            let payload = error_payload(error.to_string(), state.mqtt.status().await);
            serde_json::to_string(&payload)
        }
    }
    .unwrap_or_else(|error| json!({"ok": false, "error": error.to_string()}).to_string());

    Event::default().event("current-print").data(payload)
}

fn error_payload(error: impl Into<String>, mqtt: MqttStatusPayload) -> ErrorPayload {
    ErrorPayload {
        ok: false,
        error: error.into(),
        updated_at: Utc::now().to_rfc3339(),
        mqtt,
        devices: Vec::new(),
    }
}

fn overlay_device(device: DeviceSummary) -> OverlayDevice {
    let mut progress_source = "reported";
    let mut progress = device.progress.and_then(progress_number);
    if progress.is_none() {
        progress = estimated_progress(&device);
        progress_source = "estimated";
    }
    let thumbnail = device
        .thumbnail_task
        .as_deref()
        .map(|task| thumbnail_path(&device.id, task));

    OverlayDevice {
        id: device.id,
        name: device.name,
        online: device.online,
        is_printing: device.is_printing,
        title: device.title.or(device.task_name.clone()),
        filename: device.filename,
        task_name: device.task_name,
        task_status: device.task_status,
        task_source: task_source_label(device.task_source),
        mode: device.print_mode,
        progress: progress.map(|value| (value * 10.0).round() / 10.0),
        progress_source: progress.map(|_| progress_source.to_owned()),
        total_print_time: device.prediction.map(format_seconds),
        weight: device.weight.as_deref().and_then(format_weight),
        layer_current: device.layer_current,
        layer_total: device.layer_total,
        time_remaining: device.remaining_seconds.map(format_seconds),
        toolhead_temp: device.toolhead_temperature.map(format_temperature),
        bed_temp: device.bed_temperature.map(format_temperature),
        fan_speed: device.fan_speed.map(format_percent),
        started: device.start_time,
        plate: device.plate_index,
        ams_spools: device
            .ams_spools
            .into_iter()
            .map(OverlaySpool::from)
            .collect(),
        external_spool: device.external_spool.map(OverlaySpool::from),
        thumbnail,
    }
}

impl From<Spool> for OverlaySpool {
    fn from(spool: Spool) -> Self {
        Self {
            label: spool.label,
            material: spool.material,
            color: spool.color,
            active: spool.active,
        }
    }
}

fn task_source_label(task_source: TaskSource) -> &'static str {
    match task_source {
        TaskSource::PrinterStatus => "printer status",
    }
}

fn thumbnail_path(device_id: &str, task: &str) -> String {
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("device", device_id)
        .append_pair("task", task)
        .finish();
    format!("/api/thumbnail?{query}")
}

fn estimated_progress(device: &DeviceSummary) -> Option<f64> {
    if device.progress.is_some() {
        return None;
    }
    let start = device
        .start_time
        .as_deref()
        .and_then(parse_bambu_datetime)?;
    let prediction = device.prediction?;
    if prediction <= 0.0 {
        return None;
    }
    let elapsed = (Utc::now() - start).num_seconds() as f64;
    Some((elapsed / prediction * 100.0).clamp(0.0, 100.0))
}

fn format_temperature(value: f64) -> String {
    format!("{}C", value.round() as i64)
}

fn format_percent(value: f64) -> String {
    format!("{}%", value.clamp(0.0, 100.0))
}

fn progress_number(value: f64) -> Option<f64> {
    value.is_finite().then(|| value.clamp(0.0, 100.0))
}

fn format_seconds(seconds: f64) -> String {
    let total_seconds = seconds as i64;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let remaining_seconds = total_seconds % 60;
    let mut parts = Vec::new();
    if hours > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 {
        parts.push(format!("{minutes}m"));
    }
    if remaining_seconds > 0 || parts.is_empty() {
        parts.push(format!("{remaining_seconds}s"));
    }
    parts.join(" ")
}

fn format_weight(value: &str) -> Option<String> {
    let grams = value.trim().parse::<f64>().ok()?;
    if grams >= 1000.0 {
        Some(format!("{:.1}kg", grams / 1000.0))
    } else {
        Some(format!("{grams:.1}g").replace(".0g", "g"))
    }
}

fn parse_bambu_datetime(text: &str) -> Option<chrono::DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(text)
        .or_else(|_| chrono::DateTime::parse_from_rfc3339(&text.replace('Z', "+00:00")))
        .ok()
        .map(|parsed| parsed.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde::de::DeserializeOwned;
    use serde_json::{json, Value};

    use crate::{
        bambu::{CloudDevice, PrinterStatus},
        devices::KnownDevice,
        overlay::summarize_devices,
    };

    use super::overlay_device;

    fn decode<T: DeserializeOwned>(value: Value) -> T {
        serde_json::from_value(value).expect("fixture should match typed API shape")
    }

    fn device(value: Value) -> KnownDevice {
        KnownDevice::from_cloud(decode::<CloudDevice>(value)).expect("device should have an ID")
    }

    #[test]
    fn overlay_device_uses_matching_mqtt_report_fields_only() {
        let devices = vec![
            device(json!({
                    "dev_id": "printer-a",
                    "print": {
                        "mc_percent": 12,
                        "nozzle_temper": 210
                    }
            })),
            device(json!({
                    "dev_id": "printer-b",
                    "print": {
                        "mc_percent": 1
                    }
            })),
        ];
        let reports = HashMap::from([(
            "printer-a".to_owned(),
            decode::<PrinterStatus>(json!({
                "mc_percent": 42,
                "bed_temper": 60
            })),
        )]);

        let devices = summarize_devices(&devices, &reports)
            .into_iter()
            .map(overlay_device)
            .collect::<Vec<_>>();

        assert_eq!(devices[0].progress, Some(42.0));
        assert_eq!(devices[0].toolhead_temp.as_deref(), Some("210C"));
        assert_eq!(devices[0].bed_temp.as_deref(), Some("60C"));
        assert_eq!(devices[1].progress, Some(1.0));
    }

    #[test]
    fn overlay_device_keeps_cloud_spools_when_mqtt_report_is_empty() {
        let devices = vec![device(json!({
                    "dev_id": "printer-a",
                    "print": {
                        "mc_percent": 12,
                        "ams": {
                            "ams": [
                                {
                                    "id": 0,
                                    "tray": [
                                        {
                                            "id": 0,
                                            "tray_type": "PLA",
                                            "tray_color": "ff0000ff"
                                        }
                                    ]
                                }
                            ]
                        },
                        "vt_tray": {
                            "id": 777,
                            "tray_type": "PETG",
                            "tray_color": "336699ff"
                        }
                    }
        }))];
        let reports = HashMap::from([(
            "printer-a".to_owned(),
            decode::<PrinterStatus>(json!({
                "mc_percent": 42,
                "ams": {"tray_now": "777", "ams": [{"id": 0, "tray": [{"id": 0, "tray_color": "00000000"}]}]},
                "vt_tray": {"id": 777, "tray_color": "00000000"}
            })),
        )]);

        let device = summarize_devices(&devices, &reports)
            .into_iter()
            .map(overlay_device)
            .next()
            .unwrap();

        assert_eq!(device.progress, Some(42.0));
        assert_eq!(device.ams_spools.len(), 1);
        assert_eq!(device.ams_spools[0].material, "PLA");
        assert_eq!(device.ams_spools[0].color, "#FF0000");
        assert!(!device.ams_spools[0].active);
        assert_eq!(device.external_spool.as_ref().unwrap().material, "PETG");
        assert_eq!(device.external_spool.as_ref().unwrap().color, "#336699");
        assert!(device.external_spool.as_ref().unwrap().active);
    }

    #[test]
    fn overlay_device_uses_catalog_status_and_spools() {
        let devices = vec![device(json!({
                    "dev_id": "printer-a",
                    "dev_name": "Office X1",
                    "dev_online": true,
                    "print": {
                        "subtask_name": "Calibration cube",
                        "mc_percent": 25,
                        "cost_time": 3600,
                        "gcode_start_time": "2026-05-11T00:00:00Z",
                        "layer_num": 4,
                        "total_layer_num": 20,
                        "nozzle_temper": 220,
                        "bed_temper": 60,
                        "ams": {
                            "tray_now": "0",
                            "ams": [
                                {
                                    "id": 0,
                                    "tray": [
                                        {
                                            "id": 0,
                                            "tray_type": "PLA",
                                            "tray_color": "ff0000ff"
                                        }
                                    ]
                                }
                            ]
                        }
                    }
        }))];

        let device = summarize_devices(&devices, &HashMap::new())
            .into_iter()
            .map(overlay_device)
            .next()
            .unwrap();

        assert_eq!(device.name, "Office X1");
        assert_eq!(device.title.as_deref(), Some("Calibration cube"));
        assert_eq!(device.task_source, "printer status");
        assert_eq!(device.progress, Some(25.0));
        assert_eq!(device.total_print_time.as_deref(), Some("1h"));
        assert_eq!(device.weight, None);
        assert_eq!(device.plate, None);
        assert_eq!(
            device.thumbnail.as_deref(),
            Some("/api/thumbnail?device=printer-a&task=Calibration+cube")
        );
        assert_eq!(device.ams_spools.len(), 1);
        assert_eq!(device.ams_spools[0].material, "PLA");
        assert_eq!(device.ams_spools[0].color, "#FF0000");
        assert!(device.ams_spools[0].active);
    }
}
