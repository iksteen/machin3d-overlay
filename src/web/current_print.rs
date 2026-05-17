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
    device_summary::{summarize_devices, DeviceSummary, Spool, TaskSource},
    devices::DeviceRegistry,
    mqtt::{MqttRuntime, MqttStatusPayload},
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
    service_status: &'static str,
    service_connected: bool,
    service_error: Option<String>,
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
        let snapshot = self.mqtt.snapshot().await;
        let devices = summarize_devices(
            self.registry.devices(),
            &snapshot.devices,
            &snapshot.connections,
        )
        .into_iter()
        .map(overlay_device)
        .collect();

        Ok(OverlayPayload {
            ok: true,
            updated_at: Utc::now().to_rfc3339(),
            mqtt: snapshot.status,
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
    let mut shutdown = state.shutdown.subscribe();
    let stream = stream! {
        yield Ok(current_print_event(&state).await);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
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
        service_status: device.service_status.as_str(),
        service_connected: device.service_connected,
        service_error: device.service_error,
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
    use crate::device_summary::{DeviceSummary, Spool, TaskSource};

    use super::overlay_device;

    #[test]
    fn overlay_device_formats_web_payload_fields() {
        let device = overlay_device(DeviceSummary {
            id: "printer-a".to_owned(),
            name: "Office X1".to_owned(),
            online: true,
            service_status: crate::mqtt::MqttConnectionStatus::Connected,
            service_connected: true,
            is_printing: true,
            title: Some("Calibration cube".to_owned()),
            task_name: Some("Calibration cube".to_owned()),
            task_status: Some("RUNNING".to_owned()),
            task_source: TaskSource::PrinterStatus,
            prediction: Some(3600.0),
            progress: Some(25.04),
            weight: Some("1250".to_owned()),
            layer_current: Some(4),
            layer_total: Some(20),
            remaining_seconds: Some(90.0),
            toolhead_temperature: Some(219.6),
            bed_temperature: Some(60.4),
            fan_speed: Some(101.0),
            start_time: Some("2026-05-11T00:00:00Z".to_owned()),
            print_mode: Some("Standard".to_owned()),
            plate_index: None,
            thumbnail_task: Some("Calibration cube".to_owned()),
            ams_spools: vec![Spool {
                label: "1".to_owned(),
                material: "PLA".to_owned(),
                color: "#FF0000".to_owned(),
                active: true,
            }],
            external_spool: None,
            ..DeviceSummary::default()
        });

        assert_eq!(device.name, "Office X1");
        assert_eq!(device.service_status, "connected");
        assert!(device.service_connected);
        assert_eq!(device.service_error, None);
        assert_eq!(device.title.as_deref(), Some("Calibration cube"));
        assert_eq!(device.task_source, "printer status");
        assert_eq!(device.progress, Some(25.0));
        assert_eq!(device.total_print_time.as_deref(), Some("1h"));
        assert_eq!(device.weight.as_deref(), Some("1.2kg"));
        assert_eq!(device.time_remaining.as_deref(), Some("1m 30s"));
        assert_eq!(device.toolhead_temp.as_deref(), Some("220C"));
        assert_eq!(device.bed_temp.as_deref(), Some("60C"));
        assert_eq!(device.fan_speed.as_deref(), Some("100%"));
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
