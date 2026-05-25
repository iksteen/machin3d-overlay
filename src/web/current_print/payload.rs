//! Wire-shaped types for the current-print SSE stream and the snapshot JSON
//! response. Owns the projection from `DeviceSummary` to `DevicePayload`, and
//! the value-formatting helpers the projection uses to render seconds,
//! temperatures, weights, and percentages for the overlay.

use chrono::Utc;
use serde::Serialize;

use crate::{
    device_summary::DeviceSummary,
    live::{LiveStatusPayload, Material},
};

use super::super::paths;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::web) struct CurrentPrintPayload {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    updated_at: String,
    mqtt: LiveStatusPayload,
    devices: Vec<DevicePayload>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct DevicePayload {
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
    print_speed: Option<String>,
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
    materials: Vec<MaterialPayload>,
    thumbnail: Option<String>,
    thumbnail_task: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct MaterialPayload {
    label: String,
    kind: String,
    color: String,
    active: bool,
}

impl CurrentPrintPayload {
    pub(in crate::web) fn success(
        mqtt: LiveStatusPayload,
        devices: impl IntoIterator<Item = DeviceSummary>,
    ) -> CurrentPrintPayload {
        CurrentPrintPayload {
            ok: true,
            error: None,
            updated_at: Utc::now().to_rfc3339(),
            mqtt,
            devices: devices.into_iter().map(DevicePayload::from).collect(),
        }
    }

    pub(in crate::web) fn error(
        error: impl Into<String>,
        mqtt: LiveStatusPayload,
    ) -> CurrentPrintPayload {
        CurrentPrintPayload {
            ok: false,
            error: Some(error.into()),
            updated_at: Utc::now().to_rfc3339(),
            mqtt,
            devices: Vec::new(),
        }
    }
}

impl From<DeviceSummary> for DevicePayload {
    fn from(device: DeviceSummary) -> Self {
        let (progress, progress_source) = progress_with_source(&device);
        let thumbnail_task = device.thumbnail_task;
        let thumbnail = thumbnail_task
            .as_ref()
            .map(|_| paths::thumbnail(&device.id));

        Self {
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
            // The only task source the overlay currently knows about is
            // the printer's own status report. If we ever surface other
            // sources (cloud queue, slicer-side metadata), this becomes a
            // per-source label again.
            task_source: "printer status",
            print_speed: device.print_speed,
            progress: progress.map(round_progress),
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
            materials: device
                .materials
                .into_iter()
                .map(MaterialPayload::from)
                .collect(),
            thumbnail,
            thumbnail_task,
        }
    }
}

impl From<Material> for MaterialPayload {
    fn from(material: Material) -> Self {
        Self {
            label: material.label,
            kind: material.kind,
            color: material.color,
            active: material.active,
        }
    }
}

fn progress_with_source(device: &DeviceSummary) -> (Option<f64>, &'static str) {
    if let Some(progress) = device.progress.and_then(progress_number) {
        return (Some(progress), "reported");
    }

    (estimated_progress(device), "estimated")
}

fn estimated_progress(device: &DeviceSummary) -> Option<f64> {
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

fn round_progress(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
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
    use crate::{
        device_summary::DeviceSummary,
        live::{ConnectionStatus, LiveStatusPayload, Material},
    };

    use super::CurrentPrintPayload;

    #[test]
    fn device_payload_formats_web_payload_fields() {
        let payload = CurrentPrintPayload::success(
            LiveStatusPayload {
                any_connected: true,
                error: None,
                updated_at: None,
            },
            [DeviceSummary {
                id: "printer-a".to_owned(),
                name: "Office X1".to_owned(),
                online: true,
                service_status: ConnectionStatus::Connected,
                service_connected: true,
                is_printing: true,
                title: Some("Calibration cube".to_owned()),
                task_name: Some("Calibration cube".to_owned()),
                task_status: Some("RUNNING".to_owned()),
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
                print_speed: Some("Standard".to_owned()),
                plate_index: None,
                thumbnail_task: Some("Calibration cube".to_owned()),
                materials: vec![Material {
                    label: "1".to_owned(),
                    kind: "PLA".to_owned(),
                    color: "#FF0000".to_owned(),
                    active: true,
                }],
                ..DeviceSummary::default()
            }],
        );
        let device = &payload.devices[0];

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
            Some("/devices/printer-a/thumbnail")
        );
        assert_eq!(device.thumbnail_task.as_deref(), Some("Calibration cube"));
        assert_eq!(device.materials.len(), 1);
        assert_eq!(device.materials[0].kind, "PLA");
        assert_eq!(device.materials[0].color, "#FF0000");
        assert!(device.materials[0].active);
    }

    #[test]
    fn success_payload_omits_error_field() {
        let payload = CurrentPrintPayload::success(
            LiveStatusPayload {
                any_connected: true,
                error: None,
                updated_at: None,
            },
            [],
        );
        let value = serde_json::to_value(payload).unwrap();

        assert_eq!(value["ok"], true);
        assert!(value.get("error").is_none());
    }
}
