//! Convert a Moonraker `printer.objects.subscribe` status map into a
//! vendor-neutral `PrinterReport`.

use serde_json::{Map, Value};

use crate::live::{Material, PrinterReport};

use super::moonraker::{extruders, get_f64, get_print_info_i64, get_string};

pub(super) fn to_live(status: &Map<String, Value>) -> PrinterReport {
    let filename = get_string(status, "print_stats", "filename")
        .map(str::trim)
        .filter(|filename| !filename.is_empty())
        .map(str::to_owned);

    let progress = get_f64(status, "display_status", "progress")
        .map(|fraction| (fraction * 100.0).clamp(0.0, 100.0));

    let toolhead_temperature = active_extruder_temperature(status);
    let bed_temperature = get_f64(status, "heater_bed", "temperature");
    let fan_speed = get_f64(status, "fan", "speed").map(|fraction| (fraction * 100.0).clamp(0.0, 100.0));

    let layer_current = get_print_info_i64(status, "current_layer");
    let layer_total = get_print_info_i64(status, "total_layer");

    let materials = materials_from_status(status);
    let active_material = active_tool_label(status);

    PrinterReport {
        task_id: filename.clone(),
        task_name: filename.clone(),
        status: map_state(get_string(status, "print_stats", "state")),
        filename,
        start_time: None,
        progress,
        prediction_seconds: None,
        remaining_minutes: None,
        weight: None,
        layer_current,
        layer_total,
        toolhead_temperature,
        bed_temperature,
        fan_speed,
        print_mode: None,
        materials,
        active_material,
    }
}

fn map_state(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    Some(match value.to_ascii_lowercase().as_str() {
        "standby" => "IDLE".to_owned(),
        "printing" => "RUNNING".to_owned(),
        "paused" => "PAUSED".to_owned(),
        "complete" => "FINISH".to_owned(),
        "cancelled" | "error" => "FAILED".to_owned(),
        _ => value.to_ascii_uppercase(),
    })
}

fn active_extruder_temperature(status: &Map<String, Value>) -> Option<f64> {
    let extruders = extruders(status);
    if let Some(active) = extruders
        .values()
        .find(|value| is_active_extruder(value))
        .and_then(|value| value.get("temperature").and_then(Value::as_f64))
    {
        return Some(active);
    }
    extruders
        .get(&0)
        .and_then(|value| value.get("temperature").and_then(Value::as_f64))
}

fn materials_from_status(status: &Map<String, Value>) -> Vec<Material> {
    let extruders = extruders(status);
    let mut indices: Vec<usize> = extruders.keys().copied().collect();
    indices.sort();
    indices
        .into_iter()
        .map(|index| Material {
            label: format!("T{}", index + 1),
            kind: "Filament".to_owned(),
            color: "#9CA3AF".to_owned(),
            active: false,
        })
        .collect()
}

fn active_tool_label(status: &Map<String, Value>) -> Option<String> {
    let extruders = extruders(status);
    for (index, value) in &extruders {
        if is_active_extruder(value) {
            return Some(format!("T{}", index + 1));
        }
    }
    None
}

fn is_active_extruder(value: &Value) -> bool {
    value.get("active_pin").and_then(Value::as_bool) == Some(true)
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map, Value};

    use super::to_live;

    fn status(value: Value) -> Map<String, Value> {
        let Value::Object(map) = value else { panic!("status must be object") };
        map
    }

    #[test]
    fn maps_printing_state_and_progress() {
        let report = to_live(&status(json!({
            "print_stats": {
                "state": "printing",
                "filename": "Cube.gcode",
                "info": { "current_layer": 12, "total_layer": 80 }
            },
            "display_status": { "progress": 0.4275 },
            "extruder": { "temperature": 215.0, "active_pin": true },
            "extruder1": { "temperature": 24.0, "active_pin": false },
            "extruder2": { "temperature": 25.0, "active_pin": false },
            "extruder3": { "temperature": 26.0, "active_pin": false },
            "heater_bed": { "temperature": 60.5 },
            "fan": { "speed": 0.6 }
        })));

        assert_eq!(report.status.as_deref(), Some("RUNNING"));
        assert_eq!(report.filename.as_deref(), Some("Cube.gcode"));
        assert_eq!(report.progress, Some(42.75));
        assert_eq!(report.layer_current, Some(12));
        assert_eq!(report.layer_total, Some(80));
        assert_eq!(report.toolhead_temperature, Some(215.0));
        assert_eq!(report.bed_temperature, Some(60.5));
        assert_eq!(report.fan_speed, Some(60.0));
        assert_eq!(report.materials.len(), 4);
        assert_eq!(report.materials[0].label, "T1");
        assert_eq!(report.materials[3].label, "T4");
        assert_eq!(report.active_material.as_deref(), Some("T1"));
    }

    #[test]
    fn maps_standby_to_idle() {
        let report = to_live(&status(json!({
            "print_stats": { "state": "standby", "filename": "" }
        })));
        assert_eq!(report.status.as_deref(), Some("IDLE"));
        assert!(report.filename.is_none());
    }

    #[test]
    fn falls_back_to_first_extruder_when_none_active() {
        let report = to_live(&status(json!({
            "print_stats": { "state": "standby" },
            "extruder": { "temperature": 24.0, "active_pin": false },
            "extruder1": { "temperature": 26.0, "active_pin": false }
        })));
        assert_eq!(report.toolhead_temperature, Some(24.0));
        assert!(report.active_material.is_none());
    }
}
