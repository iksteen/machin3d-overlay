//! Convert a Moonraker `printer.objects.subscribe` status map into a
//! vendor-neutral `PrinterReport`.

use serde_json::{Map, Value};

use crate::live::{Material, PrinterReport};

use super::client::{extruders, get_f64, get_print_info_i64, get_string};

pub(super) fn to_live(status: &Map<String, Value>) -> PrinterReport {
    let filename = get_string(status, "print_stats", "filename")
        .map(str::trim)
        .filter(|filename| !filename.is_empty())
        .map(str::to_owned);

    let progress = get_f64(status, "display_status", "progress")
        .map(|fraction| (fraction * 100.0).clamp(0.0, 100.0));

    let active_index = active_tool_index(status);
    let toolhead_temperature = toolhead_temperature(status, active_index);
    let bed_temperature = get_f64(status, "heater_bed", "temperature");
    let fan_speed =
        get_f64(status, "fan", "speed").map(|fraction| (fraction * 100.0).clamp(0.0, 100.0));
    let print_speed = get_f64(status, "gcode_move", "speed_factor")
        .map(|factor| format!("{}%", (factor * 100.0).round() as i64));

    let layer_current = get_print_info_i64(status, "current_layer");
    let layer_total = get_print_info_i64(status, "total_layer");

    let materials = materials_from_status(status);
    let active_material = active_index.map(tool_label);

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
        print_speed,
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

fn tool_label(index: usize) -> String {
    format!("T{}", index + 1)
}

/// Klipper's currently-selected extruder lives in `toolhead.extruder`,
/// formatted as `"extruder"` (index 0) or `"extruderN"` (index N>=1).
fn active_tool_index(status: &Map<String, Value>) -> Option<usize> {
    let name = get_string(status, "toolhead", "extruder")?.trim();
    if name == "extruder" {
        return Some(0);
    }
    name.strip_prefix("extruder")?.parse::<usize>().ok()
}

fn toolhead_temperature(status: &Map<String, Value>, active_index: Option<usize>) -> Option<f64> {
    let extruders = extruders(status);
    if let Some(index) = active_index {
        if let Some(value) = extruders
            .get(&index)
            .and_then(|value| value.get("temperature").and_then(Value::as_f64))
        {
            return Some(value);
        }
    }
    extruders
        .get(&0)
        .and_then(|value| value.get("temperature").and_then(Value::as_f64))
}

fn materials_from_status(status: &Map<String, Value>) -> Vec<Material> {
    if let Some(materials) = materials_from_task_config(status) {
        return materials;
    }
    let extruders = extruders(status);
    let mut indices: Vec<usize> = extruders.keys().copied().collect();
    indices.sort();
    indices
        .into_iter()
        .map(|index| Material {
            label: tool_label(index),
            kind: "Filament".to_owned(),
            color: "#9CA3AF".to_owned(),
            active: false,
        })
        .collect()
}

fn materials_from_task_config(status: &Map<String, Value>) -> Option<Vec<Material>> {
    let task = status.get("print_task_config")?.as_object()?;
    let colors = task.get("filament_color_rgba")?.as_array()?;
    if colors.is_empty() {
        return None;
    }
    let types = task.get("filament_type").and_then(Value::as_array);
    let materials = colors
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let color = value
                .as_str()
                .and_then(filament_color_from_rgba)
                .unwrap_or_else(|| "#9CA3AF".to_owned());
            let kind = types
                .and_then(|list| list.get(index))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|kind| !kind.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| "Filament".to_owned());
            Material {
                label: tool_label(index),
                kind,
                color,
                active: false,
            }
        })
        .collect();
    Some(materials)
}

/// Decode Snapmaker's per-slot filament color. The U1 reports a `RRGGBBAA`
/// hex string (no leading `#`). Empty/transparent slots are returned as
/// `None` so the caller can fall back to a neutral color.
fn filament_color_from_rgba(value: &str) -> Option<String> {
    let value = value.trim().trim_start_matches('#');
    if value.len() < 6 {
        return None;
    }
    let rgb = &value[..6];
    if rgb.eq_ignore_ascii_case("000000") && value.get(6..8) == Some("00") {
        return None;
    }
    u32::from_str_radix(rgb, 16).ok()?;
    Some(format!("#{}", rgb.to_ascii_uppercase()))
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map, Value};

    use super::to_live;

    fn status(value: Value) -> Map<String, Value> {
        let Value::Object(map) = value else {
            panic!("status must be object")
        };
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
            "extruder": { "temperature": 215.0 },
            "extruder1": { "temperature": 24.0 },
            "extruder2": { "temperature": 25.0 },
            "extruder3": { "temperature": 26.0 },
            "heater_bed": { "temperature": 60.5 },
            "fan": { "speed": 0.6 },
            "gcode_move": { "speed_factor": 1.25 },
            "toolhead": { "extruder": "extruder" }
        })));

        assert_eq!(report.status.as_deref(), Some("RUNNING"));
        assert_eq!(report.filename.as_deref(), Some("Cube.gcode"));
        assert_eq!(report.progress, Some(42.75));
        assert_eq!(report.layer_current, Some(12));
        assert_eq!(report.layer_total, Some(80));
        assert_eq!(report.toolhead_temperature, Some(215.0));
        assert_eq!(report.bed_temperature, Some(60.5));
        assert_eq!(report.fan_speed, Some(60.0));
        assert_eq!(report.print_speed.as_deref(), Some("125%"));
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
    fn falls_back_to_first_extruder_when_toolhead_is_missing() {
        let report = to_live(&status(json!({
            "print_stats": { "state": "standby" },
            "extruder": { "temperature": 24.0 },
            "extruder1": { "temperature": 26.0 }
        })));
        assert_eq!(report.toolhead_temperature, Some(24.0));
        assert!(report.active_material.is_none());
    }

    #[test]
    fn materials_use_print_task_config_colors_and_types() {
        let report = to_live(&status(json!({
            "print_stats": { "state": "printing", "filename": "Multi.gcode" },
            "extruder": { "temperature": 215.0 },
            "extruder1": { "temperature": 220.0 },
            "extruder2": { "temperature": 200.0 },
            "extruder3": { "temperature": 210.0 },
            "toolhead": { "extruder": "extruder2" },
            "print_task_config": {
                "filament_color_rgba": ["E72F1DFF", "F4C032FF", "080A0DFF", "E2DEDBFF"],
                "filament_type": ["PLA", "PLA", "PETG", "TPU"]
            }
        })));

        assert_eq!(report.materials.len(), 4);
        assert_eq!(report.materials[0].label, "T1");
        assert_eq!(report.materials[0].color, "#E72F1D");
        assert_eq!(report.materials[0].kind, "PLA");
        assert_eq!(report.materials[2].color, "#080A0D");
        assert_eq!(report.materials[2].kind, "PETG");
        assert_eq!(report.materials[3].kind, "TPU");
        assert_eq!(report.active_material.as_deref(), Some("T3"));
        assert_eq!(report.toolhead_temperature, Some(200.0));
    }

    #[test]
    fn materials_handle_transparent_slots() {
        let report = to_live(&status(json!({
            "print_stats": { "state": "printing", "filename": "x.gcode" },
            "extruder": { "temperature": 24.0 },
            "extruder1": { "temperature": 24.0 },
            "print_task_config": {
                "filament_color_rgba": ["FF0000FF", "00000000"],
                "filament_type": ["PLA", ""]
            }
        })));

        assert_eq!(report.materials[0].color, "#FF0000");
        assert_eq!(report.materials[0].kind, "PLA");
        // Transparent slot falls back to neutral color and generic kind label.
        assert_eq!(report.materials[1].color, "#9CA3AF");
        assert_eq!(report.materials[1].kind, "Filament");
    }
}
