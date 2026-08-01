//! Convert a Moonraker `printer.objects.subscribe` status map into a
//! vendor-neutral `PrinterReport`.

use chrono::DateTime;
use serde_json::{Map, Value};

use crate::live::{Material, PrinterReport};

use super::{
    client::{extruders, get_f64, get_print_info_i64, get_string},
    metadata::JobMetadata,
};

pub(super) fn to_live(status: &Map<String, Value>, eta: &mut EtaTracker) -> PrinterReport {
    let filename = get_string(status, "print_stats", "filename")
        .map(str::trim)
        .filter(|filename| !filename.is_empty())
        .map(str::to_owned);

    let fraction = progress_fraction(status);
    let progress = fraction.map(|fraction| (fraction * 100.0).clamp(0.0, 100.0));
    let (prediction_seconds, remaining_minutes) =
        eta.estimate(fraction, get_f64(status, "print_stats", "print_duration"));

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
        prediction_seconds,
        remaining_minutes,
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

/// Overlay the slicer's own facts onto a report built from the live printer
/// objects. The estimate in the gcode is a constant for the job, so it
/// replaces the derived total — which necessarily moves every time progress
/// re-anchors it. The *remaining* time keeps counting down against the derived
/// total: once a print runs off the slicer's estimate, subtracting elapsed
/// from that estimate is worse than the measured pace, not better.
pub(super) fn apply_job_metadata(report: &mut PrinterReport, metadata: &JobMetadata) {
    if let Some(estimated) = metadata
        .estimated_time
        .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
    {
        report.prediction_seconds = Some(estimated);
    }
    if let Some(grams) = metadata
        .filament_weight_total
        .filter(|grams| grams.is_finite() && *grams > 0.0)
    {
        report.weight = Some(format!("{grams}"));
    }
    if let Some(start) = metadata
        .print_start_time
        .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
        .and_then(|seconds| DateTime::from_timestamp(seconds as i64, 0))
    {
        report.start_time = Some(start.to_rfc3339());
    }
}

/// Below this the extrapolation below is meaningless (at 0.5% done, one
/// slow first layer predicts a two-day print), so we withhold an estimate
/// rather than show a wild one.
const MIN_PROGRESS_FOR_ESTIMATE: f64 = 0.01;

/// How far along the job is, as a 0..1 fraction. Prefers `display_status`,
/// which the slicer drives with `M73` and which therefore tracks *time* —
/// early layers and tool-change purges cost far more time than they do file
/// bytes. Falls back to the file-position progress a printer without `M73`
/// still reports.
fn progress_fraction(status: &Map<String, Value>) -> Option<f64> {
    get_f64(status, "display_status", "progress")
        .or_else(|| get_f64(status, "virtual_sdcard", "progress"))
        .filter(|fraction| fraction.is_finite())
}

/// Klipper reports no ETA of its own — Moonraker clients all derive one from
/// elapsed time over progress. `print_stats.print_duration` counts only time
/// actually spent printing (pauses and pre-print heating excluded), so
/// `elapsed / fraction` is the whole job and the remainder is what is left.
///
/// Recomputing that on every status update is what makes a naive ETA useless:
/// the slicer steps `M73` about once a percent — minutes apart on a long print
/// — and between two steps a fixed `fraction` with a growing `elapsed`
/// inflates the total, so the *remaining* time counts **up** at several times
/// real speed and then snaps back at the next step. So the total is re-derived
/// only when progress actually moves; in between it is held and the remainder
/// counts down against it. Both are rounded to whole minutes: the underlying
/// accuracy does not justify seconds, and the overlay should not rewrite a
/// digit every second.
#[derive(Default)]
pub(super) struct EtaTracker {
    anchor: Option<Anchor>,
}

struct Anchor {
    fraction: f64,
    elapsed: f64,
    total: f64,
}

impl EtaTracker {
    /// Returns `(prediction_seconds, remaining_minutes)`.
    fn estimate(
        &mut self,
        fraction: Option<f64>,
        elapsed: Option<f64>,
    ) -> (Option<f64>, Option<f64>) {
        let Some(fraction) =
            fraction.filter(|fraction| (MIN_PROGRESS_FOR_ESTIMATE..=1.0).contains(fraction))
        else {
            self.anchor = None;
            return (None, None);
        };
        let Some(elapsed) = elapsed.filter(|elapsed| elapsed.is_finite() && *elapsed > 0.0) else {
            self.anchor = None;
            return (None, None);
        };

        // A shrinking elapsed time means this is a different job.
        let stale = self
            .anchor
            .as_ref()
            .is_none_or(|anchor| anchor.fraction != fraction || elapsed < anchor.elapsed);
        if stale {
            self.anchor = Some(Anchor {
                fraction,
                elapsed,
                total: elapsed / fraction,
            });
        }
        let total = self.anchor.as_ref().expect("anchor set above").total;
        (
            Some((total / 60.0).round() * 60.0),
            Some(((total - elapsed) / 60.0).max(0.0).round()),
        )
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

    use super::{to_live, EtaTracker};
    use crate::live::PrinterReport;

    fn status(value: Value) -> Map<String, Value> {
        let Value::Object(map) = value else {
            panic!("status must be object")
        };
        map
    }

    /// Most cases are one-shot conversions; the ETA anchoring tests below
    /// drive `to_live` repeatedly against a tracker of their own.
    fn report(value: Value) -> PrinterReport {
        to_live(&status(value), &mut EtaTracker::default())
    }

    #[test]
    fn maps_printing_state_and_progress() {
        let report = report(json!({
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
        }));

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
    fn derives_eta_from_elapsed_time_and_progress() {
        let report = report(json!({
            "print_stats": { "state": "printing", "filename": "Cube.gcode", "print_duration": 900.0 },
            "display_status": { "progress": 0.25 },
            "virtual_sdcard": { "progress": 0.10 }
        }));

        // 900s at 25% ⇒ 3600s total, 2700s (45min) left. The file-position
        // progress is ignored while the slicer's M73 value is present.
        assert_eq!(report.progress, Some(25.0));
        assert_eq!(report.prediction_seconds, Some(3600.0));
        assert_eq!(report.remaining_minutes, Some(45.0));
    }

    #[test]
    fn falls_back_to_file_progress_without_m73() {
        let report = report(json!({
            "print_stats": { "state": "printing", "print_duration": 600.0 },
            "virtual_sdcard": { "progress": 0.5 }
        }));

        assert_eq!(report.progress, Some(50.0));
        assert_eq!(report.remaining_minutes, Some(10.0));
    }

    /// The bug this guards against: `M73` steps only every percent or so, and
    /// re-dividing elapsed by a frozen progress between two steps made the
    /// remaining time count *up* at 4x real speed.
    #[test]
    fn eta_counts_down_while_progress_is_unchanged() {
        let mut eta = EtaTracker::default();
        let tick = |eta: &mut EtaTracker, elapsed: f64, progress: f64| {
            to_live(
                &status(json!({
                    "print_stats": { "state": "printing", "print_duration": elapsed },
                    "display_status": { "progress": progress }
                })),
                eta,
            )
        };

        // 900s at 25% ⇒ 3600s total, 45min left.
        let first = tick(&mut eta, 900.0, 0.25);
        assert_eq!(first.prediction_seconds, Some(3600.0));
        assert_eq!(first.remaining_minutes, Some(45.0));

        // Five more minutes of printing, progress not yet restepped: the total
        // is held and the remainder is five minutes smaller, not larger.
        let held = tick(&mut eta, 1200.0, 0.25);
        assert_eq!(held.prediction_seconds, Some(3600.0));
        assert_eq!(held.remaining_minutes, Some(40.0));

        // A new M73 value re-anchors the estimate: 1200s at 30% ⇒ 4000s total.
        let restepped = tick(&mut eta, 1200.0, 0.30);
        assert_eq!(restepped.prediction_seconds, Some(4020.0)); // 4000s, rounded to whole minutes
        assert_eq!(restepped.remaining_minutes, Some(47.0));

        // A restarted job (elapsed goes backwards) re-anchors too.
        let restarted = tick(&mut eta, 60.0, 0.30);
        assert_eq!(restarted.prediction_seconds, Some(180.0));
    }

    #[test]
    fn withholds_eta_until_the_job_is_measurably_under_way() {
        let report = report(json!({
            "print_stats": { "state": "printing", "print_duration": 30.0 },
            "display_status": { "progress": 0.002 }
        }));

        assert_eq!(report.progress, Some(0.2));
        assert!(report.prediction_seconds.is_none());
        assert!(report.remaining_minutes.is_none());
    }

    #[test]
    fn maps_standby_to_idle() {
        let report = report(json!({
            "print_stats": { "state": "standby", "filename": "" }
        }));
        assert_eq!(report.status.as_deref(), Some("IDLE"));
        assert!(report.filename.is_none());
    }

    #[test]
    fn falls_back_to_first_extruder_when_toolhead_is_missing() {
        let report = report(json!({
            "print_stats": { "state": "standby" },
            "extruder": { "temperature": 24.0 },
            "extruder1": { "temperature": 26.0 }
        }));
        assert_eq!(report.toolhead_temperature, Some(24.0));
        assert!(report.active_material.is_none());
    }

    #[test]
    fn materials_use_print_task_config_colors_and_types() {
        let report = report(json!({
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
        }));

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
        let report = report(json!({
            "print_stats": { "state": "printing", "filename": "x.gcode" },
            "extruder": { "temperature": 24.0 },
            "extruder1": { "temperature": 24.0 },
            "print_task_config": {
                "filament_color_rgba": ["FF0000FF", "00000000"],
                "filament_type": ["PLA", ""]
            }
        }));

        assert_eq!(report.materials[0].color, "#FF0000");
        assert_eq!(report.materials[0].kind, "PLA");
        // Transparent slot falls back to neutral color and generic kind label.
        assert_eq!(report.materials[1].color, "#9CA3AF");
        assert_eq!(report.materials[1].kind, "Filament");
    }
}
