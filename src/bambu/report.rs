//! Convert Bambu's raw MQTT `PrinterStatus` into the vendor-neutral
//! `live::PrinterReport` consumed by the summary layer.
//!
//! The summary layer never sees `AmsState` / `Tray` / `speed_level` — those
//! get folded into a flat `Vec<Material>` and a `print_mode` string here.

use crate::live::{Material, PrinterReport};

use super::{AmsState, PrinterStatus, Tray};

pub(crate) fn to_live(status: &PrinterStatus) -> PrinterReport {
    let active_tray = status.ams.as_ref().and_then(|ams| ams.tray_now);
    let materials = materials(status.ams.as_ref(), status.external_tray.as_ref());
    let active_material =
        active_tray.and_then(|tray| active_material_label(&materials, status, tray));
    PrinterReport {
        task_id: status.task_id.clone(),
        task_name: status.task_name.clone(),
        status: status.status.clone(),
        filename: status.filename.clone(),
        start_time: status.start_time.clone(),
        progress: status.progress,
        prediction_seconds: status.prediction_seconds,
        remaining_minutes: status.remaining_minutes,
        weight: status.weight.clone(),
        layer_current: status.layer_current,
        layer_total: status.layer_total,
        toolhead_temperature: status.toolhead_temperature,
        bed_temperature: status.bed_temperature,
        fan_speed: status.fan_speed,
        print_mode: print_mode(status.speed_level),
        materials,
        active_material,
    }
}

fn active_material_label(
    materials: &[Material],
    status: &PrinterStatus,
    active_tray: i64,
) -> Option<String> {
    if let Some(external) = status.external_tray.as_ref() {
        if external.id == Some(active_tray) {
            return Some("ext".to_owned());
        }
    }
    let ams = status.ams.as_ref()?;
    for (ams_index, ams_unit) in ams.ams.iter().enumerate() {
        for (tray_index, tray) in ams_unit.tray.iter().enumerate() {
            let ams_id = ams_unit.id.unwrap_or(ams_index as i64);
            let tray_id = tray.id.unwrap_or(tray_index as i64);
            if active_tray == ams_id * 4 + tray_id {
                let label = if ams.ams.len() > 1 {
                    format!("{}-{}", ams_id + 1, tray_id + 1)
                } else {
                    (tray_id + 1).to_string()
                };
                if materials.iter().any(|material| material.label == label) {
                    return Some(label);
                }
            }
        }
    }
    None
}

fn print_mode(speed_level: Option<i64>) -> Option<String> {
    speed_level.map(|level| match level {
        1 => "Silent".to_owned(),
        2 => "Standard".to_owned(),
        3 => "Sport".to_owned(),
        4 => "Ludicrous".to_owned(),
        other => format!("Level {other}"),
    })
}

fn materials(ams: Option<&AmsState>, external_tray: Option<&Tray>) -> Vec<Material> {
    let mut materials = ams_materials(ams);
    if let Some(external) = external_material(external_tray) {
        materials.push(external);
    }
    materials
}

fn ams_materials(ams: Option<&AmsState>) -> Vec<Material> {
    let Some(ams) = ams else {
        return Vec::new();
    };
    let mut materials = Vec::new();
    for (ams_index, ams_unit) in ams.ams.iter().enumerate() {
        for (tray_index, tray) in ams_unit.tray.iter().enumerate() {
            let ams_id = ams_unit.id.unwrap_or(ams_index as i64);
            let tray_id = tray.id.unwrap_or(tray_index as i64);
            let mut label = (tray_id + 1).to_string();
            if ams.ams.len() > 1 {
                label = format!("{}-{label}", ams_id + 1);
            }
            if let Some(material) = material_summary(tray, label) {
                materials.push(material);
            }
        }
    }
    materials
}

fn external_material(tray: Option<&Tray>) -> Option<Material> {
    tray.and_then(|tray| material_summary(tray, "ext".to_owned()))
}

fn material_summary(tray: &Tray, label: String) -> Option<Material> {
    let kind = tray
        .material
        .clone()
        .or_else(|| tray.display_name.clone())
        .or_else(|| tray.sub_brand.clone())
        .or_else(|| tray.info_index.clone());
    let color = material_color(tray);
    if kind.is_none() && color.is_none() {
        return None;
    }
    Some(Material {
        label,
        kind: kind.unwrap_or_else(|| "Filament".to_owned()),
        color: color.unwrap_or_else(|| "#9CA3AF".to_owned()),
        active: false,
    })
}

fn material_color(tray: &Tray) -> Option<String> {
    let color = tray.color.as_ref().or_else(|| tray.cols.first())?;
    let normalized = color.trim().trim_start_matches('#');
    if normalized.len() < 6 {
        return None;
    }
    let rgb = &normalized[..6];
    if rgb.eq_ignore_ascii_case("000000") && normalized.get(6..8) == Some("00") {
        return None;
    }
    u32::from_str_radix(rgb, 16).ok()?;
    Some(format!("#{rgb}", rgb = rgb.to_ascii_uppercase()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::bambu::{PrinterStatus, Tray};

    use super::{material_color, to_live};

    #[test]
    fn material_color_normalizes_color_sources() {
        assert_eq!(
            material_color(&tray(json!({"tray_color": "00ff00ff"}))).as_deref(),
            Some("#00FF00")
        );
        assert_eq!(
            material_color(&tray(json!({"cols": ["336699ff"]}))).as_deref(),
            Some("#336699")
        );
        assert_eq!(
            material_color(&tray(json!({"tray_color": "00000000"}))),
            None
        );
        assert_eq!(material_color(&tray(json!({"tray_color": "xyz"}))), None);
    }

    #[test]
    fn to_live_packs_materials_and_print_mode() {
        let status: PrinterStatus = serde_json::from_value(json!({
            "gcode_state": "RUNNING",
            "mc_percent": 25,
            "spd_lvl": 2,
            "ams": {
                "tray_now": "0",
                "ams": [
                    {
                        "id": 0,
                        "tray": [
                            { "id": 0, "tray_type": "PLA", "tray_color": "ff0000ff" }
                        ]
                    }
                ]
            },
            "vt_tray": { "id": 255, "tray_type": "PETG", "tray_color": "336699ff" }
        }))
        .unwrap();

        let report = to_live(&status);

        assert_eq!(report.status.as_deref(), Some("RUNNING"));
        assert_eq!(report.progress, Some(25.0));
        assert_eq!(report.print_mode.as_deref(), Some("Standard"));
        assert_eq!(report.materials.len(), 2);
        assert_eq!(report.materials[0].label, "1");
        assert_eq!(report.materials[0].kind, "PLA");
        assert!(!report.materials[0].active);
        assert_eq!(report.materials[1].label, "ext");
        assert_eq!(report.materials[1].kind, "PETG");
        assert!(!report.materials[1].active);
        assert_eq!(report.active_material.as_deref(), Some("1"));
    }

    fn tray(value: serde_json::Value) -> Tray {
        serde_json::from_value(value).expect("fixture should match tray fields")
    }
}
