use crate::bambu::{AmsState, Tray};

#[derive(Debug, Clone)]
pub(crate) struct Spool {
    pub(crate) label: String,
    pub(crate) material: String,
    pub(crate) color: String,
    pub(crate) active: bool,
}

pub(super) fn ams_spools(ams: Option<&AmsState>, active_tray: Option<i64>) -> Vec<Spool> {
    let Some(ams) = ams else {
        return Vec::new();
    };
    let mut spools = Vec::new();
    for (ams_index, ams_unit) in ams.ams.iter().enumerate() {
        for (tray_index, tray) in ams_unit.tray.iter().enumerate() {
            let ams_id = ams_unit.id.unwrap_or(ams_index as i64);
            let tray_id = tray.id.unwrap_or(tray_index as i64);
            let mut label = (tray_id + 1).to_string();
            if ams.ams.len() > 1 {
                label = format!("{}-{label}", ams_id + 1);
            }
            let active = active_tray.is_some_and(|active| active == ams_id * 4 + tray_id);
            if let Some(spool) = spool_summary(tray, label, active) {
                spools.push(spool);
            }
        }
    }
    spools
}

pub(super) fn external_spool(tray: Option<&Tray>, active_tray: Option<i64>) -> Option<Spool> {
    tray.and_then(|tray| {
        let active =
            active_tray.is_some_and(|active| tray.id.is_some_and(|tray_id| active == tray_id));
        spool_summary(tray, "ext".to_owned(), active)
    })
}

fn spool_summary(tray: &Tray, label: String, active: bool) -> Option<Spool> {
    let material = tray
        .material
        .clone()
        .or_else(|| tray.display_name.clone())
        .or_else(|| tray.sub_brand.clone())
        .or_else(|| tray.info_index.clone());
    let color = spool_color(tray);
    if material.is_none() && color.is_none() {
        return None;
    }
    Some(Spool {
        label,
        material: material.unwrap_or_else(|| "Filament".to_owned()),
        color: color.unwrap_or_else(|| "#9CA3AF".to_owned()),
        active,
    })
}

fn spool_color(tray: &Tray) -> Option<String> {
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

    use crate::bambu::Tray;

    use super::spool_color;

    #[test]
    fn spool_color_normalizes_color_sources() {
        assert_eq!(
            spool_color(&tray(json!({"tray_color": "00ff00ff"}))).as_deref(),
            Some("#00FF00")
        );
        assert_eq!(
            spool_color(&tray(json!({"cols": ["336699ff"]}))).as_deref(),
            Some("#336699")
        );
        assert_eq!(spool_color(&tray(json!({"tray_color": "00000000"}))), None);
        assert_eq!(spool_color(&tray(json!({"tray_color": "xyz"}))), None);
    }

    fn tray(value: serde_json::Value) -> Tray {
        serde_json::from_value(value).expect("fixture should match tray fields")
    }
}
