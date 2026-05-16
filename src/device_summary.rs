use std::collections::HashMap;

use crate::{
    bambu::{AmsState, PrinterStatus, Tray},
    devices::KnownDevice,
    mqtt::{MqttDeviceState, PrintActivity},
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum TaskSource {
    #[default]
    PrinterStatus,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DeviceSummary {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) online: bool,
    pub(crate) task_name: Option<String>,
    pub(crate) title: Option<String>,
    pub(crate) filename: Option<String>,
    pub(crate) task_status: Option<String>,
    pub(crate) start_time: Option<String>,
    pub(crate) prediction: Option<f64>,
    pub(crate) progress: Option<f64>,
    pub(crate) thumbnail_task: Option<String>,
    pub(crate) weight: Option<String>,
    pub(crate) layer_current: Option<i64>,
    pub(crate) layer_total: Option<i64>,
    pub(crate) remaining_seconds: Option<f64>,
    pub(crate) toolhead_temperature: Option<f64>,
    pub(crate) bed_temperature: Option<f64>,
    pub(crate) fan_speed: Option<f64>,
    pub(crate) print_mode: Option<String>,
    pub(crate) ams_spools: Vec<Spool>,
    pub(crate) external_spool: Option<Spool>,
    pub(crate) is_printing: bool,
    pub(crate) task_source: TaskSource,
    pub(crate) plate_index: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct Spool {
    pub(crate) label: String,
    pub(crate) material: String,
    pub(crate) color: String,
    pub(crate) active: bool,
}

struct DeviceFields<'a> {
    device: &'a KnownDevice,
    live: Option<&'a MqttDeviceState>,
}

impl<'a> DeviceFields<'a> {
    fn new(device: &'a KnownDevice, live: Option<&'a MqttDeviceState>) -> Self {
        Self { device, live }
    }

    fn catalog_status(&self) -> &PrinterStatus {
        &self.device.status
    }

    fn report_status(&self) -> Option<&PrinterStatus> {
        self.live.map(|state| &state.report)
    }

    fn active_status(&self) -> Option<&PrinterStatus> {
        if let Some(live) = self.live {
            return live.is_active_task().then_some(&live.report);
        }
        let catalog_status = self.catalog_status();
        PrintActivity::from_report(catalog_status)
            .is_active_task()
            .then_some(catalog_status)
    }

    fn active_string(&self, pick: impl Fn(&PrinterStatus) -> Option<&String>) -> Option<String> {
        self.active_status().and_then(pick).cloned()
    }

    fn print_f64(&self, pick: impl Fn(&PrinterStatus) -> Option<f64>) -> Option<f64> {
        self.report_status()
            .and_then(&pick)
            .or_else(|| pick(self.catalog_status()))
    }

    fn active_f64(&self, pick: impl Fn(&PrinterStatus) -> Option<f64>) -> Option<f64> {
        self.active_status().and_then(pick)
    }

    fn active_i64(&self, pick: impl Fn(&PrinterStatus) -> Option<i64>) -> Option<i64> {
        self.active_status().and_then(pick)
    }

    fn ams(&self) -> Option<&AmsState> {
        self.report_status()
            .and_then(|status| status.ams.as_ref())
            .filter(|ams| ams.has_spool_data())
            .or(self.catalog_status().ams.as_ref())
    }

    fn external_tray(&self) -> Option<&Tray> {
        self.report_status()
            .and_then(|status| status.external_tray.as_ref())
            .filter(|tray| tray.has_spool_data())
            .or(self.catalog_status().external_tray.as_ref())
    }

    fn display_mode(&self) -> Option<String> {
        self.active_status().and_then(print_mode)
    }

    fn active_tray(&self) -> Option<i64> {
        self.active_status()
            .and_then(|status| status.ams.as_ref())
            .and_then(|ams| ams.tray_now)
    }

    fn task_id(&self) -> Option<String> {
        self.active_string(|print| print.task_id.as_ref())
    }

    fn device_id(&self) -> String {
        self.device.id.clone()
    }

    fn task_name(&self) -> Option<String> {
        self.active_string(|print| print.task_name.as_ref())
    }

    fn task_status(&self) -> Option<String> {
        self.active_string(|print| print.status.as_ref())
    }

    fn progress(&self) -> Option<f64> {
        self.active_f64(|print| print.progress)
    }

    fn prediction(&self) -> Option<f64> {
        self.active_f64(|print| print.prediction_seconds)
    }

    fn start_time(&self) -> Option<String> {
        self.active_string(|print| print.start_time.as_ref())
    }

    fn summary(&self) -> DeviceSummary {
        let device_id = self.device_id();
        let task_id = self.task_id();
        let task_name = self.task_name();
        let task_status = self.task_status();
        let progress = self.progress();
        let prediction = self.prediction();
        let start_time = self.start_time();
        let filename = self.active_string(|print| print.filename.as_ref());
        let active_tray = self.active_tray();
        let is_printing = self.active_status().is_some();
        let thumbnail_task = thumbnail_task(
            is_printing,
            task_id.as_deref(),
            filename.as_deref(),
            task_name.as_deref(),
            start_time.as_deref(),
        );

        DeviceSummary {
            id: device_id,
            name: self
                .device
                .name
                .clone()
                .unwrap_or_else(|| "Bambu printer".to_owned()),
            online: self.device.online.unwrap_or(true),
            task_name: task_name.clone(),
            title: task_name,
            filename: filename.clone(),
            task_status,
            start_time,
            prediction,
            progress,
            thumbnail_task,
            weight: self.active_string(|print| print.weight.as_ref()),
            layer_current: self.active_i64(|print| print.layer_current),
            layer_total: self.active_i64(|print| print.layer_total),
            remaining_seconds: self
                .active_f64(|print| print.remaining_minutes)
                .map(|minutes| minutes * 60.0),
            toolhead_temperature: self.print_f64(|print| print.toolhead_temperature),
            bed_temperature: self.print_f64(|print| print.bed_temperature),
            fan_speed: self.print_f64(|print| print.fan_speed),
            print_mode: self.display_mode(),
            ams_spools: ams_spools(self.ams(), active_tray),
            external_spool: external_spool(self.external_tray(), active_tray),
            is_printing,
            task_source: TaskSource::PrinterStatus,
            plate_index: None,
        }
    }
}

pub(crate) fn summarize_devices<'a>(
    devices: impl IntoIterator<Item = &'a KnownDevice>,
    states: &HashMap<String, MqttDeviceState>,
) -> Vec<DeviceSummary> {
    devices
        .into_iter()
        .map(|device| summarize_device(device, states))
        .collect()
}

fn summarize_device(
    device: &KnownDevice,
    states: &HashMap<String, MqttDeviceState>,
) -> DeviceSummary {
    let state = states.get(&device.id);
    let fields = DeviceFields::new(device, state);
    fields.summary()
}

fn thumbnail_task(
    has_print_status_task: bool,
    task_id: Option<&str>,
    filename: Option<&str>,
    task_name: Option<&str>,
    start_time: Option<&str>,
) -> Option<String> {
    if !has_print_status_task {
        return None;
    }
    task_id
        .or(filename)
        .or(task_name)
        .or(start_time)
        .map(str::trim)
        .filter(|task| !task.is_empty())
        .map(str::to_owned)
}

fn ams_spools(ams: Option<&AmsState>, active_tray: Option<i64>) -> Vec<Spool> {
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

fn external_spool(tray: Option<&Tray>, active_tray: Option<i64>) -> Option<Spool> {
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

fn print_mode(print_status: &PrinterStatus) -> Option<String> {
    if let Some(speed_level) = print_status.speed_level {
        return Some(match speed_level {
            1 => "Silent".to_owned(),
            2 => "Standard".to_owned(),
            3 => "Sport".to_owned(),
            4 => "Ludicrous".to_owned(),
            other => format!("Level {other}"),
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde::de::DeserializeOwned;
    use serde_json::{json, Value};

    use crate::{
        bambu::{CloudDevice, PrinterStatus, Tray},
        devices::KnownDevice,
        mqtt::MqttDeviceState,
    };

    use super::{spool_color, summarize_devices, TaskSource};

    fn decode<T: DeserializeOwned>(value: Value) -> T {
        serde_json::from_value(value).expect("fixture should match typed API shape")
    }

    fn device(value: Value) -> KnownDevice {
        KnownDevice::from_cloud(decode::<CloudDevice>(value)).expect("device should have an ID")
    }

    fn live(value: Value) -> MqttDeviceState {
        MqttDeviceState::from_report(decode::<PrinterStatus>(value))
    }

    #[test]
    fn summarize_devices_uses_matching_mqtt_report_fields_only() {
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
                        "gcode_state": "RUNNING",
                        "mc_percent": 1
                    }
            })),
        ];
        let states = HashMap::from([(
            "printer-a".to_owned(),
            live(json!({
                "gcode_state": "RUNNING",
                "mc_percent": 42,
                "bed_temper": 60
            })),
        )]);

        let summaries = summarize_devices(&devices, &states);

        assert_eq!(summaries[0].progress, Some(42.0));
        assert_eq!(summaries[0].toolhead_temperature, Some(210.0));
        assert_eq!(summaries[0].bed_temperature, Some(60.0));
        assert_eq!(summaries[1].progress, Some(1.0));
    }

    #[test]
    fn summarize_devices_keeps_cloud_spools_when_mqtt_report_is_empty() {
        let devices = vec![device(json!({
                    "dev_id": "printer-a",
                    "print": {
                        "gcode_state": "RUNNING",
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
        let states = HashMap::from([(
            "printer-a".to_owned(),
            live(json!({
                "gcode_state": "RUNNING",
                "mc_percent": 42,
                "ams": {"tray_now": "777", "ams": [{"id": 0, "tray": [{"id": 0, "tray_color": "00000000"}]}]},
                "vt_tray": {"id": 777, "tray_color": "00000000"}
            })),
        )]);

        let summary = summarize_devices(&devices, &states)
            .into_iter()
            .next()
            .unwrap();

        assert_eq!(summary.progress, Some(42.0));
        assert_eq!(summary.ams_spools.len(), 1);
        assert_eq!(summary.ams_spools[0].material, "PLA");
        assert_eq!(summary.ams_spools[0].color, "#FF0000");
        assert!(!summary.ams_spools[0].active);
        assert_eq!(summary.external_spool.as_ref().unwrap().material, "PETG");
        assert_eq!(summary.external_spool.as_ref().unwrap().color, "#336699");
        assert!(summary.external_spool.as_ref().unwrap().active);
    }

    #[test]
    fn summarize_devices_uses_catalog_status_and_spools() {
        let devices = vec![device(json!({
                    "dev_id": "printer-a",
                    "dev_name": "Office X1",
                    "dev_online": true,
                    "print": {
                        "gcode_state": "RUNNING",
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

        let summary = summarize_devices(&devices, &HashMap::new())
            .into_iter()
            .next()
            .unwrap();

        assert_eq!(summary.name, "Office X1");
        assert_eq!(summary.title.as_deref(), Some("Calibration cube"));
        assert_eq!(summary.task_source, TaskSource::PrinterStatus);
        assert_eq!(summary.progress, Some(25.0));
        assert_eq!(summary.prediction, Some(3600.0));
        assert_eq!(summary.weight, None);
        assert_eq!(summary.plate_index, None);
        assert_eq!(summary.thumbnail_task.as_deref(), Some("Calibration cube"));
        assert_eq!(summary.ams_spools.len(), 1);
        assert_eq!(summary.ams_spools[0].material, "PLA");
        assert_eq!(summary.ams_spools[0].color, "#FF0000");
        assert!(summary.ams_spools[0].active);
    }

    #[test]
    fn summarize_devices_ignores_stale_task_fields_when_printer_is_finished() {
        let devices = vec![device(json!({
            "dev_id": "printer-a",
            "dev_name": "Office X1"
        }))];
        let states = HashMap::from([(
            "printer-a".to_owned(),
            live(json!({
                "gcode_state": "FINISH",
                "subtask_name": "Calibration cube",
                "gcode_file": "calibration_cube.3mf",
                "mc_percent": 100,
                "layer_num": 20,
                "total_layer_num": 20,
                "nozzle_temper": 210,
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
            })),
        )]);

        let summary = summarize_devices(&devices, &states)
            .into_iter()
            .next()
            .unwrap();

        assert!(!summary.is_printing);
        assert_eq!(summary.title, None);
        assert_eq!(summary.filename, None);
        assert_eq!(summary.progress, None);
        assert_eq!(summary.layer_current, None);
        assert_eq!(summary.layer_total, None);
        assert_eq!(summary.thumbnail_task, None);
        assert_eq!(summary.toolhead_temperature, Some(210.0));
        assert_eq!(summary.bed_temperature, Some(60.0));
        assert_eq!(summary.ams_spools.len(), 1);
        assert!(!summary.ams_spools[0].active);
    }

    #[test]
    fn spool_color_normalizes_color_sources() {
        assert_eq!(
            spool_color(&decode::<Tray>(json!({"tray_color": "00ff00ff"}))).as_deref(),
            Some("#00FF00")
        );
        assert_eq!(
            spool_color(&decode::<Tray>(json!({"cols": ["336699ff"]}))).as_deref(),
            Some("#336699")
        );
        assert_eq!(
            spool_color(&decode::<Tray>(json!({"tray_color": "00000000"}))),
            None
        );
        assert_eq!(
            spool_color(&decode::<Tray>(json!({"tray_color": "xyz"}))),
            None
        );
    }
}
