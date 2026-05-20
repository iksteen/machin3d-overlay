use std::collections::HashMap;

use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use crate::{
    bambu::{printer_status_to_live, CloudDevice, PrinterStatus},
    devices::KnownDevice,
    live::{ConnectionStatus, DeviceConnection, DeviceLiveState},
};

use super::{summarize_devices, TaskSource};

fn decode<T: DeserializeOwned>(value: Value) -> T {
    serde_json::from_value(value).expect("fixture should match typed API shape")
}

fn device(value: Value) -> KnownDevice {
    KnownDevice::from_cloud(decode::<CloudDevice>(value)).expect("device should have an ID")
}

fn live(value: Value) -> DeviceLiveState {
    DeviceLiveState::from_report(printer_status_to_live(&decode::<PrinterStatus>(value)))
}

fn stale_live(value: Value) -> DeviceLiveState {
    DeviceLiveState::from_snapshot(
        printer_status_to_live(&decode::<PrinterStatus>(value)),
        None,
        DeviceConnection {
            key: Some("cloud".to_owned()),
            status: ConnectionStatus::Disconnected,
            error: Some("disconnected".to_owned()),
        },
    )
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

    let summaries = summarize_devices(&devices, &states, &HashMap::new());

    assert_eq!(summaries[0].service_status, ConnectionStatus::Connected);
    assert!(summaries[0].service_connected);
    assert_eq!(summaries[0].progress, Some(42.0));
    assert_eq!(summaries[0].toolhead_temperature, Some(210.0));
    assert_eq!(summaries[0].bed_temperature, Some(60.0));
    assert_eq!(summaries[1].progress, Some(1.0));
}

#[test]
fn summarize_devices_keeps_cloud_materials_when_mqtt_report_is_empty() {
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

    let summary = summarize_devices(&devices, &states, &HashMap::new())
        .into_iter()
        .next()
        .unwrap();

    assert_eq!(summary.progress, Some(42.0));
    assert_eq!(summary.materials.len(), 2);
    assert_eq!(summary.materials[0].label, "1");
    assert_eq!(summary.materials[0].kind, "PLA");
    assert_eq!(summary.materials[0].color, "#FF0000");
    assert!(!summary.materials[0].active);
    assert_eq!(summary.materials[1].label, "ext");
    assert_eq!(summary.materials[1].kind, "PETG");
    assert_eq!(summary.materials[1].color, "#336699");
    assert!(summary.materials[1].active);
}

#[test]
fn summarize_devices_uses_catalog_status_and_materials() {
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

    let summary = summarize_devices(&devices, &HashMap::new(), &HashMap::new())
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
    assert_eq!(summary.materials.len(), 1);
    assert_eq!(summary.materials[0].label, "1");
    assert_eq!(summary.materials[0].kind, "PLA");
    assert_eq!(summary.materials[0].color, "#FF0000");
    assert!(summary.materials[0].active);
}

#[test]
fn summarize_devices_uses_registered_connecting_connection_without_report() {
    let devices = vec![device(json!({
        "dev_id": "printer-a",
        "dev_name": "Office X1",
        "print": {
            "gcode_state": "RUNNING",
            "subtask_name": "Cloud catalog cube",
            "mc_percent": 12
        }
    }))];
    let connections = HashMap::from([(
        "printer-a".to_owned(),
        DeviceConnection {
            key: Some("cloud".to_owned()),
            status: ConnectionStatus::Connecting,
            error: None,
        },
    )]);

    let summary = summarize_devices(&devices, &HashMap::new(), &connections)
        .into_iter()
        .next()
        .unwrap();

    assert_eq!(summary.service_status, ConnectionStatus::Connecting);
    assert!(!summary.service_connected);
    assert_eq!(summary.service_error, None);
    assert!(!summary.is_printing);
    assert_eq!(summary.title, None);
    assert_eq!(summary.progress, None);
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

    let summary = summarize_devices(&devices, &states, &HashMap::new())
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
    assert_eq!(summary.materials.len(), 1);
    assert!(!summary.materials[0].active);
}

#[test]
fn summarize_devices_ignores_stale_mqtt_reports() {
    let devices = vec![device(json!({
        "dev_id": "printer-a",
        "dev_name": "Office X1",
        "print": {
            "gcode_state": "RUNNING",
            "subtask_name": "Cloud catalog cube",
            "mc_percent": 12,
            "nozzle_temper": 210,
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
            }
        }
    }))];
    let states = HashMap::from([(
        "printer-a".to_owned(),
        stale_live(json!({
            "gcode_state": "RUNNING",
            "subtask_name": "Calibration cube",
            "mc_percent": 42,
            "nozzle_temper": 210
        })),
    )]);

    let summary = summarize_devices(&devices, &states, &HashMap::new())
        .into_iter()
        .next()
        .unwrap();

    assert_eq!(summary.service_status, ConnectionStatus::Disconnected);
    assert!(!summary.service_connected);
    assert_eq!(summary.service_error.as_deref(), Some("disconnected"));
    assert!(!summary.is_printing);
    assert_eq!(summary.title, None);
    assert_eq!(summary.progress, None);
    assert_eq!(summary.toolhead_temperature, None);
    assert!(summary.materials.is_empty());
}
