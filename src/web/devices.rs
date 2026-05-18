use std::collections::HashSet;

use axum::{extract::State, Json};
use serde::Serialize;

use crate::devices::{DeviceRegistry, DeviceSource};

use super::{paths, AppState};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct KnownDevicesPayload {
    devices: Vec<KnownDevicePayload>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct KnownDevicePayload {
    id: String,
    name: Option<String>,
    online: Option<bool>,
    source: &'static str,
    paths: KnownDevicePaths,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct KnownDevicePaths {
    horizontal: String,
    vertical: String,
    thumbnail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    video: Option<String>,
}

pub(super) async fn known_devices(State(state): State<AppState>) -> Json<KnownDevicesPayload> {
    let runtime_video_ids = state.video.known_device_ids().await;
    Json(known_devices_payload(&state.devices, &runtime_video_ids))
}

fn known_devices_payload(
    registry: &DeviceRegistry,
    runtime_video_ids: &HashSet<String>,
) -> KnownDevicesPayload {
    KnownDevicesPayload {
        devices: registry
            .entries()
            .iter()
            .map(|entry| {
                let device = entry.device();
                let has_video = entry.has_access_code() && runtime_video_ids.contains(entry.id());

                KnownDevicePayload {
                    id: device.id.clone(),
                    name: device.name.clone(),
                    online: device.online,
                    source: device_source_label(entry.source()),
                    paths: device_paths(&device.id, has_video),
                }
            })
            .collect(),
    }
}

fn device_source_label(source: DeviceSource) -> &'static str {
    match source {
        DeviceSource::Cloud => "cloud",
        DeviceSource::Local => "local",
    }
}

fn device_paths(device_id: &str, has_video: bool) -> KnownDevicePaths {
    KnownDevicePaths {
        horizontal: paths::horizontal_overlay(device_id),
        vertical: paths::vertical_overlay(device_id),
        thumbnail: paths::thumbnail(device_id),
        video: has_video.then(|| paths::video(device_id)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::known_devices_payload;
    use crate::{
        bambu::CloudDevice,
        devices::DeviceRegistry,
        local::{LocalDevice, LocalEndpoint},
    };

    #[test]
    fn known_devices_payload_includes_paths_without_access_codes() {
        let devices = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer-b".to_owned()),
                name: Some("Garage".to_owned()),
                online: Some(false),
                access_code: Some("87654321".to_owned()),
                ..CloudDevice::default()
            }],
            vec![LocalDevice {
                id: "printer a/1".to_owned(),
                endpoint: {
                    let mut endpoint = LocalEndpoint::new("192.168.1.50", 8883, "12345678");
                    endpoint.name = Some("Office".to_owned());
                    endpoint
                },
            }],
        );

        let value = serde_json::to_value(known_devices_payload(
            &devices,
            &HashSet::from(["printer a/1".to_owned()]),
        ))
        .unwrap();
        let json = value.to_string();
        assert!(!json.contains("12345678"));
        assert!(!json.contains("87654321"));
        assert!(!json.contains("accessCode"));
        assert_eq!(value["devices"][0]["source"], "cloud");
        assert!(value["devices"][0]["paths"].get("video").is_none());
        assert_eq!(value["devices"][1]["source"], "local");
        assert_eq!(
            value["devices"][1]["paths"]["horizontal"],
            "/overlay?device=printer+a%2F1"
        );
        assert_eq!(
            value["devices"][1]["paths"]["vertical"],
            "/vertical?device=printer+a%2F1"
        );
        assert_eq!(
            value["devices"][1]["paths"]["thumbnail"],
            "/api/thumbnail?device=printer+a%2F1"
        );
        assert_eq!(
            value["devices"][1]["paths"]["video"],
            "/api/video.mjpeg?device=printer+a%2F1"
        );

        let value = serde_json::to_value(known_devices_payload(
            &devices,
            &HashSet::from(["printer-b".to_owned()]),
        ))
        .unwrap();
        assert_eq!(
            value["devices"][0]["paths"]["video"],
            "/api/video.mjpeg?device=printer-b"
        );
    }
}
