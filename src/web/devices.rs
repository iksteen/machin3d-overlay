use std::collections::HashSet;

use axum::{extract::State, Json};
use serde::Serialize;

use crate::devices::{DeviceCapabilities, DeviceEntry, DeviceRegistry};

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
    let runtime_video_ids = state.known_video_device_ids().await;
    Json(known_devices_payload(state.devices(), &runtime_video_ids))
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
                let has_video = runtime_video_ids.contains(entry.id());

                KnownDevicePayload {
                    id: device.id.clone(),
                    name: device.name.clone(),
                    online: device.online,
                    source: device_source_label(entry),
                    paths: device_paths(&device.id, has_video),
                }
            })
            .collect(),
    }
}

/// Whether the device's live data ultimately comes from the Bambu cloud
/// MQTT broker (`"cloud"`) or from a printer-local MQTT broker
/// (`"local"`). Moonraker devices are always local; Bambu devices are
/// local when a `--bbl-local-device` was configured for them.
fn device_source_label(entry: &DeviceEntry) -> &'static str {
    match entry.capabilities() {
        DeviceCapabilities::Bambu(bambu) if bambu.local_mqtt.is_some() => "local",
        DeviceCapabilities::Bambu(_) => "cloud",
        DeviceCapabilities::Moonraker(_) => "local",
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
        bambu::{
            local::{BambuLocalDevice, BambuLocalEndpoint},
            BambuCloudDevice,
        },
        devices::DeviceRegistry,
        secret::Secret,
    };

    #[test]
    fn known_devices_payload_includes_paths_without_access_codes() {
        let devices = DeviceRegistry::new(
            vec![BambuCloudDevice {
                id: Some("printer-b".to_owned()),
                name: Some("Garage".to_owned()),
                online: Some(false),
                access_code: Some(Secret::new("87654321".to_owned())),
                ..BambuCloudDevice::default()
            }],
            vec![BambuLocalDevice {
                id: "printer a/1".to_owned(),
                endpoint: {
                    let mut endpoint = BambuLocalEndpoint::new("192.168.1.50", 8883, "12345678");
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
            "/devices/printer%20a%2F1/horizontal"
        );
        assert_eq!(
            value["devices"][1]["paths"]["vertical"],
            "/devices/printer%20a%2F1/vertical"
        );
        assert_eq!(
            value["devices"][1]["paths"]["thumbnail"],
            "/devices/printer%20a%2F1/thumbnail"
        );
        assert_eq!(
            value["devices"][1]["paths"]["video"],
            "/devices/printer%20a%2F1/video.mjpeg"
        );

        let value = serde_json::to_value(known_devices_payload(
            &devices,
            &HashSet::from(["printer-b".to_owned()]),
        ))
        .unwrap();
        assert_eq!(
            value["devices"][0]["paths"]["video"],
            "/devices/printer-b/video.mjpeg"
        );
    }
}
