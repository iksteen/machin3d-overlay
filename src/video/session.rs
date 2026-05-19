use std::collections::HashMap;

use anyhow::{bail, Context, Result};

use crate::{devices::DeviceEntry, secret::Secret};

use super::{stream::VideoState, VideoEndpoint};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VideoSession {
    pub(super) device_id: String,
    pub(super) access_code: Secret<String>,
}

pub(super) async fn resolve_session(
    state: &VideoState,
    requested_device_id: Option<&str>,
) -> Result<VideoSession> {
    select_session(
        state.registry.entries(),
        &state.endpoints_by_device,
        requested_device_id,
    )
}

pub(super) fn select_session<'a>(
    devices: impl IntoIterator<Item = &'a DeviceEntry>,
    endpoints_by_device: &HashMap<String, Vec<VideoEndpoint>>,
    requested_device_id: Option<&str>,
) -> Result<VideoSession> {
    let mut devices = devices.into_iter();
    let requested_device_id = requested_device_id
        .map(str::trim)
        .filter(|device_id| !device_id.is_empty());

    if let Some(requested_device_id) = requested_device_id {
        let Some(device) = devices.find(|device| device.id().trim() == requested_device_id) else {
            bail!("device `{requested_device_id}` is not known");
        };
        if !has_video_endpoint(endpoints_by_device, device.id()) {
            bail!("device `{requested_device_id}` has no known video endpoint");
        }
        return video_session(device).with_context(|| {
            format!("device `{requested_device_id}` did not include dev_access_code")
        });
    }

    let Some(device) = devices.find(|device| has_video_endpoint(endpoints_by_device, device.id()))
    else {
        bail!("no devices have known video endpoints");
    };
    video_session(device).context("first video-capable device did not include dev_access_code")
}

fn video_session(device: &DeviceEntry) -> Option<VideoSession> {
    let device_id = device.id().trim().to_owned();
    let access_code = device.access_code()?.to_owned();
    if device_id.is_empty() || access_code.is_empty() {
        return None;
    }
    Some(VideoSession {
        device_id,
        access_code: Secret::new(access_code),
    })
}

pub(super) async fn candidate_endpoints(state: &VideoState, device_id: &str) -> Vec<VideoEndpoint> {
    let endpoints = state
        .endpoints_by_device
        .get(device_id)
        .cloned()
        .unwrap_or_default();
    let remembered = state
        .remembered_endpoints
        .lock()
        .await
        .get(device_id)
        .cloned();

    order_endpoints(endpoints, remembered)
}

pub(super) fn order_endpoints(
    endpoints: Vec<VideoEndpoint>,
    remembered: Option<VideoEndpoint>,
) -> Vec<VideoEndpoint> {
    let Some(remembered) =
        remembered.filter(|endpoint| endpoints.iter().any(|candidate| candidate == endpoint))
    else {
        return endpoints;
    };

    let mut ordered = Vec::with_capacity(endpoints.len());
    ordered.push(remembered.clone());
    ordered.extend(
        endpoints
            .into_iter()
            .filter(|endpoint| endpoint != &remembered),
    );
    ordered
}

pub(super) async fn remember_endpoint(
    state: &VideoState,
    device_id: &str,
    endpoint: &VideoEndpoint,
) {
    if !has_video_endpoint_value(&state.endpoints_by_device, device_id, endpoint) {
        return;
    }
    state
        .remembered_endpoints
        .lock()
        .await
        .insert(device_id.to_owned(), endpoint.clone());
}

fn has_video_endpoint(
    endpoints_by_device: &HashMap<String, Vec<VideoEndpoint>>,
    device_id: &str,
) -> bool {
    endpoints_by_device
        .get(device_id)
        .is_some_and(|endpoints| !endpoints.is_empty())
}

fn has_video_endpoint_value(
    endpoints_by_device: &HashMap<String, Vec<VideoEndpoint>>,
    device_id: &str,
    endpoint: &VideoEndpoint,
) -> bool {
    endpoints_by_device
        .get(device_id)
        .is_some_and(|endpoints| endpoints.iter().any(|known| known == endpoint))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr};

    use serde_json::json;

    use crate::{bambu::CloudDevice, devices::DeviceRegistry, video::VideoEndpoint};

    use super::{order_endpoints, select_session};

    fn devices(values: Vec<serde_json::Value>) -> DeviceRegistry {
        DeviceRegistry::new(
            values
                .into_iter()
                .map(|value| serde_json::from_value::<CloudDevice>(value).unwrap())
                .collect(),
            Vec::new(),
        )
    }

    fn endpoint(value: &str) -> VideoEndpoint {
        VideoEndpoint::from_str(value).expect("endpoint should parse")
    }

    fn video_endpoints(device_ids: &[&str]) -> HashMap<String, Vec<VideoEndpoint>> {
        device_ids
            .iter()
            .map(|device_id| ((*device_id).to_owned(), vec![endpoint("192.168.1.50")]))
            .collect()
    }

    #[test]
    fn selected_session_uses_real_cloud_field_names() {
        let registry = devices(vec![json!({
            "dev_id": "printer-a",
            "dev_access_code": "12345678\n"
        })]);
        let session = select_session(registry.entries(), &video_endpoints(&["printer-a"]), None)
            .expect("single device should be selected");

        assert_eq!(session.device_id, "printer-a");
        assert_eq!(session.access_code.expose(), "12345678");
    }

    #[test]
    fn selected_session_uses_first_video_capable_device_by_default() {
        let registry = devices(vec![
            json!({"dev_id": "printer-a", "dev_access_code": "11111111"}),
            json!({"dev_id": "printer-b", "dev_access_code": "22222222"}),
        ]);
        let session = select_session(registry.entries(), &video_endpoints(&["printer-b"]), None)
            .expect("first video-capable device should be selected");

        assert_eq!(session.device_id, "printer-b");
        assert_eq!(session.access_code.expose(), "22222222");
    }

    #[test]
    fn selected_session_can_match_requested_device_id() {
        let registry = devices(vec![
            json!({"dev_id": "printer-a", "dev_access_code": "11111111"}),
            json!({"dev_id": "printer-b", "dev_access_code": "22222222"}),
        ]);
        let session = select_session(
            registry.entries(),
            &video_endpoints(&["printer-b"]),
            Some("printer-b"),
        )
        .expect("requested device should be selected");

        assert_eq!(session.device_id, "printer-b");
        assert_eq!(session.access_code.expose(), "22222222");
    }

    #[test]
    fn selected_session_rejects_unknown_requested_device_id() {
        let registry = devices(vec![json!({
            "dev_id": "printer-a",
            "dev_access_code": "11111111"
        })]);
        let error = select_session(
            registry.entries(),
            &video_endpoints(&["printer-a"]),
            Some("printer-b"),
        )
        .unwrap_err();

        assert!(error.to_string().contains("printer-b"));
    }

    #[test]
    fn selected_session_rejects_known_device_without_video_endpoint() {
        let registry = devices(vec![json!({
            "dev_id": "printer-a",
            "dev_access_code": "11111111"
        })]);

        let error =
            select_session(registry.entries(), &HashMap::new(), Some("printer-a")).unwrap_err();

        assert!(error.to_string().contains("printer-a"));
        assert!(error.to_string().contains("no known video endpoint"));
    }

    #[test]
    fn remembered_video_endpoint_is_tried_first() {
        let endpoints = order_endpoints(
            vec![
                endpoint("192.168.1.50"),
                endpoint("192.168.1.51:6001"),
                endpoint("192.168.1.52"),
            ],
            Some(endpoint("192.168.1.51:6001")),
        );

        assert_eq!(
            endpoints,
            [
                endpoint("192.168.1.51:6001"),
                endpoint("192.168.1.50"),
                endpoint("192.168.1.52"),
            ]
        );
    }
}
