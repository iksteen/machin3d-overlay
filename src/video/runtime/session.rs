use anyhow::{bail, Context, Result};

use crate::devices::DeviceEntry;

use super::VideoRuntimeInner;
use crate::video::VideoEndpoint;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VideoSession {
    pub(super) device_id: String,
    pub(super) access_code: String,
}

pub(super) async fn resolve_session(
    inner: &VideoRuntimeInner,
    requested_device_id: Option<&str>,
) -> Result<VideoSession> {
    select_session(inner.registry.entries(), requested_device_id)
}

pub(super) fn select_session<'a>(
    devices: impl IntoIterator<Item = &'a DeviceEntry>,
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
        return video_session(device).with_context(|| {
            format!("device `{requested_device_id}` did not include dev_access_code")
        });
    }

    let Some(device) = devices.next() else {
        bail!("no devices are known");
    };
    video_session(device).context("first device did not include dev_access_code")
}

fn video_session(device: &DeviceEntry) -> Option<VideoSession> {
    let device_id = device.id().trim().to_owned();
    let access_code = device.access_code()?.to_owned();
    if device_id.is_empty() || access_code.is_empty() {
        return None;
    }
    Some(VideoSession {
        device_id,
        access_code,
    })
}

pub(super) async fn candidate_endpoints(
    inner: &VideoRuntimeInner,
    device_id: &str,
) -> Vec<VideoEndpoint> {
    let endpoints = inner.endpoints.clone();
    let remembered = inner.endpoint_map.lock().await.get(device_id).cloned();

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
    inner: &VideoRuntimeInner,
    device_id: &str,
    endpoint: &VideoEndpoint,
) {
    inner
        .endpoint_map
        .lock()
        .await
        .insert(device_id.to_owned(), endpoint.clone());
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;

    use crate::{
        bambu::CloudDevice,
        devices::DeviceRegistry,
        video::{runtime::session::select_session, VideoEndpoint},
    };

    use super::order_endpoints;

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

    #[test]
    fn selected_session_uses_real_cloud_field_names() {
        let registry = devices(vec![json!({
            "dev_id": "printer-a",
            "dev_access_code": "12345678\n"
        })]);
        let session =
            select_session(registry.entries(), None).expect("single device should be selected");

        assert_eq!(session.device_id, "printer-a");
        assert_eq!(session.access_code, "12345678");
    }

    #[test]
    fn selected_session_uses_first_stable_device_by_default() {
        let registry = devices(vec![
            json!({"dev_id": "printer-a", "dev_access_code": "11111111"}),
            json!({"dev_id": "printer-b", "dev_access_code": "22222222"}),
        ]);
        let session =
            select_session(registry.entries(), None).expect("first device should be selected");

        assert_eq!(session.device_id, "printer-a");
        assert_eq!(session.access_code, "11111111");
    }

    #[test]
    fn selected_session_can_match_requested_device_id() {
        let registry = devices(vec![
            json!({"dev_id": "printer-a", "dev_access_code": "11111111"}),
            json!({"dev_id": "printer-b", "dev_access_code": "22222222"}),
        ]);
        let session = select_session(registry.entries(), Some("printer-b"))
            .expect("requested device should be selected");

        assert_eq!(session.device_id, "printer-b");
        assert_eq!(session.access_code, "22222222");
    }

    #[test]
    fn selected_session_rejects_unknown_requested_device_id() {
        let registry = devices(vec![json!({
            "dev_id": "printer-a",
            "dev_access_code": "11111111"
        })]);
        let error = select_session(registry.entries(), Some("printer-b")).unwrap_err();

        assert!(error.to_string().contains("printer-b"));
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
