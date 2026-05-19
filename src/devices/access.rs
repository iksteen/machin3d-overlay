use anyhow::Result;

use crate::{
    bambu::CloudDevice,
    cloud::{bound_cloud_devices, CloudSession},
    local::LocalEndpointConfig,
    secret::Secret,
    video::VideoEndpoint,
};

use super::registry::DeviceEntry;

/// Lazy access to the Bambu Cloud `/bind` device catalog, used to fill in
/// missing access codes during local-device or explicit-video resolution. The
/// cloud call is made at most once; either the caller seeded the device list
/// up front (cloud enumeration mode) or the first hydration request triggers
/// the fetch.
pub(super) struct BindCatalog<'a> {
    cloud: Option<&'a CloudSession>,
    devices: Option<Vec<CloudDevice>>,
}

impl<'a> BindCatalog<'a> {
    pub(super) fn new(cloud: Option<&'a CloudSession>, devices: Option<Vec<CloudDevice>>) -> Self {
        Self { cloud, devices }
    }

    pub(super) async fn load_device_from_cloud(
        &mut self,
        device_id: &str,
    ) -> Result<Option<CloudDevice>> {
        if self.devices.is_none() {
            self.devices = Some(bound_cloud_devices(self.cloud).await?);
        }

        Ok(self
            .devices
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .find(|device| device.id.as_deref().map(str::trim) == Some(device_id))
            .cloned())
    }
}

pub(super) async fn hydrate_local_config(
    device_id: &str,
    endpoint: &mut LocalEndpointConfig,
    video: Option<&VideoEndpoint>,
    bind_catalog: &mut BindCatalog<'_>,
) -> Result<()> {
    if !has_access_code(endpoint.access_code.as_ref()) {
        if let Some(video) = video {
            endpoint.access_code = video.access_code().map(|code| Secret::new(code.to_owned()));
        }
    }
    if !has_access_code(endpoint.access_code.as_ref()) {
        if let Some(metadata) = bind_catalog.load_device_from_cloud(device_id).await? {
            endpoint.access_code = metadata.access_code;
            if !has_text(endpoint.name.as_deref()) {
                endpoint.name = metadata.name;
            }
        }
    }
    Ok(())
}

pub(super) async fn hydrate_device_entry(
    entry: &mut DeviceEntry,
    video: Option<&VideoEndpoint>,
    bind_catalog: &mut BindCatalog<'_>,
) -> Result<()> {
    if !entry.has_access_code() {
        if let Some(video) = video {
            entry.set_access_code(video.access_code().map(|code| Secret::new(code.to_owned())));
        }
    }
    if !entry.has_access_code() {
        let device_id = entry.id().to_owned();
        if let Some(metadata) = bind_catalog.load_device_from_cloud(&device_id).await? {
            entry.set_access_code(metadata.access_code);
            let device = entry.device_mut();
            if !has_text(device.name.as_deref()) {
                device.name = metadata.name;
            }
            device.online = device.online.or(metadata.online);
        }
    }
    Ok(())
}

pub(super) fn has_text(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

fn has_access_code(access_code: Option<&Secret<String>>) -> bool {
    has_text(access_code.map(|code| code.expose().as_str()))
}

#[cfg(test)]
mod tests {
    use super::{hydrate_local_config, BindCatalog};
    use crate::{local::LocalEndpointConfig, video::VideoEndpoint};

    fn local_config(value: &str) -> LocalEndpointConfig {
        value.parse().expect("local config should parse")
    }

    fn video_endpoint(value: &str) -> VideoEndpoint {
        value.parse().expect("video endpoint should parse")
    }

    #[tokio::test]
    async fn local_config_can_use_matching_video_access_code() {
        let video = video_endpoint("192.168.1.50,12345678");
        let mut local = local_config("192.168.1.50");
        let mut bind_catalog = BindCatalog::new(None, None);

        hydrate_local_config("printer-a", &mut local, Some(&video), &mut bind_catalog)
            .await
            .unwrap();

        assert_eq!(
            local.access_code.as_ref().map(|code| code.expose().as_str()),
            Some("12345678")
        );
    }
}
