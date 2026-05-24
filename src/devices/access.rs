use anyhow::Result;

use crate::{
    bambu::{
        cloud::{bound_cloud_devices, CloudSession},
        local::BambuLocalEndpointConfig,
        BambuCloudDevice,
    },
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
    devices: Option<Vec<BambuCloudDevice>>,
}

impl<'a> BindCatalog<'a> {
    pub(super) fn new(
        cloud: Option<&'a CloudSession>,
        devices: Option<Vec<BambuCloudDevice>>,
    ) -> Self {
        Self { cloud, devices }
    }

    pub(super) async fn load_device_from_cloud(
        &mut self,
        device_id: &str,
    ) -> Result<Option<BambuCloudDevice>> {
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
    endpoint: &mut BambuLocalEndpointConfig,
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

/// Hydrate a Bambu entry's access code (from an explicit video endpoint
/// or from cloud `/bind` metadata) and refresh its name/online fields
/// from `/bind` when those are missing. No-ops for non-Bambu entries.
pub(super) async fn hydrate_device_entry(
    entry: &mut DeviceEntry,
    video: Option<&VideoEndpoint>,
    bind_catalog: &mut BindCatalog<'_>,
) -> Result<()> {
    let device_id = entry.id().to_owned();
    let Some(bambu) = entry.bambu_mut() else {
        return Ok(());
    };
    if !bambu.has_access_code() {
        if let Some(video) = video {
            bambu.access_code = video.access_code().map(|code| Secret::new(code.to_owned()));
        }
    }
    if bambu.has_access_code() {
        return Ok(());
    }

    let Some(metadata) = bind_catalog.load_device_from_cloud(&device_id).await? else {
        return Ok(());
    };

    // Re-borrow after the async `load_device_from_cloud` boundary; the
    // entry's variant doesn't change so this always matches.
    let bambu = entry
        .bambu_mut()
        .expect("entry variant cannot change between borrows");
    bambu.access_code = metadata.access_code;

    let device = entry.device_mut();
    if !has_text(device.name.as_deref()) {
        device.name = metadata.name;
    }
    device.online = device.online.or(metadata.online);
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
    use crate::{bambu::local::BambuLocalEndpointConfig, video::VideoEndpoint};

    fn local_config(value: &str) -> BambuLocalEndpointConfig {
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
            local
                .access_code
                .as_ref()
                .map(|code| code.expose().as_str()),
            Some("12345678")
        );
    }
}
