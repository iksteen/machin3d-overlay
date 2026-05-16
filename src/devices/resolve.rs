use std::collections::HashSet;

use anyhow::{Context, Result};
use tracing::info;

use crate::{
    cloud::{bound_cloud_devices, explicit_cloud_devices, CloudSession},
    local::{infer_local_device_id, LocalDevice, LocalEndpointArg},
    video::{infer_video_device_id, VideoEndpoint},
};

use super::{
    metadata::BindCatalog,
    registry::{DeviceRegistry, KnownDevice},
};

pub(crate) async fn resolve_devices(
    cloud: Option<&CloudSession>,
    cloud_configs: &[String],
    local_configs: &[LocalEndpointArg],
    video_endpoints: &[VideoEndpoint],
) -> Result<DeviceRegistry> {
    let explicit_video = resolve_explicit_video_endpoints(video_endpoints).await?;
    let enumerate_cloud_catalog =
        should_enumerate_cloud_catalog(cloud.is_some(), cloud_configs, local_configs);
    let cloud_devices = if enumerate_cloud_catalog {
        bound_cloud_devices(cloud).await?
    } else {
        explicit_cloud_devices(cloud_configs)
    };
    let mut bind_catalog = BindCatalog::new(
        cloud,
        enumerate_cloud_catalog.then(|| cloud_devices.clone()),
    );
    let local = resolve_local_devices(local_configs, &explicit_video, &mut bind_catalog).await?;

    let mut registry = DeviceRegistry::new(cloud_devices, local);
    attach_explicit_video(&mut registry, explicit_video, &mut bind_catalog).await?;
    if registry.is_empty() {
        anyhow::bail!(
            "no devices configured; run `bambu-overlay login`, set --cloud-device, or set --local-device"
        );
    }

    Ok(registry)
}

async fn attach_explicit_video(
    registry: &mut DeviceRegistry,
    video_endpoints: Vec<(String, VideoEndpoint)>,
    bind_catalog: &mut BindCatalog<'_>,
) -> Result<()> {
    for (device_id, video) in video_endpoints {
        let Some(entry) = registry.get_mut(&device_id) else {
            anyhow::bail!(
                "--video-device `{video}` is for device `{device_id}`, but no matching cloud or local device is configured"
            );
        };
        resolve_known_device_access(entry.device_mut(), &video, bind_catalog).await?;
        entry.set_explicit_video(video);
    }
    Ok(())
}

async fn resolve_local_devices(
    configs: &[LocalEndpointArg],
    video_endpoints: &[(String, VideoEndpoint)],
    bind_catalog: &mut BindCatalog<'_>,
) -> Result<Vec<LocalDevice>> {
    let mut devices = Vec::with_capacity(configs.len());
    let mut seen = HashSet::new();
    for config in configs {
        let device_id = infer_local_device_id(config).await.with_context(|| {
            format!(
                "could not infer device ID for --local-device `{}`",
                config.endpoint()
            )
        })?;
        if !seen.insert(device_id.clone()) {
            anyhow::bail!("--local-device resolves duplicate device id `{device_id}`");
        }
        info!(
            device_id = %device_id,
            endpoint = %config.endpoint(),
            "inferred local device ID from MQTT certificate"
        );
        devices.push(
            resolve_local_device_access(device_id, config.clone(), video_endpoints, bind_catalog)
                .await?,
        );
    }
    Ok(devices)
}

async fn resolve_local_device_access(
    device_id: String,
    mut endpoint: LocalEndpointArg,
    video_endpoints: &[(String, VideoEndpoint)],
    bind_catalog: &mut BindCatalog<'_>,
) -> Result<LocalDevice> {
    if !has_access_code(endpoint.access_code.as_deref()) {
        if let Some(video) = explicit_video_for_device(video_endpoints, &device_id) {
            endpoint.access_code = video.access_code().map(str::to_owned);
        }
    }
    if !has_access_code(endpoint.access_code.as_deref()) {
        if let Some(metadata) = bind_catalog.device(&device_id).await? {
            endpoint.access_code = metadata.access_code;
            if !has_text(endpoint.name.as_deref()) {
                endpoint.name = metadata.name;
            }
        }
    }
    if !has_text(endpoint.name.as_deref()) {
        endpoint.name = Some(device_id.clone());
    }
    finalize_local_device(device_id, endpoint)
}

async fn resolve_explicit_video_endpoints(
    endpoints: &[VideoEndpoint],
) -> Result<Vec<(String, VideoEndpoint)>> {
    let mut resolved = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        let device_id = infer_video_device_id(endpoint).await.with_context(|| {
            format!("could not infer device ID for --video-device `{endpoint}`")
        })?;
        resolved.push((device_id, endpoint.clone()));
    }
    Ok(resolved)
}

fn should_enumerate_cloud_catalog(
    cloud_available: bool,
    cloud_configs: &[String],
    local_configs: &[LocalEndpointArg],
) -> bool {
    cloud_available && cloud_configs.is_empty() && local_configs.is_empty()
}

async fn resolve_known_device_access(
    device: &mut KnownDevice,
    video: &VideoEndpoint,
    bind_catalog: &mut BindCatalog<'_>,
) -> Result<()> {
    if !device.has_access_code() {
        device.access_code = video.access_code().map(str::to_owned);
    }
    if !device.has_access_code() {
        let device_id = device.id.clone();
        if let Some(metadata) = bind_catalog.device(&device_id).await? {
            device.access_code = metadata.access_code;
            if !has_text(device.name.as_deref()) {
                device.name = metadata.name;
            }
            device.online = device.online.or(metadata.online);
        }
    }
    Ok(())
}

fn explicit_video_for_device<'a>(
    video_endpoints: &'a [(String, VideoEndpoint)],
    device_id: &str,
) -> Option<&'a VideoEndpoint> {
    video_endpoints
        .iter()
        .find(|(video_device_id, _)| video_device_id == device_id)
        .map(|(_, endpoint)| endpoint)
}

fn has_access_code(access_code: Option<&str>) -> bool {
    has_text(access_code)
}

fn has_text(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

fn finalize_local_device(device_id: String, endpoint: LocalEndpointArg) -> Result<LocalDevice> {
    let access_code = endpoint
        .access_code
        .as_deref()
        .filter(|access_code| !access_code.trim().is_empty())
        .map(str::to_owned)
        .with_context(|| {
            format!(
                "--local-device `{}` is missing an access code; provide ACCESS_CODE or cloud metadata that exposes dev_access_code",
                device_id
            )
        })?;
    Ok(LocalDevice {
        id: device_id,
        endpoint: endpoint.into_endpoint(access_code),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        attach_explicit_video, resolve_local_device_access, should_enumerate_cloud_catalog,
    };
    use crate::{
        bambu::CloudDevice,
        devices::{metadata::BindCatalog, DeviceRegistry},
        local::LocalEndpointArg,
        video::VideoEndpoint,
    };

    fn local_arg(value: &str) -> LocalEndpointArg {
        value.parse().expect("local device should parse")
    }

    fn endpoint(value: &str) -> VideoEndpoint {
        value.parse().expect("video endpoint should parse")
    }

    fn explicit_video_endpoint(device_id: &str, value: &str) -> (String, VideoEndpoint) {
        (device_id.to_owned(), endpoint(value))
    }

    #[tokio::test]
    async fn local_device_name_defaults_to_device_id_when_missing() {
        let mut bind_catalog = BindCatalog::new(None, None);
        let device = resolve_local_device_access(
            "printer-a".to_owned(),
            local_arg("192.168.1.50,12345678"),
            &[],
            &mut bind_catalog,
        )
        .await
        .unwrap();

        assert_eq!(device.endpoint.name.as_deref(), Some("printer-a"));
    }

    #[tokio::test]
    async fn local_device_name_keeps_explicit_name() {
        let mut bind_catalog = BindCatalog::new(None, None);
        let device = resolve_local_device_access(
            "printer-a".to_owned(),
            local_arg("192.168.1.50,12345678,Office"),
            &[],
            &mut bind_catalog,
        )
        .await
        .unwrap();

        assert_eq!(device.endpoint.name.as_deref(), Some("Office"));
    }

    #[tokio::test]
    async fn missing_local_access_code_errors_when_no_metadata_source_exists() {
        let mut bind_catalog = BindCatalog::new(None, None);

        let error = resolve_local_device_access(
            "printer-a".to_owned(),
            local_arg("192.168.1.50"),
            &[],
            &mut bind_catalog,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Bambu Cloud token"));
    }

    #[tokio::test]
    async fn explicit_video_requires_matching_known_device() {
        let mut registry = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer-b".to_owned()),
                ..CloudDevice::default()
            }],
            Vec::new(),
        );
        let mut bind_catalog = BindCatalog::new(None, None);

        let error = attach_explicit_video(
            &mut registry,
            vec![explicit_video_endpoint("printer-a", "192.168.1.50")],
            &mut bind_catalog,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("--video-device"));
        assert!(error.to_string().contains("printer-a"));
        assert!(error
            .to_string()
            .contains("no matching cloud or local device"));
    }

    #[test]
    fn cloud_catalog_enumeration_only_happens_when_no_devices_are_configured() {
        assert!(should_enumerate_cloud_catalog(true, &[], &[]));
        assert!(!should_enumerate_cloud_catalog(false, &[], &[]));
        assert!(!should_enumerate_cloud_catalog(
            true,
            &["printer-a".to_owned()],
            &[]
        ));
        assert!(!should_enumerate_cloud_catalog(
            true,
            &[],
            &[local_arg("192.168.1.50,12345678")]
        ));
    }

    #[tokio::test]
    async fn video_access_code_can_resolve_matching_local_and_catalog_devices() {
        let video = vec![explicit_video_endpoint(
            "printer-a",
            "192.168.1.50,12345678",
        )];
        let mut bind_catalog = BindCatalog::new(None, None);
        let local_device = resolve_local_device_access(
            "printer-a".to_owned(),
            local_arg("192.168.1.50"),
            &video,
            &mut bind_catalog,
        )
        .await
        .unwrap();
        assert_eq!(local_device.endpoint.access_code.as_str(), "12345678");

        let mut registry = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer-a".to_owned()),
                ..CloudDevice::default()
            }],
            Vec::new(),
        );
        attach_explicit_video(&mut registry, video, &mut bind_catalog)
            .await
            .unwrap();
        assert_eq!(
            registry
                .get("printer-a")
                .unwrap()
                .device()
                .access_code
                .as_deref(),
            Some("12345678")
        );
    }

    #[tokio::test]
    async fn catalog_video_access_loads_bind_only_when_code_is_missing() {
        let video = vec![explicit_video_endpoint("printer-a", "192.168.1.50")];
        let mut registry = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer-a".to_owned()),
                ..CloudDevice::default()
            }],
            Vec::new(),
        );
        let mut bind_catalog = BindCatalog::new(None, None);

        let error = attach_explicit_video(&mut registry, video, &mut bind_catalog)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("Bambu Cloud token"));
    }
}
