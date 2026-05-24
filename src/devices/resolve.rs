use std::{collections::HashSet, path::Path, sync::Arc};

use anyhow::{Context, Result};
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::{info, warn};

use crate::{
    cloud::{bound_cloud_devices, explicit_cloud_devices, CloudSession},
    local::{infer_local_device_id, LocalDevice, LocalEndpointConfig},
    snapmaker::{
        load_snap_tokens, probe_system_info, SnapMqttCreds, SnapToken, SnapmakerDevice,
        SnapmakerDeviceConfig,
    },
    video::VideoEndpoint,
};

use super::{
    access::BindCatalog,
    access::{has_text, hydrate_local_config},
    registry::{DeviceRegistry, DeviceRegistryBuilder},
    video::ExplicitVideoEndpoints,
};

const STARTUP_PROBE_CONCURRENCY: usize = 8;

pub(crate) async fn resolve_devices(
    cloud: Option<&CloudSession>,
    cloud_configs: &[String],
    local_configs: &[LocalEndpointConfig],
    video_endpoints: &[VideoEndpoint],
    snapmaker_configs: &[SnapmakerDeviceConfig],
    snap_token_file: Option<&Path>,
) -> Result<DeviceRegistry> {
    ensure_unique_cloud_configs(cloud_configs)?;
    let explicit_video = ExplicitVideoEndpoints::resolve(video_endpoints).await?;
    let enumerate_cloud_catalog = should_enumerate_cloud_catalog(
        cloud.is_some(),
        cloud_configs,
        local_configs,
        snapmaker_configs,
    );
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
    let snap_tokens = load_snap_token_catalog(snap_token_file)?;
    let snapmaker_devices = resolve_snapmaker_devices(snapmaker_configs, &snap_tokens).await?;

    let mut builder = DeviceRegistryBuilder::new(cloud_devices, local, snapmaker_devices);
    explicit_video
        .attach(&mut builder, &mut bind_catalog)
        .await?;
    let registry = builder.build();
    if registry.is_empty() {
        anyhow::bail!(
            "no devices configured; run `bambu-overlay login`, set --bbl-cloud-device, --bbl-local-device, or --snap-device"
        );
    }

    Ok(registry)
}

fn load_snap_token_catalog(token_file: Option<&Path>) -> Result<Vec<SnapToken>> {
    let Some(path) = token_file else {
        return Ok(Vec::new());
    };
    load_snap_tokens(path)
}

fn match_snap_token(host: &str, tokens: &[SnapToken]) -> Option<SnapToken> {
    tokens.iter().find(|token| token.host == host).cloned()
}

fn snap_creds_from_token(serial: &str, token: SnapToken) -> SnapMqttCreds {
    if token.sn != serial {
        warn!(
            paired_sn = %token.sn,
            probed_sn = %serial,
            host = %token.host,
            "Snapmaker probed serial differs from paired SN; using probed SN for MQTT topic"
        );
    }
    token.into()
}

async fn resolve_snapmaker_devices(
    configs: &[SnapmakerDeviceConfig],
    snap_tokens: &[SnapToken],
) -> Result<Vec<SnapmakerDevice>> {
    let semaphore = Arc::new(Semaphore::new(STARTUP_PROBE_CONCURRENCY));
    let mut probes = JoinSet::new();
    for (index, config) in configs.iter().cloned().enumerate() {
        let semaphore = Arc::clone(&semaphore);
        let matched_token = match_snap_token(&config.endpoint.host, snap_tokens);
        probes.spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .context("Snapmaker probe concurrency limiter closed")?;
            let info = probe_system_info(&config.endpoint).await.with_context(|| {
                format!(
                    "could not discover Snapmaker serial for --snap-device `{}`",
                    config.endpoint
                )
            })?;
            let mtls = matched_token.map(|token| snap_creds_from_token(&info.serial, token));
            Ok::<_, anyhow::Error>((
                index,
                SnapmakerDevice {
                    serial: info.serial,
                    endpoint: config.endpoint,
                    name: info.name,
                    mtls,
                },
            ))
        });
    }

    let mut discovered: Vec<Option<SnapmakerDevice>> = (0..configs.len()).map(|_| None).collect();
    while let Some(result) = probes.join_next().await {
        let (index, device) = result.context("Snapmaker probe task failed")??;
        info!(
            device_id = %device.serial,
            endpoint = %device.endpoint,
            paired = device.mtls.is_some(),
            "discovered Snapmaker device"
        );
        discovered[index] = Some(device);
    }
    let mut seen = HashSet::new();
    let mut devices = Vec::with_capacity(configs.len());
    for device in discovered.into_iter().flatten() {
        if !seen.insert(device.serial.clone()) {
            anyhow::bail!(
                "--snap-device endpoints resolve to duplicate serial `{}`",
                device.serial
            );
        }
        devices.push(device);
    }
    Ok(devices)
}

fn ensure_unique_cloud_configs(cloud_configs: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    for device_id in cloud_configs {
        let device_id = device_id.trim();
        if !seen.insert(device_id.to_owned()) {
            anyhow::bail!("--bbl-cloud-device specified duplicate device id `{device_id}`");
        }
    }
    Ok(())
}

async fn resolve_local_devices(
    configs: &[LocalEndpointConfig],
    video_endpoints: &ExplicitVideoEndpoints,
    bind_catalog: &mut BindCatalog<'_>,
) -> Result<Vec<LocalDevice>> {
    let mut devices = Vec::with_capacity(configs.len());
    let mut seen = HashSet::new();
    let device_ids = infer_local_device_ids(configs).await?;
    for (config, device_id) in configs.iter().zip(device_ids) {
        if !seen.insert(device_id.clone()) {
            anyhow::bail!("--bbl-local-device resolves duplicate device id `{device_id}`");
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

async fn infer_local_device_ids(configs: &[LocalEndpointConfig]) -> Result<Vec<String>> {
    let semaphore = Arc::new(Semaphore::new(STARTUP_PROBE_CONCURRENCY));
    let mut probes = JoinSet::new();
    for (index, config) in configs.iter().cloned().enumerate() {
        let semaphore = Arc::clone(&semaphore);
        probes.spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .context("local device probe concurrency limiter closed")?;
            let endpoint = config.endpoint();
            let device_id = infer_local_device_id(&config).await.with_context(|| {
                format!("could not infer device ID for --bbl-local-device `{endpoint}`")
            })?;
            Ok::<_, anyhow::Error>((index, device_id))
        });
    }

    let mut device_ids = vec![None; configs.len()];
    while let Some(result) = probes.join_next().await {
        let (index, device_id) = result.context("local device probe task failed")??;
        device_ids[index] = Some(device_id);
    }

    device_ids
        .into_iter()
        .map(|device_id| device_id.context("local device probe did not return a device ID"))
        .collect()
}

async fn resolve_local_device_access(
    device_id: String,
    mut endpoint: LocalEndpointConfig,
    video_endpoints: &ExplicitVideoEndpoints,
    bind_catalog: &mut BindCatalog<'_>,
) -> Result<LocalDevice> {
    hydrate_local_config(
        &device_id,
        &mut endpoint,
        video_endpoints.for_device(&device_id),
        bind_catalog,
    )
    .await?;
    if !has_text(endpoint.name.as_deref()) {
        endpoint.name = Some(device_id.clone());
    }
    finalize_local_device(device_id, endpoint)
}

fn should_enumerate_cloud_catalog(
    cloud_available: bool,
    cloud_configs: &[String],
    local_configs: &[LocalEndpointConfig],
    snapmaker_configs: &[SnapmakerDeviceConfig],
) -> bool {
    cloud_available
        && cloud_configs.is_empty()
        && local_configs.is_empty()
        && snapmaker_configs.is_empty()
}

fn finalize_local_device(device_id: String, endpoint: LocalEndpointConfig) -> Result<LocalDevice> {
    let access_code = endpoint
        .access_code
        .as_ref()
        .filter(|access_code| !access_code.expose().trim().is_empty())
        .cloned()
        .with_context(|| {
            format!(
                "--bbl-local-device `{}` is missing an access code; provide ACCESS_CODE or cloud metadata that exposes dev_access_code",
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
        ensure_unique_cloud_configs, resolve_devices, resolve_local_device_access,
        should_enumerate_cloud_catalog,
    };
    use crate::{
        devices::{access::BindCatalog, video::ExplicitVideoEndpoints},
        local::LocalEndpointConfig,
    };

    fn local_arg(value: &str) -> LocalEndpointConfig {
        value.parse().expect("local device should parse")
    }

    #[tokio::test]
    async fn local_device_name_defaults_to_device_id_when_missing() {
        let mut bind_catalog = BindCatalog::new(None, None);
        let device = resolve_local_device_access(
            "printer-a".to_owned(),
            local_arg("192.168.1.50,12345678"),
            &ExplicitVideoEndpoints::default(),
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
            &ExplicitVideoEndpoints::default(),
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
            &ExplicitVideoEndpoints::default(),
            &mut bind_catalog,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Bambu Cloud token"));
    }

    #[test]
    fn cloud_catalog_enumeration_only_happens_when_no_devices_are_configured() {
        assert!(should_enumerate_cloud_catalog(true, &[], &[], &[]));
        assert!(!should_enumerate_cloud_catalog(false, &[], &[], &[]));
        assert!(!should_enumerate_cloud_catalog(
            true,
            &["printer-a".to_owned()],
            &[],
            &[],
        ));
        assert!(!should_enumerate_cloud_catalog(
            true,
            &[],
            &[local_arg("192.168.1.50,12345678")],
            &[],
        ));
        assert!(!should_enumerate_cloud_catalog(
            true,
            &[],
            &[],
            &["192.168.0.120".parse().unwrap()],
        ));
    }

    #[test]
    fn duplicate_explicit_cloud_devices_are_rejected() {
        let error =
            ensure_unique_cloud_configs(&["printer-a".to_owned(), " printer-a ".to_owned()])
                .unwrap_err();

        assert!(error.to_string().contains("--bbl-cloud-device"));
        assert!(error.to_string().contains("printer-a"));
    }

    #[tokio::test]
    async fn explicit_cloud_devices_resolve_without_cloud_session() {
        let registry = resolve_devices(None, &["printer-a".to_owned()], &[], &[], &[], None)
            .await
            .expect("explicit cloud device should not require /bind metadata");

        let cloud_ids: Vec<_> = registry
            .entries()
            .iter()
            .filter(|entry| entry.has_cloud_mqtt())
            .map(|entry| entry.id().to_owned())
            .collect();
        assert_eq!(cloud_ids, vec!["printer-a".to_owned()]);
        assert_eq!(registry.first().unwrap().id(), "printer-a");
    }

    #[tokio::test]
    async fn no_configured_devices_errors_without_cloud_enumeration() {
        let error = resolve_devices(None, &[], &[], &[], &[], None)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("no devices configured"));
    }
}
