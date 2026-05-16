use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use serde::Serialize;
use tracing::{debug, info};

use crate::{
    bambu::{CloudDevice, PrinterStatus},
    cloud::{bound_cloud_devices, explicit_cloud_devices, CloudSession},
    local::{infer_local_device_id, Endpoint, LocalDevice, LocalEndpointArg},
    video::{infer_video_device_id, probe_video_endpoint, VideoEndpoint, DEFAULT_VIDEO_PORT},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DeviceSource {
    Cloud,
    Local,
}

#[derive(Debug, Clone)]
pub(crate) struct KnownDevice {
    pub(crate) id: String,
    pub(crate) name: Option<String>,
    pub(crate) online: Option<bool>,
    pub(crate) access_code: Option<String>,
    pub(crate) status: PrinterStatus,
}

impl KnownDevice {
    pub(crate) fn from_cloud(device: CloudDevice) -> Option<Self> {
        Some(Self {
            id: non_empty_string(device.id)?,
            name: device.name,
            online: device.online,
            access_code: device.access_code,
            status: device.status,
        })
    }

    pub(crate) fn from_local(device: &LocalDevice) -> Self {
        Self {
            id: device.id.clone(),
            name: device.endpoint.name.clone(),
            online: Some(true),
            access_code: Some(device.endpoint.access_code.clone()),
            status: PrinterStatus::default(),
        }
    }

    pub(crate) fn has_access_code(&self) -> bool {
        self.access_code
            .as_deref()
            .is_some_and(|code| !code.trim().is_empty())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DeviceEntry {
    device: KnownDevice,
    capabilities: DeviceCapabilities,
}

#[derive(Debug, Clone, Default)]
struct DeviceCapabilities {
    cloud_mqtt: bool,
    local_mqtt: Option<LocalDevice>,
    explicit_video: Option<VideoEndpoint>,
}

#[derive(Debug, Clone)]
pub(crate) struct DeviceRegistry {
    entries: Vec<DeviceEntry>,
    entry_by_id: HashMap<String, usize>,
}

impl DeviceEntry {
    fn from_cloud(device: KnownDevice) -> Self {
        Self {
            device,
            capabilities: DeviceCapabilities {
                cloud_mqtt: true,
                ..DeviceCapabilities::default()
            },
        }
    }

    fn from_local(local: LocalDevice) -> Self {
        Self {
            device: KnownDevice::from_local(&local),
            capabilities: DeviceCapabilities {
                local_mqtt: Some(local),
                ..DeviceCapabilities::default()
            },
        }
    }

    pub(crate) fn device(&self) -> &KnownDevice {
        &self.device
    }

    pub(crate) fn id(&self) -> &str {
        self.device.id.as_str()
    }

    pub(crate) fn source(&self) -> DeviceSource {
        if self.local().is_some() {
            DeviceSource::Local
        } else {
            DeviceSource::Cloud
        }
    }

    pub(crate) fn has_cloud_mqtt(&self) -> bool {
        self.capabilities.cloud_mqtt
    }

    pub(crate) fn local(&self) -> Option<&LocalDevice> {
        self.capabilities.local_mqtt.as_ref()
    }

    pub(crate) fn explicit_video(&self) -> Option<&VideoEndpoint> {
        self.capabilities.explicit_video.as_ref()
    }
}

impl DeviceRegistry {
    pub(crate) fn new(cloud_devices: Vec<CloudDevice>, local_devices: Vec<LocalDevice>) -> Self {
        let local_ids = local_devices
            .iter()
            .map(|device| device.id.as_str())
            .collect::<HashSet<_>>();
        let mut registry = Self {
            entries: Vec::new(),
            entry_by_id: HashMap::new(),
        };
        for device in cloud_devices.into_iter().filter(|device| {
            device
                .id
                .as_deref()
                .map(str::trim)
                .is_none_or(|device_id| !local_ids.contains(device_id))
        }) {
            if let Some(device) = KnownDevice::from_cloud(device) {
                registry.push(DeviceEntry::from_cloud(device));
            }
        }
        for local in local_devices {
            registry.push(DeviceEntry::from_local(local));
        }

        registry
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn entries(&self) -> &[DeviceEntry] {
        &self.entries
    }

    pub(crate) fn devices(&self) -> impl Iterator<Item = &KnownDevice> {
        self.entries.iter().map(|entry| &entry.device)
    }

    pub(crate) fn first(&self) -> Option<&DeviceEntry> {
        self.entries.first()
    }

    pub(crate) fn get(&self, device_id: &str) -> Option<&DeviceEntry> {
        self.entry_by_id
            .get(device_id)
            .and_then(|index| self.entries.get(*index))
    }

    pub(crate) fn local_devices(&self) -> Vec<LocalDevice> {
        self.entries
            .iter()
            .filter_map(|entry| entry.local().cloned())
            .collect()
    }

    pub(crate) fn cloud_mqtt_ids(&self) -> Vec<String> {
        self.entries
            .iter()
            .filter(|entry| entry.has_cloud_mqtt())
            .map(|entry| entry.device.id.clone())
            .collect()
    }

    async fn attach_explicit_video(
        &mut self,
        video_endpoints: Vec<(String, VideoEndpoint)>,
        cloud: Option<&CloudSession>,
        bind_metadata: &mut Option<Vec<CloudDevice>>,
    ) -> Result<()> {
        for (device_id, video) in video_endpoints {
            let Some(index) = self.entry_by_id.get(&device_id).copied() else {
                anyhow::bail!(
                    "--video-device `{video}` is for device `{device_id}`, but no matching cloud or local device is configured"
                );
            };
            let entry = &mut self.entries[index];
            resolve_known_device_access(&mut entry.device, &video, cloud, bind_metadata).await?;
            entry.capabilities.explicit_video = Some(video);
        }
        Ok(())
    }

    fn push(&mut self, entry: DeviceEntry) {
        if self.entry_by_id.contains_key(entry.id()) {
            return;
        }
        self.entry_by_id
            .insert(entry.id().to_owned(), self.entries.len());
        self.entries.push(entry);
    }
}

pub(crate) struct ResolvedVideoEndpoints {
    pub(crate) endpoints: Vec<VideoEndpoint>,
    pub(crate) endpoint_map: HashMap<String, VideoEndpoint>,
}

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
    let mut bind_metadata = enumerate_cloud_catalog.then(|| cloud_devices.clone());
    let local =
        resolve_local_devices(local_configs, &explicit_video, cloud, &mut bind_metadata).await?;

    let mut registry = DeviceRegistry::new(cloud_devices, local);
    registry
        .attach_explicit_video(explicit_video, cloud, &mut bind_metadata)
        .await?;
    if registry.is_empty() {
        anyhow::bail!(
            "no devices configured; run `bambu-overlay login`, set --cloud-device, or set --local-device"
        );
    }

    Ok(registry)
}

pub(crate) async fn resolve_video_endpoints(
    registry: &DeviceRegistry,
) -> Result<ResolvedVideoEndpoints> {
    let mut endpoints = Vec::new();
    let mut endpoint_map = HashMap::new();
    let mut candidates = Vec::new();
    let mut probes = tokio::task::JoinSet::new();

    for entry in registry.entries() {
        let Some(endpoint) = entry.explicit_video() else {
            continue;
        };
        info!(
            device_id = %entry.id(),
            endpoint = %endpoint,
            "validated explicit local video endpoint"
        );
        endpoints.push(endpoint.clone());
        candidates.push(endpoint.clone());
        endpoint_map.insert(entry.id().to_owned(), endpoint.clone());
    }

    for device in registry.local_devices() {
        let endpoint = local_video_endpoint(&device);
        if candidates.iter().any(|candidate| candidate == &endpoint) {
            continue;
        }

        candidates.push(endpoint.clone());
        let device_id = device.id.clone();
        probes.spawn(async move {
            let result = probe_video_endpoint(&device_id, &endpoint).await;
            (device_id, endpoint, result)
        });
    }

    while let Some(result) = probes.join_next().await {
        match result {
            Ok((device_id, endpoint, Ok(()))) => {
                info!(
                    device_id = %device_id,
                    endpoint = %endpoint,
                    "auto-enabled local video endpoint"
                );
                endpoint_map.insert(device_id, endpoint.clone());
                endpoints.push(endpoint);
            }
            Ok((device_id, endpoint, Err(error))) => {
                debug!(
                    device_id = %device_id,
                    endpoint = %endpoint,
                    error = %error,
                    "local video endpoint probe failed"
                );
            }
            Err(error) => {
                debug!(%error, "local video endpoint probe task failed");
            }
        }
    }

    Ok(ResolvedVideoEndpoints {
        endpoints,
        endpoint_map,
    })
}

async fn resolve_local_devices(
    configs: &[LocalEndpointArg],
    video_endpoints: &[(String, VideoEndpoint)],
    cloud: Option<&CloudSession>,
    bind_metadata: &mut Option<Vec<CloudDevice>>,
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
            resolve_local_device_access(
                device_id,
                config.clone(),
                video_endpoints,
                cloud,
                bind_metadata,
            )
            .await?,
        );
    }
    Ok(devices)
}

async fn resolve_local_device_access(
    device_id: String,
    mut endpoint: LocalEndpointArg,
    video_endpoints: &[(String, VideoEndpoint)],
    cloud: Option<&CloudSession>,
    bind_metadata: &mut Option<Vec<CloudDevice>>,
) -> Result<LocalDevice> {
    if !has_access_code(endpoint.access_code.as_deref()) {
        if let Some(video) = explicit_video_for_device(video_endpoints, &device_id) {
            endpoint.access_code = video.access_code().map(str::to_owned);
        }
    }
    if !has_access_code(endpoint.access_code.as_deref()) {
        if let Some(metadata) = bind_device(cloud, bind_metadata, &device_id).await? {
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
    cloud: Option<&CloudSession>,
    bind_metadata: &mut Option<Vec<CloudDevice>>,
) -> Result<()> {
    if !device.has_access_code() {
        device.access_code = video.access_code().map(str::to_owned);
    }
    if !device.has_access_code() {
        let device_id = device.id.clone();
        if let Some(metadata) = bind_device(cloud, bind_metadata, &device_id).await? {
            device.access_code = metadata.access_code;
            if !has_text(device.name.as_deref()) {
                device.name = metadata.name;
            }
            device.online = device.online.or(metadata.online);
        }
    }
    Ok(())
}

async fn bind_device(
    cloud: Option<&CloudSession>,
    bind_metadata: &mut Option<Vec<CloudDevice>>,
    device_id: &str,
) -> Result<Option<CloudDevice>> {
    if bind_metadata.is_none() {
        *bind_metadata = Some(bound_cloud_devices(cloud).await?);
    }
    Ok(bind_metadata
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .find(|device| device.id.as_deref() == Some(device_id))
        .cloned())
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

fn non_empty_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
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

fn local_video_endpoint(device: &LocalDevice) -> VideoEndpoint {
    VideoEndpoint::new(
        Endpoint::new(device.endpoint.host().to_owned(), DEFAULT_VIDEO_PORT),
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        finalize_local_device, local_video_endpoint, resolve_local_device_access,
        should_enumerate_cloud_catalog, DeviceRegistry,
    };
    use crate::{
        bambu::CloudDevice,
        local::{LocalDevice, LocalEndpointArg},
        video::VideoEndpoint,
    };

    fn local_arg(value: &str) -> LocalEndpointArg {
        value.parse().expect("local device should parse")
    }

    fn local(id: &str, value: &str) -> LocalDevice {
        finalize_local_device(id.to_owned(), local_arg(value))
            .expect("local device should be complete")
    }

    fn endpoint(value: &str) -> VideoEndpoint {
        value.parse().expect("video endpoint should parse")
    }

    fn explicit_video_endpoint(device_id: &str, value: &str) -> (String, VideoEndpoint) {
        (device_id.to_owned(), endpoint(value))
    }

    #[test]
    fn registry_uses_local_device_when_ids_overlap() {
        let local_devices = vec![local("printer-a", "192.168.1.50,12345678,Office")];
        let registry = DeviceRegistry::new(
            vec![
                CloudDevice {
                    id: Some(" printer-a ".to_owned()),
                    name: Some("Cloud Office".to_owned()),
                    access_code: Some("87654321".to_owned()),
                    ..CloudDevice::default()
                },
                CloudDevice {
                    id: Some("printer-b".to_owned()),
                    name: Some("Garage".to_owned()),
                    ..CloudDevice::default()
                },
            ],
            local_devices,
        );
        let devices = registry.devices().collect::<Vec<_>>();

        assert_eq!(devices.len(), 2);
        assert_eq!(devices[0].id, "printer-b");
        assert_eq!(devices[1].id, "printer-a");
        assert_eq!(devices[1].access_code.as_deref(), Some("12345678"));
    }

    #[test]
    fn registry_ignores_cloud_devices_without_ids() {
        let registry = DeviceRegistry::new(
            vec![
                CloudDevice {
                    id: Some("printer-a".to_owned()),
                    ..CloudDevice::default()
                },
                CloudDevice::default(),
            ],
            Vec::new(),
        );
        let ids = registry
            .devices()
            .map(|device| device.id.clone())
            .collect::<Vec<_>>();

        assert_eq!(ids, vec!["printer-a".to_owned()]);
    }

    #[test]
    fn registry_cloud_mqtt_ids_only_include_cloud_devices() {
        let registry = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer-a".to_owned()),
                ..CloudDevice::default()
            }],
            vec![local("printer-b", "192.168.1.50,12345678,Office")],
        );

        assert_eq!(registry.cloud_mqtt_ids(), vec!["printer-a".to_owned()]);
    }

    #[test]
    fn local_video_endpoint_uses_host_and_default_port() {
        let device = local("printer-a", "192.168.1.50,12345678,Office");

        assert_eq!(local_video_endpoint(&device), endpoint("192.168.1.50:6000"));
    }

    #[tokio::test]
    async fn local_device_name_defaults_to_device_id_when_missing() {
        let mut bind_metadata = None;
        let device = resolve_local_device_access(
            "printer-a".to_owned(),
            local_arg("192.168.1.50,12345678"),
            &[],
            None,
            &mut bind_metadata,
        )
        .await
        .unwrap();

        assert_eq!(device.endpoint.name.as_deref(), Some("printer-a"));
    }

    #[tokio::test]
    async fn local_device_name_keeps_explicit_name() {
        let mut bind_metadata = None;
        let device = resolve_local_device_access(
            "printer-a".to_owned(),
            local_arg("192.168.1.50,12345678,Office"),
            &[],
            None,
            &mut bind_metadata,
        )
        .await
        .unwrap();

        assert_eq!(device.endpoint.name.as_deref(), Some("Office"));
    }

    #[tokio::test]
    async fn missing_local_access_code_errors_when_no_metadata_source_exists() {
        let mut bind_metadata = None;

        let error = resolve_local_device_access(
            "printer-a".to_owned(),
            local_arg("192.168.1.50"),
            &[],
            None,
            &mut bind_metadata,
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
        let mut bind_metadata = None;

        let error = registry
            .attach_explicit_video(
                vec![explicit_video_endpoint("printer-a", "192.168.1.50")],
                None,
                &mut bind_metadata,
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
        let mut bind_metadata = None;
        let local_device = resolve_local_device_access(
            "printer-a".to_owned(),
            local_arg("192.168.1.50"),
            &video,
            None,
            &mut bind_metadata,
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
        registry
            .attach_explicit_video(video, None, &mut bind_metadata)
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
        let mut bind_metadata = None;

        let error = registry
            .attach_explicit_video(video, None, &mut bind_metadata)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("Bambu Cloud token"));
    }
}
