use std::collections::HashMap;

use anyhow::{Context, Result};
use tracing::{debug, info};

use crate::{
    local::{Endpoint, LocalDevice},
    video::{infer_video_device_id, probe_video_endpoint, VideoEndpoint, DEFAULT_VIDEO_PORT},
};

use super::{access::hydrate_device_entry, metadata::BindCatalog, registry::DeviceRegistry};

#[derive(Default)]
pub(super) struct ExplicitVideoEndpoints {
    endpoints: Vec<(String, VideoEndpoint)>,
}

pub(crate) struct ResolvedVideoEndpoints {
    pub(crate) endpoints: Vec<VideoEndpoint>,
    pub(crate) device_endpoints: HashMap<String, VideoEndpoint>,
}

impl ExplicitVideoEndpoints {
    pub(super) async fn resolve(endpoints: &[VideoEndpoint]) -> Result<Self> {
        let mut resolved = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            let device_id = infer_video_device_id(endpoint).await.with_context(|| {
                format!("could not infer device ID for --video-device `{endpoint}`")
            })?;
            resolved.push((device_id, endpoint.clone()));
        }
        Ok(Self {
            endpoints: resolved,
        })
    }

    pub(super) fn for_device(&self, device_id: &str) -> Option<&VideoEndpoint> {
        self.endpoints
            .iter()
            .find(|(video_device_id, _)| video_device_id == device_id)
            .map(|(_, endpoint)| endpoint)
    }

    pub(super) async fn attach(
        self,
        registry: &mut DeviceRegistry,
        bind_catalog: &mut BindCatalog<'_>,
    ) -> Result<()> {
        for (device_id, video) in self.endpoints {
            let Some(entry) = registry.get_mut(&device_id) else {
                anyhow::bail!(
                    "--video-device `{video}` is for device `{device_id}`, but no matching cloud or local device is configured"
                );
            };
            hydrate_device_entry(entry, Some(&video), bind_catalog).await?;
            entry.set_explicit_video(video);
        }
        Ok(())
    }
}

pub(crate) async fn resolve_video_endpoints(
    registry: &DeviceRegistry,
) -> Result<ResolvedVideoEndpoints> {
    let mut endpoints = Vec::new();
    let mut device_endpoints = HashMap::new();
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
        device_endpoints.insert(entry.id().to_owned(), endpoint.clone());
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
                device_endpoints.insert(device_id, endpoint.clone());
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
        device_endpoints,
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
    use crate::{
        bambu::CloudDevice,
        devices::{metadata::BindCatalog, video::ExplicitVideoEndpoints, DeviceRegistry},
        local::{LocalDevice, LocalEndpoint},
        video::VideoEndpoint,
    };

    use super::local_video_endpoint;

    fn endpoint(value: &str) -> VideoEndpoint {
        value.parse().expect("video endpoint should parse")
    }

    fn explicit_video_endpoint(device_id: &str, value: &str) -> (String, VideoEndpoint) {
        (device_id.to_owned(), endpoint(value))
    }

    #[test]
    fn local_video_endpoint_uses_host_and_default_port() {
        let device = LocalDevice {
            id: "printer-a".to_owned(),
            endpoint: LocalEndpoint::new("192.168.1.50", 8883, "12345678"),
        };

        assert_eq!(local_video_endpoint(&device), endpoint("192.168.1.50:6000"));
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

        let error = ExplicitVideoEndpoints {
            endpoints: vec![explicit_video_endpoint("printer-a", "192.168.1.50")],
        }
        .attach(&mut registry, &mut bind_catalog)
        .await
        .unwrap_err();

        assert!(error.to_string().contains("--video-device"));
        assert!(error.to_string().contains("printer-a"));
        assert!(error
            .to_string()
            .contains("no matching cloud or local device"));
    }

    #[tokio::test]
    async fn explicit_video_access_code_updates_cloud_device() {
        let mut registry = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer-a".to_owned()),
                ..CloudDevice::default()
            }],
            Vec::new(),
        );
        let mut bind_catalog = BindCatalog::new(None, None);

        ExplicitVideoEndpoints {
            endpoints: vec![explicit_video_endpoint(
                "printer-a",
                "192.168.1.50,12345678",
            )],
        }
        .attach(&mut registry, &mut bind_catalog)
        .await
        .unwrap();

        assert_eq!(
            registry.get("printer-a").unwrap().access_code(),
            Some("12345678")
        );
    }

    #[tokio::test]
    async fn explicit_video_loads_bind_when_code_is_missing() {
        let mut registry = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer-a".to_owned()),
                ..CloudDevice::default()
            }],
            Vec::new(),
        );
        let mut bind_catalog = BindCatalog::new(None, None);

        let error = ExplicitVideoEndpoints {
            endpoints: vec![explicit_video_endpoint("printer-a", "192.168.1.50")],
        }
        .attach(&mut registry, &mut bind_catalog)
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Bambu Cloud token"));
    }
}
