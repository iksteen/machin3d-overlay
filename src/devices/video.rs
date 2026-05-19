use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Context, Result};
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::{debug, info};

use crate::{
    local::{Endpoint, LocalDevice},
    video::{infer_video_device_id, probe_video_endpoint, VideoEndpoint, DEFAULT_VIDEO_PORT},
};

use super::{
    access::hydrate_device_entry,
    metadata::BindCatalog,
    registry::{DeviceRegistry, DeviceRegistryBuilder},
};

const STARTUP_PROBE_CONCURRENCY: usize = 8;

#[derive(Default)]
pub(super) struct ExplicitVideoEndpoints {
    endpoints: Vec<(String, VideoEndpoint)>,
}

#[derive(Default)]
pub(crate) struct ResolvedVideoEndpoints {
    pub(crate) endpoints_by_device: HashMap<String, Vec<VideoEndpoint>>,
}

impl ResolvedVideoEndpoints {
    fn add(&mut self, device_id: impl Into<String>, endpoint: VideoEndpoint) {
        let endpoints = self
            .endpoints_by_device
            .entry(device_id.into())
            .or_default();
        if !endpoints.iter().any(|known| known == &endpoint) {
            endpoints.push(endpoint);
        }
    }

    fn has_endpoint(&self, device_id: &str, endpoint: &VideoEndpoint) -> bool {
        self.endpoints_by_device
            .get(device_id)
            .is_some_and(|endpoints| endpoints.iter().any(|known| known == endpoint))
    }
}

impl ExplicitVideoEndpoints {
    pub(super) async fn resolve(endpoints: &[VideoEndpoint]) -> Result<Self> {
        let semaphore = Arc::new(Semaphore::new(STARTUP_PROBE_CONCURRENCY));
        let mut probes = JoinSet::new();
        for (index, endpoint) in endpoints.iter().cloned().enumerate() {
            let semaphore = Arc::clone(&semaphore);
            probes.spawn(async move {
                let _permit = semaphore
                    .acquire_owned()
                    .await
                    .context("video endpoint probe concurrency limiter closed")?;
                let device_id = infer_video_device_id(&endpoint).await.with_context(|| {
                    format!("could not infer device ID for --video-device `{endpoint}`")
                })?;
                Ok::<_, anyhow::Error>((index, device_id, endpoint))
            });
        }

        let mut resolved = vec![None; endpoints.len()];
        while let Some(result) = probes.join_next().await {
            let (index, device_id, endpoint) =
                result.context("video endpoint probe task failed")??;
            resolved[index] = Some((device_id, endpoint));
        }
        let endpoints = resolved
            .into_iter()
            .map(|endpoint| endpoint.context("video endpoint probe did not return a device ID"))
            .collect::<Result<Vec<_>>>()?;
        ensure_unique_video_devices(&endpoints)?;
        Ok(Self { endpoints })
    }

    pub(super) fn for_device(&self, device_id: &str) -> Option<&VideoEndpoint> {
        self.endpoints
            .iter()
            .find(|(video_device_id, _)| video_device_id == device_id)
            .map(|(_, endpoint)| endpoint)
    }

    pub(super) async fn attach(
        self,
        builder: &mut DeviceRegistryBuilder,
        bind_catalog: &mut BindCatalog<'_>,
    ) -> Result<()> {
        for (device_id, video) in self.endpoints {
            let Some(entry) = builder.entry_mut(&device_id) else {
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

fn ensure_unique_video_devices(endpoints: &[(String, VideoEndpoint)]) -> Result<()> {
    let mut seen = HashSet::new();
    for (device_id, endpoint) in endpoints {
        if !seen.insert(device_id.as_str()) {
            anyhow::bail!(
                "--video-device `{endpoint}` resolves to duplicate device id `{device_id}`"
            );
        }
    }
    Ok(())
}

pub(crate) async fn resolve_video_endpoints(
    registry: &DeviceRegistry,
) -> Result<ResolvedVideoEndpoints> {
    let mut resolved = ResolvedVideoEndpoints::default();
    let mut probes = tokio::task::JoinSet::new();
    let semaphore = Arc::new(Semaphore::new(STARTUP_PROBE_CONCURRENCY));

    for entry in registry.entries() {
        let Some(endpoint) = entry.explicit_video() else {
            continue;
        };
        info!(
            device_id = %entry.id(),
            endpoint = %endpoint,
            "validated explicit local video endpoint"
        );
        resolved.add(entry.id(), endpoint.clone());
    }

    for device in registry.local_devices() {
        let endpoint = local_video_endpoint(&device);
        if resolved.has_endpoint(&device.id, &endpoint) {
            continue;
        }

        let device_id = device.id.clone();
        let semaphore = Arc::clone(&semaphore);
        probes.spawn(async move {
            let result = async {
                let _permit = semaphore
                    .acquire_owned()
                    .await
                    .context("video endpoint probe concurrency limiter closed")?;
                probe_video_endpoint(&device_id, &endpoint).await
            }
            .await;
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
                resolved.add(device_id, endpoint);
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

    Ok(resolved)
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
        devices::{
            metadata::BindCatalog, video::ExplicitVideoEndpoints, DeviceRegistryBuilder,
        },
        local::{LocalDevice, LocalEndpoint},
        video::VideoEndpoint,
    };

    use super::{ensure_unique_video_devices, local_video_endpoint};

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

    #[test]
    fn duplicate_explicit_video_device_ids_are_rejected() {
        let error = ensure_unique_video_devices(&[
            explicit_video_endpoint("printer-a", "192.168.1.50"),
            explicit_video_endpoint("printer-a", "192.168.1.51"),
        ])
        .unwrap_err();

        assert!(error.to_string().contains("--video-device"));
        assert!(error.to_string().contains("printer-a"));
    }

    #[tokio::test]
    async fn explicit_video_requires_matching_known_device() {
        let mut builder = DeviceRegistryBuilder::new(
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
        .attach(&mut builder, &mut bind_catalog)
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
        let mut builder = DeviceRegistryBuilder::new(
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
        .attach(&mut builder, &mut bind_catalog)
        .await
        .unwrap();

        let registry = builder.build();
        assert_eq!(
            registry.get("printer-a").unwrap().access_code(),
            Some("12345678")
        );
    }

    #[tokio::test]
    async fn explicit_video_loads_bind_when_code_is_missing() {
        let mut builder = DeviceRegistryBuilder::new(
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
        .attach(&mut builder, &mut bind_catalog)
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Bambu Cloud token"));
    }
}
