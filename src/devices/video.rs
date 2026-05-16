use std::collections::HashMap;

use anyhow::Result;
use tracing::{debug, info};

use crate::{
    local::{Endpoint, LocalDevice},
    video::{probe_video_endpoint, VideoEndpoint, DEFAULT_VIDEO_PORT},
};

use super::registry::DeviceRegistry;

pub(crate) struct ResolvedVideoEndpoints {
    pub(crate) endpoints: Vec<VideoEndpoint>,
    pub(crate) endpoint_map: HashMap<String, VideoEndpoint>,
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

fn local_video_endpoint(device: &LocalDevice) -> VideoEndpoint {
    VideoEndpoint::new(
        Endpoint::new(device.endpoint.host().to_owned(), DEFAULT_VIDEO_PORT),
        None,
    )
}

#[cfg(test)]
mod tests {
    use crate::{
        local::{LocalDevice, LocalEndpoint},
        video::VideoEndpoint,
    };

    use super::local_video_endpoint;

    fn endpoint(value: &str) -> VideoEndpoint {
        value.parse().expect("video endpoint should parse")
    }

    #[test]
    fn local_video_endpoint_uses_host_and_default_port() {
        let device = LocalDevice {
            id: "printer-a".to_owned(),
            endpoint: LocalEndpoint::new("192.168.1.50", 8883, "12345678"),
        };

        assert_eq!(local_video_endpoint(&device), endpoint("192.168.1.50:6000"));
    }
}
