use std::time::Duration;

use anyhow::{ensure, Context, Result};
use tokio::net::TcpStream;

use crate::device_tls;

use super::endpoint::VideoEndpoint;

const VIDEO_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

pub async fn probe_video_endpoint(device_id: &str, endpoint: &VideoEndpoint) -> Result<()> {
    let device_id = device_id.trim();
    ensure!(!device_id.is_empty(), "device ID is empty");
    let address = endpoint.address();
    let tcp = connect_video_tcp(endpoint, VIDEO_PROBE_TIMEOUT, "probing video server").await?;

    let tls = device_tls::tokio_connector()?;
    let socket = tokio::time::timeout(VIDEO_PROBE_TIMEOUT, tls.connect(device_id, tcp))
        .await
        .with_context(|| format!("timed out probing video TLS at {address}"))?
        .with_context(|| format!("failed TLS handshake while probing video server at {address}"))?;
    let certificate_device_id = device_tls::peer_device_id(&socket)
        .context("video server certificate did not include a usable common name")?;
    ensure!(
        certificate_device_id == device_id,
        "video endpoint certificate is for device `{certificate_device_id}`, not `{device_id}`"
    );

    Ok(())
}

pub async fn infer_video_device_id(endpoint: &VideoEndpoint) -> Result<String> {
    let address = endpoint.address();
    let tcp = connect_video_tcp(endpoint, VIDEO_PROBE_TIMEOUT, "probing video server").await?;

    let tls = device_tls::tokio_connector()?;
    let socket = tokio::time::timeout(VIDEO_PROBE_TIMEOUT, tls.connect(endpoint.host(), tcp))
        .await
        .with_context(|| format!("timed out probing video TLS at {address}"))?
        .with_context(|| format!("failed TLS handshake while probing video server at {address}"))?;

    device_tls::peer_device_id(&socket)
        .context("video server certificate did not include a usable common name")
}

pub(super) async fn connect_video_tcp(
    endpoint: &VideoEndpoint,
    timeout: Duration,
    action: &str,
) -> Result<TcpStream> {
    let address = endpoint.address();
    tokio::time::timeout(
        timeout,
        TcpStream::connect((endpoint.host(), endpoint.port())),
    )
    .await
    .with_context(|| format!("timed out {action} at {address}"))?
    .with_context(|| format!("failed to connect to video server at {address}"))
}
