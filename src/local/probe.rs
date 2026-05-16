use std::time::Duration;

use anyhow::{Context, Result};
use tokio::net::TcpStream;

use crate::device_tls;

use super::LocalEndpointConfig;

const LOCAL_MQTT_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn infer_local_device_id(device: &LocalEndpointConfig) -> Result<String> {
    let endpoint = device.endpoint();
    let address = endpoint.to_string();
    let tcp = tokio::time::timeout(
        LOCAL_MQTT_PROBE_TIMEOUT,
        TcpStream::connect((endpoint.host.as_str(), endpoint.port)),
    )
    .await
    .with_context(|| format!("timed out probing local MQTT TLS at {address}"))?
    .with_context(|| format!("failed to connect to local MQTT TLS at {address}"))?;

    let tls = device_tls::tokio_connector()?;
    let socket = tokio::time::timeout(
        LOCAL_MQTT_PROBE_TIMEOUT,
        tls.connect(endpoint.host.as_str(), tcp),
    )
    .await
    .with_context(|| format!("timed out handshaking local MQTT TLS at {address}"))?
    .with_context(|| format!("failed local MQTT TLS handshake at {address}"))?;

    device_tls::peer_device_id(&socket)
        .with_context(|| format!("local MQTT certificate at {address} did not include a device ID"))
}
