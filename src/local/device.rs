use std::{fmt, time::Duration};

use anyhow::{Context, Result};
use tokio::net::TcpStream;

use crate::{device_tls, secret::Secret};

use super::{Endpoint, LocalEndpointConfig};

const LOCAL_MQTT_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Reads the printer's device ID (serial number) from its MQTT-over-TLS
/// certificate common name. Used at startup so `--local-device` does not need
/// the operator to provide the device ID.
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalEndpoint {
    pub endpoint: Endpoint,
    pub access_code: Secret<String>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalDevice {
    pub id: String,
    pub endpoint: LocalEndpoint,
}

impl LocalEndpoint {
    #[cfg(test)]
    pub fn new(host: impl Into<String>, port: u16, access_code: impl Into<String>) -> Self {
        Self {
            endpoint: Endpoint::new(host, port),
            access_code: Secret::new(access_code.into()),
            name: None,
        }
    }

    pub fn host(&self) -> &str {
        self.endpoint.host.as_str()
    }

    pub fn port(&self) -> u16 {
        self.endpoint.port
    }

    pub fn access_code(&self) -> &str {
        self.access_code.expose().as_str()
    }
}

impl fmt::Display for LocalDevice {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}={}", self.id, self.endpoint)
    }
}

impl fmt::Display for LocalEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.endpoint.fmt(formatter)
    }
}
