//! Inspect a Moonraker printer's `machine/system_info` to learn its stable
//! identity (serial number) and friendly name before the runtime registry
//! freezes. Mirrors the role of `infer_local_device_id` for Bambu local
//! printers: a startup-time round trip that turns a user-supplied LAN
//! endpoint into a fully-shaped device entry.

use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

use super::MoonrakerEndpoint;

const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub(crate) struct MoonrakerSystemInfo {
    pub(crate) serial: String,
    pub(crate) name: Option<String>,
}

pub(crate) async fn probe_system_info(endpoint: &MoonrakerEndpoint) -> Result<MoonrakerSystemInfo> {
    let url = format!(
        "http://{host}:{port}/machine/system_info",
        host = endpoint.host,
        port = endpoint.port,
    );
    let client = reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .build()
        .context("failed to build Moonraker HTTP client")?;
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("failed to call Moonraker at {url}"))?
        .error_for_status()
        .with_context(|| format!("Moonraker at {url} returned an error"))?;
    let body: SystemInfoResponse = response
        .json()
        .await
        .with_context(|| format!("Moonraker at {url} returned unexpected JSON"))?;
    let product = body.result.system_info.product_info;
    let serial = product
        .serial_number
        .map(|serial| serial.trim().to_owned())
        .filter(|serial| !serial.is_empty())
        .with_context(|| {
            format!("Moonraker at {url} did not report a serial_number in machine/system_info")
        })?;
    let name = product
        .device_name
        .or(product.machine_type)
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty());
    Ok(MoonrakerSystemInfo { serial, name })
}

#[derive(Deserialize)]
struct SystemInfoResponse {
    result: SystemInfoResult,
}

#[derive(Deserialize)]
struct SystemInfoResult {
    system_info: SystemInfoBody,
}

#[derive(Deserialize)]
struct SystemInfoBody {
    #[serde(default)]
    product_info: ProductInfo,
}

#[derive(Default, Deserialize)]
struct ProductInfo {
    #[serde(default)]
    serial_number: Option<String>,
    #[serde(default)]
    device_name: Option<String>,
    #[serde(default)]
    machine_type: Option<String>,
}
