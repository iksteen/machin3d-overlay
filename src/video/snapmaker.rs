//! Snapmaker / Klipper camera worker.
//!
//! The U1 only writes frames to `/server/files/camera/monitor.jpg` while
//! "monitor" mode is active — by default the file is frozen on the last
//! captured frame. We wake the camera by calling `camera.start_monitor` over
//! Moonraker's JSON-RPC-over-WebSocket (an `~HTTP` endpoint), then poll the
//! JPEG with `If-Modified-Since` so only freshly-written frames are
//! forwarded. When the last subscriber disconnects we send
//! `camera.stop_monitor` so the camera daemon can shut down again.

use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use reqwest::header::{IF_MODIFIED_SINCE, LAST_MODIFIED};
use serde_json::json;
use tokio::time::sleep;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use tracing::{debug, warn};

use crate::{service::ShutdownReceiver, snapmaker::SnapmakerEndpoint};

use super::stream::DeviceVideoStream;

const POLL_INTERVAL: Duration = Duration::from_millis(250);
const POLL_TIMEOUT: Duration = Duration::from_secs(8);
const RETRY_INITIAL_DELAY: Duration = Duration::from_secs(2);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);
/// Tight overall budget for the stop_monitor cleanup so we don't blow the
/// parent shutdown grace period (5s by default).
const STOP_MONITOR_BUDGET: Duration = Duration::from_secs(3);
/// The `domain` parameter is used by Snapmaker's camera daemon to track which
/// client requested the monitor mode. A unique value keeps stop requests from
/// other clients (e.g. Snapmaker Orca) from killing our stream and vice
/// versa.
const MONITOR_DOMAIN: &str = "bambu-overlay";

pub(super) async fn run_snapmaker_stream_worker(
    endpoint: SnapmakerEndpoint,
    stream: Arc<DeviceVideoStream>,
    mut shutdown: ShutdownReceiver,
) {
    let url = format!(
        "http://{host}:{port}/server/files/camera/monitor.jpg",
        host = endpoint.host,
        port = endpoint.port,
    );
    let client = match reqwest::Client::builder().timeout(POLL_TIMEOUT).build() {
        Ok(client) => client,
        Err(error) => {
            warn!(
                device_id = %stream.device_id,
                %error,
                "failed to build Snapmaker camera HTTP client"
            );
            return;
        }
    };

    if let Err(error) = control_camera(&endpoint, "camera.start_monitor").await {
        warn!(
            device_id = %stream.device_id,
            error = %error_chain(&error),
            "Snapmaker camera start_monitor failed; falling back to passive poll"
        );
    } else {
        debug!(device_id = %stream.device_id, "Snapmaker camera monitor started");
    }

    let mut delay = RETRY_INITIAL_DELAY;
    let mut last_modified: Option<String> = None;

    while stream.clients.load(Ordering::SeqCst) > 0 {
        let attempt = tokio::select! {
            result = poll_once(&client, &url, last_modified.as_deref(), &stream) => result,
            _ = shutdown.cancelled() => break,
        };
        match attempt {
            Ok(outcome) => {
                if let Some(value) = outcome.last_modified {
                    last_modified = Some(value);
                }
                if let Some(bytes) = outcome.frame {
                    let _ = stream.frames.send(bytes);
                }
                delay = RETRY_INITIAL_DELAY;
                tokio::select! {
                    _ = sleep_or_no_clients(&stream, POLL_INTERVAL) => {}
                    _ = shutdown.cancelled() => break,
                }
            }
            Err(error) => {
                if stream.clients.load(Ordering::SeqCst) == 0 {
                    break;
                }
                warn!(
                    device_id = %stream.device_id,
                    error = %error_chain(&error),
                    "Snapmaker camera poll failed"
                );
                tokio::select! {
                    _ = sleep_or_no_clients(&stream, delay) => {}
                    _ = shutdown.cancelled() => break,
                }
                delay = (delay + delay / 2).min(RETRY_MAX_DELAY);
            }
        }
    }

    match tokio::time::timeout(
        STOP_MONITOR_BUDGET,
        control_camera(&endpoint, "camera.stop_monitor"),
    )
    .await
    {
        Ok(Ok(())) => debug!(device_id = %stream.device_id, "Snapmaker camera monitor stopped"),
        Ok(Err(error)) => warn!(
            device_id = %stream.device_id,
            error = %error_chain(&error),
            "Snapmaker camera stop_monitor failed"
        ),
        Err(_) => warn!(
            device_id = %stream.device_id,
            "Snapmaker camera stop_monitor timed out"
        ),
    }
}

/// Send a one-shot Moonraker JSON-RPC request to start or stop monitor mode.
/// The response comes back asynchronously over an internal MQTT bus, so we
/// don't try to await it — the request being accepted by the repeater
/// endpoint is enough.
async fn control_camera(endpoint: &SnapmakerEndpoint, method: &str) -> Result<()> {
    let url = format!(
        "ws://{host}:{port}/websocket",
        host = endpoint.host,
        port = endpoint.port,
    );
    let request = url
        .as_str()
        .into_client_request()
        .with_context(|| format!("invalid Moonraker WebSocket URL `{url}`"))?;
    let (mut socket, _response) = connect_async(request)
        .await
        .with_context(|| format!("failed to connect to Moonraker at {url} for camera control"))?;
    let req_id: i64 = 1;
    let payload = json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": {
            "req_id": req_id,
            "domain": MONITOR_DOMAIN,
            "interval": 0,
            "expect_pw": false,
        },
        "id": req_id,
    });
    socket
        .send(Message::Text(payload.to_string()))
        .await
        .with_context(|| format!("failed to send Moonraker `{method}` request"))?;
    // Drain a few messages so the send actually flushes before we close.
    for _ in 0..5 {
        match tokio::time::timeout(Duration::from_millis(500), socket.next()).await {
            Ok(Some(Ok(_))) => continue,
            _ => break,
        }
    }
    let _ = socket.close(None).await;
    Ok(())
}

struct PollOutcome {
    frame: Option<Bytes>,
    last_modified: Option<String>,
}

async fn poll_once(
    client: &reqwest::Client,
    url: &str,
    last_modified: Option<&str>,
    _stream: &DeviceVideoStream,
) -> Result<PollOutcome> {
    let mut request = client.get(url);
    if let Some(value) = last_modified {
        request = request.header(IF_MODIFIED_SINCE, value);
    }
    let response = request
        .send()
        .await
        .context("Snapmaker camera HTTP request failed")?;
    if response.status() == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(PollOutcome {
            frame: None,
            last_modified: None,
        });
    }
    let response = response
        .error_for_status()
        .context("Snapmaker camera returned an error status")?;
    let new_last_modified = response
        .headers()
        .get(LAST_MODIFIED)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let bytes = response
        .bytes()
        .await
        .context("failed to read Snapmaker camera body")?;
    if !bytes.starts_with(&[0xff, 0xd8]) {
        anyhow::bail!("Snapmaker camera response is not a JPEG");
    }
    Ok(PollOutcome {
        frame: Some(Bytes::from(bytes.to_vec())),
        last_modified: new_last_modified,
    })
}

async fn sleep_or_no_clients(stream: &DeviceVideoStream, delay: Duration) {
    let no_clients = stream.no_clients.notified();
    tokio::pin!(no_clients);
    if stream.clients.load(Ordering::SeqCst) == 0 {
        return;
    }
    tokio::select! {
        _ = sleep(delay) => {}
        _ = &mut no_clients => {}
    }
}

fn error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}
