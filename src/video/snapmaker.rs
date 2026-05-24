//! Snapmaker / Klipper camera worker.
//!
//! The U1 only writes frames to `/server/files/camera/monitor.jpg` while
//! "monitor" mode is active — by default the file is frozen on the last
//! captured frame. Paired devices wake the camera by publishing
//! `camera.start_monitor` over mTLS MQTT to `<sn>/request`; unpaired
//! devices just poll the JPEG and serve whatever the printer is willing
//! to return (stale unless a print or Orca is keeping the daemon awake).

use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use reqwest::header::{IF_MODIFIED_SINCE, LAST_MODIFIED};
use rumqttc::{AsyncClient, ConnectReturnCode, Event, EventLoop, MqttOptions, Packet, QoS};
use serde::Deserialize;
use serde_json::json;
use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tracing::{debug, info, warn};

use crate::{
    service::ShutdownReceiver,
    snapmaker::{mtls, SnapMqttCreds, SnapmakerEndpoint},
};

use super::{source::SnapmakerVideoSource, stream::DeviceVideoStream};

const POLL_INTERVAL: Duration = Duration::from_millis(250);
const POLL_TIMEOUT: Duration = Duration::from_secs(8);
const RETRY_INITIAL_DELAY: Duration = Duration::from_secs(2);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);
/// Tight overall budget for the stop_monitor cleanup so we don't blow the
/// parent shutdown grace period (5s by default).
const STOP_MONITOR_BUDGET: Duration = Duration::from_secs(3);
/// The `domain` parameter the camera daemon recognizes for LAN-side
/// monitor mode. Observed value Orca sends (and the daemon accepts) is
/// the literal string `"lan"` — arbitrary client identifiers are rejected
/// with `Start monitor failed`. The `monitor_domain` field on the
/// daemon's `notify_camera_status_change` notification echoes this value.
const MONITOR_DOMAIN: &str = "lan";
const MQTT_KEEPALIVE: Duration = Duration::from_secs(30);
/// Time we wait for CONNACK during mTLS publisher startup before
/// abandoning the session.
const MQTT_CONNECT_BUDGET: Duration = Duration::from_secs(8);
/// Time we wait for the daemon's response to a `<sn>/request` publish.
const MQTT_RESPONSE_BUDGET: Duration = Duration::from_secs(10);

pub(super) async fn run_snapmaker_stream_worker(
    source: &SnapmakerVideoSource,
    stream: Arc<DeviceVideoStream>,
    mut shutdown: ShutdownReceiver,
) {
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

    let publisher = match source.mtls.as_ref() {
        Some(mtls) => match SnapMqttPublisher::connect(source, mtls).await {
            Ok(publisher) => {
                info!(
                    device_id = %stream.device_id,
                    host = %source.endpoint.host,
                    port = mtls.port,
                    "opened Snapmaker mTLS session for camera control"
                );
                Some(publisher)
            }
            Err(error) => {
                warn!(
                    device_id = %stream.device_id,
                    error = %error_chain(&error),
                    "could not open Snapmaker mTLS publisher; polling without waking the camera daemon"
                );
                None
            }
        },
        None => None,
    };

    let session = CameraSession::for_endpoint(&source.endpoint);
    if let Some(publisher) = publisher.as_ref() {
        if let Err(error) = publisher.start_monitor().await {
            warn!(
                device_id = %stream.device_id,
                error = %error_chain(&error),
                "Snapmaker camera start_monitor failed; polling anyway in case daemon is already active"
            );
        } else {
            info!(
                device_id = %stream.device_id,
                poll_url = %session.poll_url,
                "Snapmaker camera start_monitor succeeded"
            );
        }
    }

    let mut delay = RETRY_INITIAL_DELAY;
    let mut last_modified: Option<String> = None;

    while stream.clients.load(Ordering::SeqCst) > 0 {
        let attempt = tokio::select! {
            result = poll_once(&client, &session, last_modified.as_deref()) => result,
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

    if let Some(publisher) = publisher {
        match tokio::time::timeout(STOP_MONITOR_BUDGET, publisher.stop_monitor()).await {
            Ok(Ok(())) => {
                debug!(device_id = %stream.device_id, "Snapmaker camera monitor stopped")
            }
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
        publisher.shutdown().await;
    }
}

/// What the camera daemon told us when monitor mode started. We only use
/// the URL right now — the `pw`/`salt`/`iterations` fields the daemon
/// sometimes returns appear to be informational (the JPEG endpoint serves
/// frames without authentication once monitor mode is active), so we
/// ignore them until evidence shows otherwise.
struct CameraSession {
    poll_url: String,
}

impl CameraSession {
    /// Build the poll URL. Despite the daemon returning a `url` field in
    /// its `camera.start_monitor` response (`/files/camera/monitor.jpg`),
    /// the path Orca actually polls — verified from a packet capture of a
    /// working session — is `/server/files/camera/monitor.jpg`. The
    /// daemon's `url` field is a moonraker-internal mount path; the
    /// HTTP-facing path keeps the `/server/` prefix.
    fn for_endpoint(endpoint: &SnapmakerEndpoint) -> Self {
        Self {
            poll_url: format!(
                "http://{host}:{port}/server/files/camera/monitor.jpg",
                host = endpoint.host,
                port = endpoint.port,
            ),
        }
    }
}

/// Shape of the `result` field on a successful `camera.start_monitor`
/// response. We only need to confirm the daemon accepted the request;
/// other fields the daemon returns (`url`, `pw`, `salt`, `iterations`)
/// appear to be informational on current firmware — the HTTP-facing
/// poll path is the constant `/server/files/camera/monitor.jpg`.
#[derive(Debug, Deserialize)]
struct StartMonitorResult {
    state: String,
}

fn parse_start_monitor_result(response: &serde_json::Value) -> Result<StartMonitorResult> {
    if let Some(error) = response.get("error") {
        anyhow::bail!("daemon returned JSON-RPC error: {error}");
    }
    let result = response
        .get("result")
        .context("response missing `result` field")?;
    serde_json::from_value(result.clone()).context("`result` field is not a StartMonitorResult")
}

/// Long-lived mTLS MQTT session against the printer's bespoke control
/// plane. Holds an `AsyncClient` plus the driver task that keeps the
/// event loop pumping (subscriptions, keepalives, broker pushes).
///
/// A persistent session matters because the camera daemon on the printer
/// only routes `<sn>/request` to itself for clients that are actively
/// subscribed to `<sn>/response` — the broker's view of "an authorized
/// session is present". A connect-publish-disconnect cycle is accepted at
/// the MQTT layer (PUBACK comes back) but the daemon never sees it.
struct SnapMqttPublisher {
    client: AsyncClient,
    request_topic: String,
    driver: JoinHandle<()>,
    pending: PendingResponses,
}

/// Registry of in-flight requests waiting for a `<sn>/response` publish.
type PendingResponses = Arc<Mutex<HashMap<u64, oneshot::Sender<serde_json::Value>>>>;

impl SnapMqttPublisher {
    async fn connect(source: &SnapmakerVideoSource, mtls: &SnapMqttCreds) -> Result<Self> {
        let sn = source.device_id.as_str();
        let mut options = MqttOptions::new(
            mtls.clientid.clone(),
            source.endpoint.host.clone(),
            mtls.port,
        );
        options.set_keep_alive(MQTT_KEEPALIVE);
        options.set_clean_session(true);
        options.set_transport(
            mtls::transport_for(mtls).context("could not build Snapmaker mTLS transport")?,
        );

        let (client, mut eventloop) = AsyncClient::new(options, 32);

        // Subscribe to `<sn>/response` — the daemon delivers replies to
        // our `<sn>/request` publishes there. The daemon will not route a
        // request to itself unless the publisher is also a present
        // subscriber of `<sn>/response`.
        let response_topic = format!("{sn}/response");
        client
            .subscribe(response_topic.clone(), QoS::AtMostOnce)
            .await
            .with_context(|| format!("failed to subscribe to {response_topic}"))?;

        wait_for_connack(&mut eventloop).await?;

        let pending: PendingResponses = Arc::new(Mutex::new(HashMap::new()));
        let driver_pending = Arc::clone(&pending);
        let driver_sn = sn.to_owned();
        let driver = tokio::spawn(drive_eventloop(
            eventloop,
            driver_sn,
            response_topic,
            driver_pending,
        ));

        Ok(Self {
            client,
            request_topic: format!("{sn}/request"),
            driver,
            pending,
        })
    }

    async fn start_monitor(&self) -> Result<()> {
        let response = self.invoke("camera.start_monitor").await?;
        let result = parse_start_monitor_result(&response)
            .with_context(|| format!("camera.start_monitor response did not parse: {response}"))?;
        if result.state != "success" {
            anyhow::bail!(
                "camera.start_monitor returned state `{}` (expected `success`)",
                result.state
            );
        }
        Ok(())
    }

    async fn stop_monitor(&self) -> Result<()> {
        self.invoke("camera.stop_monitor").await?;
        Ok(())
    }

    /// Publish `<method>` against `<sn>/request` and wait for the
    /// matching response on `<sn>/response`. Returns the full JSON-RPC
    /// envelope so the caller can pull out `result` or `error`.
    async fn invoke(&self, method: &str) -> Result<serde_json::Value> {
        let req_id = unix_millis_id();
        let (tx, rx) = oneshot::channel();
        self.register_pending(req_id, tx);

        let payload = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": {
                "domain": MONITOR_DOMAIN,
                "interval": 0,
                "expect_pw": true,
            },
            "id": req_id,
        }))
        .context("could not encode camera control payload")?;

        if let Err(error) = self
            .client
            .publish(self.request_topic.clone(), QoS::AtLeastOnce, false, payload)
            .await
        {
            self.cancel_pending(req_id);
            return Err(error).context("failed to enqueue camera control publish");
        }

        match timeout(MQTT_RESPONSE_BUDGET, rx).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_)) => anyhow::bail!("driver dropped response channel for id={req_id}"),
            Err(_) => {
                self.cancel_pending(req_id);
                anyhow::bail!(
                    "timed out after {}s waiting for `{method}` response (id={req_id})",
                    MQTT_RESPONSE_BUDGET.as_secs()
                );
            }
        }
    }

    fn register_pending(&self, id: u64, tx: oneshot::Sender<serde_json::Value>) {
        self.pending
            .lock()
            .expect("pending-responses mutex poisoned")
            .insert(id, tx);
    }

    fn cancel_pending(&self, id: u64) {
        self.pending
            .lock()
            .expect("pending-responses mutex poisoned")
            .remove(&id);
    }

    async fn shutdown(self) {
        let _ = self.client.disconnect().await;
        self.driver.abort();
        let _ = self.driver.await;
    }
}

fn unix_millis_id() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

async fn wait_for_connack(eventloop: &mut EventLoop) -> Result<()> {
    let deadline = tokio::time::Instant::now() + MQTT_CONNECT_BUDGET;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            anyhow::bail!(
                "timed out waiting for Snapmaker mTLS CONNACK after {}s",
                MQTT_CONNECT_BUDGET.as_secs()
            );
        }
        let event = tokio::time::timeout(remaining, eventloop.poll())
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting for Snapmaker mTLS CONNACK"))?
            .context("Snapmaker mTLS event loop failed during connect")?;
        if let Event::Incoming(Packet::ConnAck(ack)) = event {
            if ack.code != ConnectReturnCode::Success {
                anyhow::bail!("Snapmaker mTLS CONNECT rejected: {:?}", ack.code);
            }
            return Ok(());
        }
    }
}

async fn drive_eventloop(
    mut eventloop: EventLoop,
    sn: String,
    response_topic: String,
    pending: PendingResponses,
) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(publish))) if publish.topic == response_topic => {
                deliver_response(&publish.payload, &pending);
            }
            Ok(_) => {}
            Err(error) => {
                debug!(sn = %sn, error = %error, "Snapmaker mTLS event loop ended");
                return;
            }
        }
    }
}

fn deliver_response(payload: &[u8], pending: &PendingResponses) {
    let value: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(value) => value,
        Err(error) => {
            debug!(
                error = %error,
                "snap-mqtt: response payload was not JSON"
            );
            return;
        }
    };
    let Some(id) = value.get("id").and_then(|v| v.as_u64()) else {
        // Responses to other clients on the broker also land here; we
        // only care about the ones with our own ids.
        return;
    };
    let Some(sender) = pending
        .lock()
        .expect("pending-responses mutex poisoned")
        .remove(&id)
    else {
        return;
    };
    let _ = sender.send(value);
}

struct PollOutcome {
    frame: Option<Bytes>,
    last_modified: Option<String>,
}

async fn poll_once(
    client: &reqwest::Client,
    session: &CameraSession,
    last_modified: Option<&str>,
) -> Result<PollOutcome> {
    // Cache-bust each request. Orca uses `?_nocache=<unix_ms>_<n>`; nginx
    // and the printer's intermediate caches will otherwise hand back the
    // same JPEG over and over even after the daemon writes a new one.
    let nocache = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    let url = format!("{}?_nocache={nocache}", session.poll_url);
    let mut request = client.get(&url);
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
    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let www_authenticate = response
        .headers()
        .get(reqwest::header::WWW_AUTHENTICATE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let new_last_modified = response
        .headers()
        .get(LAST_MODIFIED)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let bytes = response
        .bytes()
        .await
        .context("failed to read Snapmaker camera body")?;
    if bytes.starts_with(&[0xff, 0xd8]) {
        return Ok(PollOutcome {
            frame: Some(Bytes::from(bytes.to_vec())),
            last_modified: new_last_modified,
        });
    }
    anyhow::bail!(
        "Snapmaker camera response is not a JPEG: status={status} content_type={content_type:?} www_authenticate={www_authenticate:?} body_bytes={n} body_preview={preview:?}",
        n = bytes.len(),
        preview = preview_body(&bytes),
    );
}

fn preview_body(bytes: &[u8]) -> String {
    const MAX: usize = 256;
    let slice = if bytes.len() > MAX {
        &bytes[..MAX]
    } else {
        bytes
    };
    match std::str::from_utf8(slice) {
        Ok(text) => text.to_owned(),
        Err(_) => slice
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(" "),
    }
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
