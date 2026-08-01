//! Snapmaker U1 camera-wake control plane.
//!
//! The U1's camera daemon only writes fresh frames to
//! `/server/files/camera/monitor.jpg` while "monitor" mode is active; by
//! default the file is frozen on the last captured frame, and the daemon's
//! watchdog ends capture ~361 s after the last `camera.start_monitor` (it
//! starts counting down at ~311 s), so monitor mode has to be re-armed on
//! [`HEARTBEAT`] for as long as anyone is watching.
//!
//! There are two ways to reach the daemon, both verified against firmware
//! 1.5.2.12:
//!
//! - the printer's bespoke per-printer **mTLS MQTT** control plane, from
//!   `snap-pair` material ([`SnapMqttPublisher`]) — works from anywhere that
//!   can reach the broker, and returns the daemon's own reply;
//! - Moonraker's **`camera.*` JSON-RPC repeater** over its WebSocket
//!   ([`repeater`]) — no certificate and no API key, because the U1 ships with
//!   the private-address ranges in `trusted_clients`, but only from such a
//!   trusted client IP.
//!
//! Paired devices use the credentialed path; everything else falls back to the
//! repeater. This is the one Snapmaker-specific bolt-on of the Moonraker video
//! path (see [`super::moonraker`]).

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use rumqttc::{AsyncClient, ConnectReturnCode, Event, EventLoop, MqttOptions, Packet, QoS};
use serde::Deserialize;
use serde_json::json;
use tokio::{sync::oneshot, task::JoinHandle, time::timeout};
use tracing::{debug, info, warn};

use crate::{
    errors::error_chain,
    moonraker::{u1::mtls, SnapMqttCreds},
};

use super::source::MoonrakerVideoSource;

/// How often the caller must re-arm monitor mode. Comfortably inside the
/// daemon's ~361 s watchdog; each `start_monitor` resets it.
pub(super) const HEARTBEAT: Duration = Duration::from_secs(120);

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
/// Tight overall budget for the stop_monitor cleanup so we don't blow the
/// parent shutdown grace period (5s by default).
const STOP_MONITOR_BUDGET: Duration = Duration::from_secs(3);

/// The transport this device's camera daemon is driven over, held for the
/// worker's lifetime and released via [`CameraControl::stop`].
pub(super) enum CameraControl {
    /// Paired U1: long-lived mTLS MQTT session.
    Mtls(SnapMqttPublisher),
    /// Unpaired U1 on a trusted LAN address: Moonraker's camera repeater.
    Repeater,
}

/// Open the camera control plane for this device. Prefers the paired mTLS
/// session and falls back to the unauthenticated repeater — a printer we
/// cannot reach over MQTT is usually still reachable over the same Moonraker
/// WebSocket the status backend already talks to.
pub(super) async fn open_control(source: &MoonrakerVideoSource) -> CameraControl {
    let Some(creds) = source.mtls.as_ref() else {
        return CameraControl::Repeater;
    };
    match SnapMqttPublisher::connect(source, creds).await {
        Ok(publisher) => {
            info!(
                device_id = %source.device_id,
                host = %source.endpoint.host,
                port = creds.port,
                "opened Snapmaker mTLS session for camera control"
            );
            CameraControl::Mtls(publisher)
        }
        Err(error) => {
            warn!(
                device_id = %source.device_id,
                error = %error_chain(&error),
                "could not open Snapmaker mTLS publisher; falling back to the Moonraker camera repeater"
            );
            CameraControl::Repeater
        }
    }
}

impl CameraControl {
    /// Arm (or re-arm) monitor mode. Failures are logged, not returned: a
    /// failed wake only means frames may be stale — another client may
    /// already be keeping the daemon awake — so the caller polls regardless.
    pub(super) async fn start_monitor(&self, source: &MoonrakerVideoSource) {
        let outcome = match self {
            CameraControl::Mtls(publisher) => publisher.start_monitor().await,
            CameraControl::Repeater => repeater::start_monitor(source).await,
        };
        match outcome {
            Ok(()) => debug!(device_id = %source.device_id, "Snapmaker camera monitor armed"),
            Err(error) => warn!(
                device_id = %source.device_id,
                error = %error_chain(&error),
                "Snapmaker camera start_monitor failed; polling anyway in case daemon is already active"
            ),
        }
    }

    /// Release the camera and tear the session down. The camera is a shared
    /// single-camera resource (timelapse and defect detection contend for it),
    /// so we hand it back when the last viewer leaves instead of waiting out
    /// the watchdog.
    pub(super) async fn stop(self, source: &MoonrakerVideoSource) {
        match self {
            CameraControl::Mtls(publisher) => publisher.stop_and_shutdown(&source.device_id).await,
            CameraControl::Repeater => {
                match timeout(STOP_MONITOR_BUDGET, repeater::stop_monitor(source)).await {
                    Ok(Ok(())) => {
                        debug!(device_id = %source.device_id, "Snapmaker camera monitor stopped")
                    }
                    Ok(Err(error)) => warn!(
                        device_id = %source.device_id,
                        error = %error_chain(&error),
                        "Snapmaker camera stop_monitor failed"
                    ),
                    Err(_) => {
                        warn!(device_id = %source.device_id, "Snapmaker camera stop_monitor timed out")
                    }
                }
            }
        }
    }
}

/// Moonraker's `camera.*` JSON-RPC repeater: it forwards the call onto the
/// printer's internal MQTT bus on our behalf, so no client certificate is
/// involved. Only reachable from an IP in the printer's `trusted_clients`
/// (every private range, on stock config) and only over the WebSocket — the
/// HTTP transport for these methods is disabled on the device.
mod repeater {
    use std::time::Duration;

    use anyhow::{Context, Result};
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{json, Value};
    use tokio::time::timeout;
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{client::IntoClientRequest, Message},
    };

    use super::{MoonrakerVideoSource, MONITOR_DOMAIN};

    /// Seconds between the captures the daemon writes to `monitor.jpg`.
    const CAPTURE_INTERVAL: u32 = 1;
    /// One request per socket, so a fixed id is enough to match the reply.
    const REQUEST_ID: u64 = 1;
    const CONNECT_BUDGET: Duration = Duration::from_secs(5);
    /// How long we listen for an objection before declaring success. Accepted
    /// calls are answered with *silence* — verified on firmware 1.5.2.12: a
    /// `camera.start_monitor` that demonstrably woke the daemon drew no reply
    /// in 20 s, while a bogus method came back `-32601` in 40 ms. So only
    /// failures speak, and they speak immediately.
    const OBJECTION_GRACE: Duration = Duration::from_secs(1);

    pub(super) async fn start_monitor(source: &MoonrakerVideoSource) -> Result<()> {
        let params = json!({
            "req_id": REQUEST_ID,
            "domain": MONITOR_DOMAIN,
            "interval": CAPTURE_INTERVAL,
            "expect_pw": false,
        });
        call(source, "camera.start_monitor", params).await
    }

    pub(super) async fn stop_monitor(source: &MoonrakerVideoSource) -> Result<()> {
        let params = json!({ "req_id": REQUEST_ID, "domain": MONITOR_DOMAIN });
        call(source, "camera.stop_monitor", params).await
    }

    /// The repeater is fire-and-forget: Moonraker publishes onto the printer's
    /// MQTT bus and says nothing, while the daemon's own reply (carrying
    /// `url`/`pw`) goes to the MQTT `camera/response` topic we cannot see from
    /// here. That costs us nothing — the frame path is the fixed URL
    /// `super::super::moonraker::CameraSession` builds. So we send, listen
    /// just long enough to catch a rejection, and treat silence as success.
    async fn call(source: &MoonrakerVideoSource, method: &str, params: Value) -> Result<()> {
        let url = format!(
            "ws://{}:{}/websocket",
            source.endpoint.host, source.endpoint.port
        );
        let request = url
            .as_str()
            .into_client_request()
            .with_context(|| format!("invalid Moonraker WebSocket URL `{url}`"))?;
        let (mut socket, _response) = timeout(CONNECT_BUDGET, connect_async(request))
            .await
            .with_context(|| format!("timed out connecting to Moonraker at {url}"))?
            .with_context(|| format!("failed to connect to Moonraker at {url}"))?;

        let payload = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": REQUEST_ID,
        });
        socket
            .send(Message::Text(payload.to_string()))
            .await
            .with_context(|| format!("failed to send `{method}` to {url}"))?;

        let objection = async {
            while let Some(message) = socket.next().await {
                let Message::Text(text) = message.context("Moonraker WS read failed")? else {
                    continue;
                };
                let value: Value = serde_json::from_str(&text)
                    .context("Moonraker WS sent a message that is not valid JSON")?;
                // Everything else on this socket is unsolicited notification
                // traffic (`notify_proc_stat_update` and friends).
                if value.get("id").and_then(Value::as_u64) != Some(REQUEST_ID) {
                    continue;
                }
                if let Some(error) = value.get("error") {
                    anyhow::bail!("Moonraker returned an error for `{method}`: {error}");
                }
                return Ok(());
            }
            anyhow::bail!("Moonraker closed before acting on `{method}`")
        };
        let outcome = timeout(OBJECTION_GRACE, objection).await.unwrap_or(Ok(()));
        let _ = socket.close(None).await;
        outcome
    }
}

/// Shape of the `result` field on a successful `camera.start_monitor`
/// response. We only need to confirm the daemon accepted the request;
/// other fields the daemon returns (`url`, `pw`, `salt`, `iterations`)
/// appear to be informational on current firmware — the HTTP-facing poll
/// URL is owned by [`super::moonraker`]'s `CameraSession`, not derived from
/// this response.
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
pub(super) struct SnapMqttPublisher {
    client: AsyncClient,
    request_topic: String,
    driver: JoinHandle<()>,
    pending: PendingResponses,
}

/// Registry of in-flight requests waiting for a `<sn>/response` publish.
type PendingResponses = Arc<Mutex<HashMap<u64, oneshot::Sender<serde_json::Value>>>>;

impl SnapMqttPublisher {
    async fn connect(source: &MoonrakerVideoSource, creds: &SnapMqttCreds) -> Result<Self> {
        let sn = source.device_id.as_str();
        let mut options = MqttOptions::new(
            creds.clientid.clone(),
            source.endpoint.host.clone(),
            creds.port,
        );
        options.set_keep_alive(MQTT_KEEPALIVE);
        options.set_clean_session(true);
        options.set_transport(
            mtls::transport_for(creds).context("could not build Snapmaker mTLS transport")?,
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

    /// Stop monitor mode and tear the session down, all within a tight
    /// budget so the cleanup never overruns the parent shutdown grace.
    pub(super) async fn stop_and_shutdown(self, device_id: &str) {
        match timeout(STOP_MONITOR_BUDGET, self.stop_monitor()).await {
            Ok(Ok(())) => debug!(device_id = %device_id, "Snapmaker camera monitor stopped"),
            Ok(Err(error)) => warn!(
                device_id = %device_id,
                error = %error_chain(&error),
                "Snapmaker camera stop_monitor failed"
            ),
            Err(_) => warn!(device_id = %device_id, "Snapmaker camera stop_monitor timed out"),
        }
        let _ = self.client.disconnect().await;
        self.driver.abort();
        let _ = self.driver.await;
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
