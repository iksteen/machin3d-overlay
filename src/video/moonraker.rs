//! Moonraker / Klipper camera worker.
//!
//! Polls the printer's camera JPEG and fans frames out to the shared stream.
//!
//! Two things here are still Snapmaker-U1-shaped rather than fully generic:
//! the poll URL (`CameraSession::for_endpoint`) is the U1's legacy
//! `/server/files/camera/monitor.jpg` path rather than a generic
//! `/server/webcams/list` lookup, and the U1's camera daemon must be armed —
//! and re-armed inside its ~6 minute watchdog — before it captures anything
//! ([`super::u1_camera`], which owns those details). The wake is invoked as a
//! hook so the poll loop itself stays vendor-neutral.

use std::{
    sync::{atomic::Ordering, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use reqwest::header::{IF_MODIFIED_SINCE, LAST_MODIFIED};
use tokio::time::{sleep, Instant};
use tracing::warn;

use crate::{errors::error_chain, moonraker::MoonrakerEndpoint, service::ShutdownReceiver};

use super::{source::MoonrakerVideoSource, stream::DeviceVideoStream, u1_camera};

const POLL_INTERVAL: Duration = Duration::from_millis(250);
const POLL_TIMEOUT: Duration = Duration::from_secs(8);
const RETRY_INITIAL_DELAY: Duration = Duration::from_secs(2);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

pub(super) async fn run_moonraker_stream_worker(
    source: &MoonrakerVideoSource,
    stream: Arc<DeviceVideoStream>,
    mut shutdown: ShutdownReceiver,
) {
    let client = match reqwest::Client::builder().timeout(POLL_TIMEOUT).build() {
        Ok(client) => client,
        Err(error) => {
            warn!(
                device_id = %stream.device_id,
                %error,
                "failed to build Moonraker camera HTTP client"
            );
            return;
        }
    };

    let session = CameraSession::for_endpoint(&source.endpoint);
    // Snapmaker U1: the camera daemon captures only while monitor mode is
    // armed, and disarms itself ~6 minutes after the last request — so wake it
    // before the first poll and re-arm every heartbeat. A plain Moonraker
    // printer just logs the rejected method and streams (or doesn't) on its own.
    let camera = u1_camera::open_control(source).await;
    let mut next_wake = Instant::now();

    let mut delay = RETRY_INITIAL_DELAY;
    let mut last_modified: Option<String> = None;

    while stream.clients.load(Ordering::SeqCst) > 0 {
        if Instant::now() >= next_wake {
            tokio::select! {
                _ = camera.start_monitor(source) => {}
                _ = shutdown.cancelled() => break,
            }
            next_wake = Instant::now() + u1_camera::HEARTBEAT;
        }
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
                    "Moonraker camera poll failed"
                );
                tokio::select! {
                    _ = sleep_or_no_clients(&stream, delay) => {}
                    _ = shutdown.cancelled() => break,
                }
                delay = (delay + delay / 2).min(RETRY_MAX_DELAY);
            }
        }
    }

    camera.stop(source).await;
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
    fn for_endpoint(endpoint: &MoonrakerEndpoint) -> Self {
        Self {
            poll_url: format!(
                "http://{host}:{port}/server/files/camera/monitor.jpg",
                host = endpoint.host,
                port = endpoint.port,
            ),
        }
    }
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
        .context("Moonraker camera HTTP request failed")?;
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
        .context("failed to read Moonraker camera body")?;
    if bytes.starts_with(&[0xff, 0xd8]) {
        return Ok(PollOutcome {
            frame: Some(Bytes::from(bytes.to_vec())),
            last_modified: new_last_modified,
        });
    }
    anyhow::bail!(
        "Moonraker camera response is not a JPEG: status={status} content_type={content_type:?} www_authenticate={www_authenticate:?} body_bytes={n} body_preview={preview:?}",
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
