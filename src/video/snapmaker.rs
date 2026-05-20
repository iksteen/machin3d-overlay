//! Snapmaker / Klipper camera worker.
//!
//! The U1 exposes the latest camera frame as a static JPEG at
//! `/server/files/camera/monitor.jpg`. The file is rewritten in place when a
//! new frame is available; the response includes `Last-Modified`, so we poll
//! with `If-Modified-Since` and only forward bytes when the frame actually
//! changed.

use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use reqwest::header::{IF_MODIFIED_SINCE, LAST_MODIFIED};
use tokio::time::sleep;
use tracing::warn;

use crate::snapmaker::SnapmakerEndpoint;

use super::stream::DeviceVideoStream;

const POLL_INTERVAL: Duration = Duration::from_millis(250);
const POLL_TIMEOUT: Duration = Duration::from_secs(8);
const RETRY_INITIAL_DELAY: Duration = Duration::from_secs(2);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

pub(super) async fn run_snapmaker_stream_worker(
    endpoint: SnapmakerEndpoint,
    stream: Arc<DeviceVideoStream>,
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
    let mut delay = RETRY_INITIAL_DELAY;
    let mut last_modified: Option<String> = None;

    while stream.clients.load(Ordering::SeqCst) > 0 {
        match poll_once(&client, &url, last_modified.as_deref(), &stream).await {
            Ok(outcome) => {
                if let Some(value) = outcome.last_modified {
                    last_modified = Some(value);
                }
                if let Some(bytes) = outcome.frame {
                    let _ = stream.frames.send(bytes);
                }
                delay = RETRY_INITIAL_DELAY;
                sleep_or_no_clients(&stream, POLL_INTERVAL).await;
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
                sleep_or_no_clients(&stream, delay).await;
                delay = (delay + delay / 2).min(RETRY_MAX_DELAY);
            }
        }
    }
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
