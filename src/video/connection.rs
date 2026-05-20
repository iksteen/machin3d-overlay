use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::{info, warn};

use crate::{device_tls, service::ShutdownReceiver};

use super::{
    endpoint::VideoEndpoint,
    probe::connect_video_tcp,
    protocol::{auth_packet, is_jpeg, MAX_FRAME_SIZE},
    session::{candidate_endpoints, remember_endpoint, resolve_session, VideoSession},
    stream::{DeviceVideoStream, VideoState},
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(15);
const RETRY_INITIAL_DELAY: Duration = Duration::from_secs(1);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

pub(super) async fn run_stream_worker(
    state: Arc<VideoState>,
    stream: Arc<DeviceVideoStream>,
    mut shutdown: ShutdownReceiver,
) {
    let mut delay = RETRY_INITIAL_DELAY;
    while stream.clients.load(Ordering::SeqCst) > 0 {
        let attempt = tokio::select! {
            result = run_stream_attempt(&state, &stream) => result,
            _ = shutdown.cancelled() => return,
        };
        match attempt {
            Ok(()) => delay = RETRY_INITIAL_DELAY,
            Err(error) => {
                if stream.clients.load(Ordering::SeqCst) == 0 {
                    break;
                }
                warn!(
                    device_id = %stream.device_id,
                    error = %error_chain(&error),
                    "video stream disconnected"
                );
                tokio::select! {
                    _ = sleep_or_no_clients(&stream, delay) => {}
                    _ = shutdown.cancelled() => return,
                }
                delay = (delay + delay / 2).min(RETRY_MAX_DELAY);
            }
        }
    }
}

async fn run_stream_attempt(state: &VideoState, stream: &DeviceVideoStream) -> Result<()> {
    let session = resolve_session(state, Some(&stream.device_id)).await?;
    let endpoints = candidate_endpoints(state, &session.device_id).await;
    let mut last_error = None;

    for endpoint in endpoints {
        match stream_from_endpoint(state, stream, &session, &endpoint).await {
            Ok(()) => return Ok(()),
            Err(_) if stream.clients.load(Ordering::SeqCst) == 0 => return Ok(()),
            Err(error) => {
                warn!(
                    device_id = %session.device_id,
                    endpoint = %endpoint,
                    error = %error_chain(&error),
                    "video endpoint failed"
                );
                last_error = Some(error);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no video endpoints configured")))
}

async fn stream_from_endpoint(
    state: &VideoState,
    device_stream: &DeviceVideoStream,
    session: &VideoSession,
    endpoint: &VideoEndpoint,
) -> Result<()> {
    let address = endpoint.address();
    let tcp = connect_video_tcp(endpoint, CONNECT_TIMEOUT, "connecting to video server").await?;
    let mut socket = authenticate_stream(state, tcp, &address, session).await?;

    info!(
        device_id = %session.device_id,
        address = %address,
        "connected to printer video stream"
    );

    let mut header = [0_u8; 16];
    while device_stream.clients.load(Ordering::SeqCst) > 0 {
        if !read_exact_or_no_clients(
            device_stream,
            &mut socket,
            &mut header,
            "video frame header",
        )
        .await?
        {
            break;
        }
        let frame_size = u32::from_le_bytes(header[0..4].try_into().expect("u32 slice")) as usize;
        ensure!(
            (1..=MAX_FRAME_SIZE).contains(&frame_size),
            "invalid video frame size {frame_size}"
        );

        let mut frame = vec![0_u8; frame_size];
        if !read_exact_or_no_clients(device_stream, &mut socket, &mut frame, "video frame").await? {
            break;
        }
        if is_jpeg(&frame) {
            remember_endpoint(state, &session.device_id, endpoint).await;
            let _ = device_stream.frames.send(Bytes::from(frame));
        } else {
            warn!("discarding video frame without JPEG magic bytes");
        }
    }

    Ok(())
}

async fn authenticate_stream(
    state: &VideoState,
    tcp: TcpStream,
    address: &str,
    session: &VideoSession,
) -> Result<tokio_native_tls::TlsStream<TcpStream>> {
    let mut socket = state
        .tls
        .connect(&session.device_id, tcp)
        .await
        .with_context(|| format!("failed TLS handshake with video server at {address}"))?;
    let certificate_device_id = device_tls::peer_device_id(&socket)
        .context("video server certificate did not include a usable common name")?;
    if certificate_device_id != session.device_id {
        bail!(
            "video endpoint certificate is for device `{certificate_device_id}`, not requested device `{}`",
            session.device_id
        );
    }

    socket
        .write_all(&auth_packet(session.access_code.expose())?)
        .await
        .context("failed to send video authentication packet")?;
    socket
        .flush()
        .await
        .context("failed to flush video authentication packet")?;
    Ok(socket)
}

async fn sleep_or_no_clients(stream: &DeviceVideoStream, delay: Duration) {
    let no_clients = stream.no_clients.notified();
    tokio::pin!(no_clients);
    if stream.clients.load(Ordering::SeqCst) == 0 {
        return;
    }
    tokio::select! {
        _ = tokio::time::sleep(delay) => {}
        _ = &mut no_clients => {}
    }
}

async fn read_exact_or_no_clients<S>(
    device_stream: &DeviceVideoStream,
    reader: &mut S,
    buffer: &mut [u8],
    label: &str,
) -> Result<bool>
where
    S: AsyncRead + Unpin,
{
    let no_clients = device_stream.no_clients.notified();
    tokio::pin!(no_clients);
    if device_stream.clients.load(Ordering::SeqCst) == 0 {
        return Ok(false);
    }

    tokio::select! {
        read = tokio::time::timeout(READ_TIMEOUT, reader.read_exact(buffer)) => {
            read
                .with_context(|| format!("timed out reading {label}"))?
                .with_context(|| format!("failed to read {label}"))?;
            Ok(true)
        }
        _ = &mut no_clients => Ok(false),
    }
}

fn error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}
