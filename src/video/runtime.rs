use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

mod session;
mod worker;

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    sync::{broadcast, mpsc, Mutex, Notify},
    task::JoinSet,
};
use tokio_native_tls::TlsConnector;
use tracing::{error, info, warn};

use crate::{device_tls, devices::DeviceRegistry, service::ShutdownReceiver};

use super::{
    endpoint::VideoEndpoint,
    probe::connect_video_tcp,
    protocol::{auth_packet, is_jpeg, MAX_FRAME_SIZE},
};

use self::session::{candidate_endpoints, remember_endpoint, resolve_session, VideoSession};
use self::worker::{observe_worker, VideoWorkerHandle, VideoWorkerTask};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(15);
const RETRY_INITIAL_DELAY: Duration = Duration::from_secs(1);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct VideoRuntime {
    inner: Arc<VideoRuntimeInner>,
}

struct VideoRuntimeInner {
    registry: DeviceRegistry,
    endpoints: Vec<VideoEndpoint>,
    tls: TlsConnector,
    streams: Mutex<HashMap<String, Arc<VideoStream>>>,
    endpoint_map: Mutex<HashMap<String, VideoEndpoint>>,
    worker_tx: mpsc::UnboundedSender<VideoWorkerTask>,
    worker_rx: Mutex<mpsc::UnboundedReceiver<VideoWorkerTask>>,
}

struct VideoStream {
    device_id: String,
    frames: broadcast::Sender<Bytes>,
    clients: AtomicUsize,
    no_clients: Notify,
    worker: Mutex<Option<VideoWorkerHandle>>,
}

pub struct VideoSubscription {
    receiver: broadcast::Receiver<Bytes>,
    _guard: VideoClientGuard,
}

struct VideoClientGuard {
    stream: Arc<VideoStream>,
}

impl VideoRuntime {
    pub(crate) fn new(
        registry: DeviceRegistry,
        endpoints: Vec<VideoEndpoint>,
        endpoint_map: HashMap<String, VideoEndpoint>,
    ) -> Result<Self> {
        let tls = device_tls::tokio_connector()?;
        let (worker_tx, worker_rx) = mpsc::unbounded_channel();
        Ok(Self {
            inner: Arc::new(VideoRuntimeInner {
                registry,
                endpoints,
                tls,
                streams: Mutex::new(HashMap::new()),
                endpoint_map: Mutex::new(endpoint_map),
                worker_tx,
                worker_rx: Mutex::new(worker_rx),
            }),
        })
    }

    pub async fn subscribe(&self, device_id: Option<&str>) -> Result<VideoSubscription> {
        if self.inner.endpoints.is_empty() {
            bail!("video stream is disabled; set at least one --video-device");
        }

        let session = resolve_session(&self.inner, device_id).await?;
        let stream = self.stream_for_device(&session.device_id).await;
        let receiver = stream.frames.subscribe();
        stream.clients.fetch_add(1, Ordering::SeqCst);
        let guard = VideoClientGuard {
            stream: Arc::clone(&stream),
        };
        self.ensure_worker(stream).await?;

        Ok(VideoSubscription {
            receiver,
            _guard: guard,
        })
    }

    pub async fn known_device_ids(&self) -> HashSet<String> {
        self.inner
            .endpoint_map
            .lock()
            .await
            .keys()
            .cloned()
            .collect()
    }

    pub(crate) async fn watch_workers(&self, mut shutdown: ShutdownReceiver) {
        let mut worker_rx = self.inner.worker_rx.lock().await;
        let mut workers = JoinSet::new();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    self.abort_workers().await;
                    while let Some(result) = workers.join_next().await {
                        log_worker_observer_result(result);
                    }
                    break;
                }
                Some(task) = worker_rx.recv() => {
                    workers.spawn(observe_worker(task));
                }
                Some(result) = workers.join_next(), if !workers.is_empty() => {
                    log_worker_observer_result(result);
                }
            }
        }
    }

    async fn abort_workers(&self) {
        let streams = self
            .inner
            .streams
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for stream in streams {
            stream.abort_worker().await;
        }
    }

    async fn stream_for_device(&self, device_id: &str) -> Arc<VideoStream> {
        let mut streams = self.inner.streams.lock().await;
        if let Some(stream) = streams.get(device_id) {
            return Arc::clone(stream);
        }

        let (frames, _) = broadcast::channel(4);
        let stream = Arc::new(VideoStream {
            device_id: device_id.to_owned(),
            frames,
            clients: AtomicUsize::new(0),
            no_clients: Notify::new(),
            worker: Mutex::new(None),
        });
        streams.insert(device_id.to_owned(), Arc::clone(&stream));
        stream
    }

    async fn ensure_worker(&self, stream: Arc<VideoStream>) -> Result<()> {
        let mut worker = stream.worker.lock().await;
        if worker.as_ref().is_some_and(|worker| !worker.is_finished()) {
            return Ok(());
        }
        *worker = Some(spawn_worker(&self.inner, &stream)?);
        Ok(())
    }
}

fn log_worker_observer_result(
    result: std::result::Result<self::worker::VideoWorkerExit, tokio::task::JoinError>,
) {
    match result {
        Ok(exit) => exit.log(),
        Err(error) => {
            error!(%error, "video worker observer task failed");
        }
    }
}

impl VideoSubscription {
    pub async fn recv(&mut self) -> Result<Bytes, broadcast::error::RecvError> {
        self.receiver.recv().await
    }
}

impl Drop for VideoClientGuard {
    fn drop(&mut self) {
        if let Ok(previous) =
            self.stream
                .clients
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |clients| {
                    clients.checked_sub(1)
                })
        {
            if previous == 1 {
                self.stream.no_clients.notify_waiters();
            }
        }
    }
}

impl VideoStream {
    async fn abort_worker(&self) {
        if let Some(worker) = self.worker.lock().await.as_ref() {
            worker.abort();
        }
    }
}

fn spawn_worker(
    inner: &Arc<VideoRuntimeInner>,
    stream: &Arc<VideoStream>,
) -> Result<VideoWorkerHandle> {
    let finished = Arc::new(AtomicBool::new(false));
    let finished_for_task = Arc::clone(&finished);
    let device_id = stream.device_id.clone();
    let worker_inner = Arc::clone(inner);
    let worker_stream = Arc::clone(stream);
    let handle = tokio::spawn(async move {
        run_worker(worker_inner, worker_stream).await;
        finished_for_task.store(true, Ordering::SeqCst);
    });
    let abort = handle.abort_handle();
    if let Err(error) = inner.worker_tx.send(VideoWorkerTask {
        device_id,
        finished: Arc::clone(&finished),
        handle,
    }) {
        let task = error.0;
        task.handle.abort();
        task.finished.store(true, Ordering::SeqCst);
        bail!("video worker lifecycle monitor is not running");
    }
    Ok(VideoWorkerHandle::new(abort, finished))
}

async fn run_worker(inner: Arc<VideoRuntimeInner>, stream: Arc<VideoStream>) {
    let mut delay = RETRY_INITIAL_DELAY;
    while stream.clients.load(Ordering::SeqCst) > 0 {
        match stream_once(&inner, &stream).await {
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
                sleep_or_no_clients(&stream, delay).await;
                delay = (delay + delay / 2).min(RETRY_MAX_DELAY);
            }
        }
    }
}

async fn stream_once(inner: &VideoRuntimeInner, stream: &VideoStream) -> Result<()> {
    let session = resolve_session(inner, Some(&stream.device_id)).await?;
    let endpoints = candidate_endpoints(inner, &session.device_id).await;
    let mut last_error = None;

    for endpoint in endpoints {
        match stream_endpoint_once(inner, stream, &session, &endpoint).await {
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

async fn stream_endpoint_once(
    inner: &VideoRuntimeInner,
    video: &VideoStream,
    session: &VideoSession,
    endpoint: &VideoEndpoint,
) -> Result<()> {
    let address = endpoint.address();
    let tcp = connect_video_tcp(endpoint, CONNECT_TIMEOUT, "connecting to video server").await?;

    let mut socket = inner
        .tls
        .connect(&session.device_id, tcp)
        .await
        .with_context(|| format!("failed TLS handshake with video server at {address}"))?;
    let certificate_device_id = device_tls::peer_device_id(&socket)
        .context("video server certificate did not include a usable common name")?;
    if certificate_device_id != session.device_id {
        remember_endpoint(inner, &certificate_device_id, endpoint).await;
        bail!(
            "video endpoint certificate is for device `{certificate_device_id}`, not requested device `{}`",
            session.device_id
        );
    }

    socket
        .write_all(&auth_packet(&session.access_code)?)
        .await
        .context("failed to send video authentication packet")?;
    socket
        .flush()
        .await
        .context("failed to flush video authentication packet")?;

    info!(
        device_id = %session.device_id,
        address = %address,
        "connected to printer video stream"
    );

    let mut header = [0_u8; 16];
    while video.clients.load(Ordering::SeqCst) > 0 {
        if !read_exact_with_timeout(video, &mut socket, &mut header, "video frame header").await? {
            break;
        }
        let frame_size = u32::from_le_bytes(header[0..4].try_into().expect("u32 slice")) as usize;
        ensure!(
            (1..=MAX_FRAME_SIZE).contains(&frame_size),
            "invalid video frame size {frame_size}"
        );

        let mut frame = vec![0_u8; frame_size];
        if !read_exact_with_timeout(video, &mut socket, &mut frame, "video frame").await? {
            break;
        }
        if is_jpeg(&frame) {
            remember_endpoint(inner, &session.device_id, endpoint).await;
            let _ = video.frames.send(Bytes::from(frame));
        } else {
            warn!("discarding video frame without JPEG magic bytes");
        }
    }

    Ok(())
}

async fn sleep_or_no_clients(stream: &VideoStream, delay: Duration) {
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

async fn read_exact_with_timeout<S>(
    video: &VideoStream,
    stream: &mut S,
    buffer: &mut [u8],
    label: &str,
) -> Result<bool>
where
    S: AsyncRead + Unpin,
{
    let no_clients = video.no_clients.notified();
    tokio::pin!(no_clients);
    if video.clients.load(Ordering::SeqCst) == 0 {
        return Ok(false);
    }

    tokio::select! {
        read = tokio::time::timeout(READ_TIMEOUT, stream.read_exact(buffer)) => {
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
