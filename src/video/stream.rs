use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::{bail, Result};
use bytes::Bytes;
use tokio::{
    sync::{
        broadcast,
        mpsc::{self, error::TrySendError},
        Mutex, Notify,
    },
    task::JoinSet,
};
use tokio_native_tls::TlsConnector;
use tracing::error;

use crate::{device_tls, devices::DeviceRegistry, service::ShutdownReceiver};

use super::{
    connection::run_stream_worker,
    endpoint::VideoEndpoint,
    session::resolve_session,
    worker::{observe_worker, VideoWorkerExit, VideoWorkerHandle, VideoWorkerTask},
};

const WORKER_QUEUE_CAPACITY: usize = 64;

#[derive(Clone)]
pub struct VideoStreams {
    state: Arc<VideoState>,
}

pub(super) struct VideoState {
    pub(super) registry: DeviceRegistry,
    pub(super) endpoints: Vec<VideoEndpoint>,
    pub(super) tls: TlsConnector,
    pub(super) device_streams: Mutex<HashMap<String, Arc<DeviceVideoStream>>>,
    pub(super) remembered_endpoints: Mutex<HashMap<String, VideoEndpoint>>,
    worker_tx: mpsc::Sender<VideoWorkerTask>,
    worker_rx: Mutex<mpsc::Receiver<VideoWorkerTask>>,
}

pub(super) struct DeviceVideoStream {
    pub(super) device_id: String,
    pub(super) frames: broadcast::Sender<Bytes>,
    pub(super) clients: AtomicUsize,
    pub(super) no_clients: Notify,
    worker: Mutex<Option<VideoWorkerHandle>>,
}

pub(crate) struct VideoSubscription {
    receiver: broadcast::Receiver<Bytes>,
    _guard: VideoSubscriptionGuard,
}

struct VideoSubscriptionGuard {
    stream: Arc<DeviceVideoStream>,
}

impl VideoStreams {
    pub(crate) fn new(
        registry: DeviceRegistry,
        endpoints: Vec<VideoEndpoint>,
        remembered_endpoints: HashMap<String, VideoEndpoint>,
    ) -> Result<Self> {
        let tls = device_tls::tokio_connector()?;
        let (worker_tx, worker_rx) = mpsc::channel(WORKER_QUEUE_CAPACITY);
        Ok(Self {
            state: Arc::new(VideoState {
                registry,
                endpoints,
                tls,
                device_streams: Mutex::new(HashMap::new()),
                remembered_endpoints: Mutex::new(remembered_endpoints),
                worker_tx,
                worker_rx: Mutex::new(worker_rx),
            }),
        })
    }

    pub async fn subscribe(&self, device_id: Option<&str>) -> Result<VideoSubscription> {
        if self.state.endpoints.is_empty() {
            bail!("video stream is disabled; set at least one --video-device");
        }

        let session = resolve_session(&self.state, device_id).await?;
        let stream = self.device_stream(&session.device_id).await;
        let receiver = stream.frames.subscribe();
        stream.clients.fetch_add(1, Ordering::SeqCst);
        let guard = VideoSubscriptionGuard {
            stream: Arc::clone(&stream),
        };
        self.ensure_worker(stream).await?;

        Ok(VideoSubscription {
            receiver,
            _guard: guard,
        })
    }

    pub async fn known_device_ids(&self) -> HashSet<String> {
        self.state
            .remembered_endpoints
            .lock()
            .await
            .keys()
            .cloned()
            .collect()
    }

    pub(crate) async fn watch_workers(&self, mut shutdown: ShutdownReceiver) {
        let mut worker_rx = self.state.worker_rx.lock().await;
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
            .state
            .device_streams
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for stream in streams {
            stream.abort_worker().await;
        }
    }

    async fn device_stream(&self, device_id: &str) -> Arc<DeviceVideoStream> {
        let mut streams = self.state.device_streams.lock().await;
        if let Some(stream) = streams.get(device_id) {
            return Arc::clone(stream);
        }

        let (frames, _) = broadcast::channel(4);
        let stream = Arc::new(DeviceVideoStream {
            device_id: device_id.to_owned(),
            frames,
            clients: AtomicUsize::new(0),
            no_clients: Notify::new(),
            worker: Mutex::new(None),
        });
        streams.insert(device_id.to_owned(), Arc::clone(&stream));
        stream
    }

    async fn ensure_worker(&self, stream: Arc<DeviceVideoStream>) -> Result<()> {
        let mut worker = stream.worker.lock().await;
        if worker.as_ref().is_some_and(|worker| !worker.is_finished()) {
            return Ok(());
        }
        *worker = Some(spawn_worker(&self.state, &stream)?);
        Ok(())
    }
}

fn spawn_worker(
    state: &Arc<VideoState>,
    stream: &Arc<DeviceVideoStream>,
) -> Result<VideoWorkerHandle> {
    let finished = Arc::new(AtomicBool::new(false));
    let finished_for_task = Arc::clone(&finished);
    let device_id = stream.device_id.clone();
    let worker_state = Arc::clone(state);
    let worker_stream = Arc::clone(stream);
    let handle = tokio::spawn(async move {
        run_stream_worker(worker_state, worker_stream).await;
        finished_for_task.store(true, Ordering::SeqCst);
    });
    let abort = handle.abort_handle();
    if let Err(error) = state.worker_tx.try_send(VideoWorkerTask {
        device_id,
        finished: Arc::clone(&finished),
        handle,
    }) {
        let (reason, task) = match error {
            TrySendError::Full(task) => ("full", task),
            TrySendError::Closed(task) => ("closed", task),
        };
        task.handle.abort();
        task.finished.store(true, Ordering::SeqCst);
        bail!("video worker lifecycle monitor queue is {reason}");
    }
    Ok(VideoWorkerHandle::new(abort, finished))
}

fn log_worker_observer_result(
    result: std::result::Result<VideoWorkerExit, tokio::task::JoinError>,
) {
    match result {
        Ok(exit) => exit.log(),
        Err(error) => {
            error!(%error, "video worker observer task failed");
        }
    }
}

impl VideoSubscription {
    pub(crate) async fn recv(&mut self) -> Result<Bytes, broadcast::error::RecvError> {
        self.receiver.recv().await
    }
}

impl Drop for VideoSubscriptionGuard {
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

impl DeviceVideoStream {
    async fn abort_worker(&self) {
        if let Some(worker) = self.worker.lock().await.as_ref() {
            worker.abort();
        }
    }
}
