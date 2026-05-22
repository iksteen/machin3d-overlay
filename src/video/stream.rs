use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use tokio::{
    sync::{
        broadcast,
        mpsc::{self, error::TrySendError},
        Mutex, Notify,
    },
    task::JoinSet,
};
use tracing::error;

use crate::service::{Shutdown, ShutdownReceiver};

use super::{
    source::VideoSource,
    worker::{observe_worker, VideoWorkerExit, VideoWorkerHandle, VideoWorkerTask},
};

const WORKER_QUEUE_CAPACITY: usize = 64;

#[derive(Clone)]
pub struct VideoStreams {
    state: Arc<VideoState>,
}

/// Receives lifecycle events from spawned video stream workers. Held outside
/// `VideoStreams` (which is cloneable and shared) so `watch_workers` owns the
/// receiver by value; calling it more than once is now a type error.
pub struct VideoWorkerEvents {
    receiver: mpsc::Receiver<VideoWorkerTask>,
}

struct VideoState {
    sources: HashMap<String, Arc<VideoSource>>,
    device_streams: Mutex<HashMap<String, Arc<DeviceVideoStream>>>,
    worker_tx: mpsc::Sender<VideoWorkerTask>,
    shutdown: Shutdown,
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
        sources: HashMap<String, Arc<VideoSource>>,
        shutdown: Shutdown,
    ) -> Result<(Self, VideoWorkerEvents)> {
        let (worker_tx, worker_rx) = mpsc::channel(WORKER_QUEUE_CAPACITY);
        let streams = Self {
            state: Arc::new(VideoState {
                sources,
                device_streams: Mutex::new(HashMap::new()),
                worker_tx,
                shutdown,
            }),
        };
        Ok((
            streams,
            VideoWorkerEvents {
                receiver: worker_rx,
            },
        ))
    }

    pub async fn subscribe(&self, device_id: Option<&str>) -> Result<VideoSubscription> {
        let device_id = self.select_device(device_id)?;
        let stream = self.device_stream(&device_id).await;
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
        self.state.sources.keys().cloned().collect()
    }

    fn select_device(&self, requested: Option<&str>) -> Result<String> {
        let requested = requested.map(str::trim).filter(|id| !id.is_empty());
        if let Some(requested) = requested {
            if self.state.sources.contains_key(requested) {
                return Ok(requested.to_owned());
            }
            bail!("device `{requested}` has no configured video source");
        }
        self.state
            .sources
            .values()
            .next()
            .map(|source| source.device_id().to_owned())
            .ok_or_else(|| anyhow!("no devices have configured video sources"))
    }

    pub(crate) async fn watch_workers(
        &self,
        events: VideoWorkerEvents,
        mut shutdown: ShutdownReceiver,
    ) {
        let mut worker_rx = events.receiver;
        let mut workers = JoinSet::new();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    // Workers subscribe to the same shutdown via `state.shutdown`
                    // and exit cooperatively (e.g. the Snapmaker worker uses
                    // the window to send `camera.stop_monitor`). The parent
                    // `ServiceTasks` grace period will abort us if we exceed
                    // it.
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
    let worker_stream = Arc::clone(stream);
    let source = state
        .sources
        .get(&device_id)
        .cloned()
        .with_context(|| format!("device `{device_id}` has no configured video source"))?;
    let shutdown = state.shutdown.subscribe();
    let handle = tokio::spawn(async move {
        source.run(worker_stream, shutdown).await;
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

impl DeviceVideoStream {}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr};

    use crate::{
        bambu::CloudDevice,
        devices::DeviceRegistry,
        secret::Secret,
        service::Shutdown,
        video::{endpoint::VideoEndpoint, source::collect_sources},
    };

    use super::VideoStreams;

    fn endpoint(value: &str) -> VideoEndpoint {
        VideoEndpoint::from_str(value).expect("endpoint should parse")
    }

    #[tokio::test]
    async fn subscribe_rejects_device_without_a_configured_source() {
        let registry = DeviceRegistry::new(
            vec![
                CloudDevice {
                    id: Some("printer-a".to_owned()),
                    access_code: Some(Secret::new("11111111".to_owned())),
                    ..CloudDevice::default()
                },
                CloudDevice {
                    id: Some("printer-b".to_owned()),
                    access_code: Some(Secret::new("22222222".to_owned())),
                    ..CloudDevice::default()
                },
            ],
            Vec::new(),
        );
        let bambu_endpoints =
            HashMap::from([("printer-b".to_owned(), vec![endpoint("192.168.1.50")])]);
        let sources = collect_sources(&registry, &bambu_endpoints).expect("sources build");
        let (streams, _events) =
            VideoStreams::new(sources, Shutdown::new()).expect("video streams");

        let error = match streams.subscribe(Some("printer-a")).await {
            Ok(_) => panic!("device without a source should not subscribe"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("printer-a"));
        assert!(error.to_string().contains("no configured video source"));
    }
}
