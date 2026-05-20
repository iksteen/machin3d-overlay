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
use tokio_native_tls::TlsConnector;
use tracing::error;

use crate::{
    backend::Backend,
    device_tls,
    devices::{DeviceEntry, DeviceRegistry},
    service::ShutdownReceiver,
};

use super::{
    connection::run_stream_worker,
    endpoint::VideoEndpoint,
    snapmaker::run_snapmaker_stream_worker,
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

pub(super) struct VideoState {
    pub(super) registry: DeviceRegistry,
    pub(super) endpoints_by_device: HashMap<String, Vec<VideoEndpoint>>,
    pub(super) tls: TlsConnector,
    pub(super) device_streams: Mutex<HashMap<String, Arc<DeviceVideoStream>>>,
    pub(super) remembered_endpoints: Mutex<HashMap<String, VideoEndpoint>>,
    worker_tx: mpsc::Sender<VideoWorkerTask>,
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
        endpoints_by_device: HashMap<String, Vec<VideoEndpoint>>,
    ) -> Result<(Self, VideoWorkerEvents)> {
        let tls = device_tls::tokio_connector()?;
        let (worker_tx, worker_rx) = mpsc::channel(WORKER_QUEUE_CAPACITY);
        let streams = Self {
            state: Arc::new(VideoState {
                registry,
                endpoints_by_device,
                tls,
                device_streams: Mutex::new(HashMap::new()),
                remembered_endpoints: Mutex::new(HashMap::new()),
                worker_tx,
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
        self.state
            .registry
            .entries()
            .iter()
            .filter(|entry| device_has_video(&self.state, entry))
            .map(|entry| entry.id().to_owned())
            .collect()
    }

    fn select_device(&self, requested: Option<&str>) -> Result<String> {
        let requested = requested.map(str::trim).filter(|id| !id.is_empty());
        if let Some(requested) = requested {
            let entry = self
                .state
                .registry
                .get(requested)
                .with_context(|| format!("device `{requested}` is not known"))?;
            if !device_has_video(&self.state, entry) {
                bail!(missing_video_message(&self.state, entry));
            }
            return Ok(entry.id().to_owned());
        }
        self.state
            .registry
            .entries()
            .iter()
            .find(|entry| device_has_video(&self.state, entry))
            .map(|entry| entry.id().to_owned())
            .ok_or_else(|| {
                anyhow!(
                    "video stream is disabled; set at least one --bbl-video-device or --snap-device"
                )
            })
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
    let worker_stream = Arc::clone(stream);

    let entry = state
        .registry
        .get(&device_id)
        .with_context(|| format!("device `{device_id}` is not known"))?;
    let handle = match entry.backend() {
        Backend::Bambu => {
            let worker_state = Arc::clone(state);
            tokio::spawn(async move {
                run_stream_worker(worker_state, worker_stream).await;
                finished_for_task.store(true, Ordering::SeqCst);
            })
        }
        Backend::Snapmaker => {
            let endpoint = entry
                .snapmaker_endpoint()
                .cloned()
                .with_context(|| format!("Snapmaker device `{device_id}` has no endpoint"))?;
            tokio::spawn(async move {
                run_snapmaker_stream_worker(endpoint, worker_stream).await;
                finished_for_task.store(true, Ordering::SeqCst);
            })
        }
    };
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

fn device_has_video(state: &VideoState, entry: &DeviceEntry) -> bool {
    match entry.backend() {
        Backend::Bambu => {
            state.endpoints_by_device.contains_key(entry.id()) && entry.has_access_code()
        }
        Backend::Snapmaker => entry.snapmaker_endpoint().is_some(),
    }
}

fn missing_video_message(state: &VideoState, entry: &DeviceEntry) -> String {
    match entry.backend() {
        Backend::Bambu => {
            if !state.endpoints_by_device.contains_key(entry.id()) {
                format!(
                    "device `{}` has no known video endpoint",
                    entry.id()
                )
            } else {
                format!(
                    "device `{}` does not include dev_access_code",
                    entry.id()
                )
            }
        }
        Backend::Snapmaker => format!(
            "device `{}` is a Snapmaker without a configured Moonraker endpoint",
            entry.id()
        ),
    }
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr};

    use crate::{bambu::CloudDevice, devices::DeviceRegistry, secret::Secret, video::VideoEndpoint};

    use super::VideoStreams;

    fn endpoint(value: &str) -> VideoEndpoint {
        VideoEndpoint::from_str(value).expect("endpoint should parse")
    }

    #[tokio::test]
    async fn subscribe_rejects_device_without_endpoint_even_when_video_is_enabled() {
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
        let (streams, _events) = VideoStreams::new(
            registry,
            HashMap::from([("printer-b".to_owned(), vec![endpoint("192.168.1.50")])]),
        )
        .expect("video streams should initialize");

        let error = match streams.subscribe(Some("printer-a")).await {
            Ok(_) => panic!("device without endpoint should not subscribe"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("printer-a"));
        assert!(error.to_string().contains("no known video endpoint"));
    }
}
