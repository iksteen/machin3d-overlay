use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Result};
use tokio::sync::Semaphore;
use tokio::task::{Id, JoinError, JoinSet};
use tracing::{debug, error, warn};

use crate::{
    bambu::PrinterStatus,
    cloud::CloudSession,
    devices::DeviceRegistry,
    mqtt::{MqttDeviceState, MqttRuntime},
    service::ShutdownReceiver,
};

use super::{
    cache::TaskKey,
    error_chain,
    jobs::{JobCompletion, JobOrder, JobStart, ThumbnailJob, ThumbnailJobs},
    source, ThumbnailStatus,
};

const LOADING_RETRY_DELAY: Duration = Duration::from_secs(2);
const MISSING_RETRY_DELAY: Duration = Duration::from_secs(30);
const MAX_CONCURRENT_THUMBNAIL_FETCHES: usize = 3;

#[derive(Clone)]
pub(crate) struct ThumbnailService {
    inner: Arc<ThumbnailInner>,
}

struct ThumbnailInner {
    mqtt: MqttRuntime,
    cloud: Option<CloudSession>,
    registry: DeviceRegistry,
    jobs: ThumbnailJobs,
    fetch_permits: Semaphore,
}

impl ThumbnailService {
    pub(crate) fn new(
        mqtt: MqttRuntime,
        cloud: Option<CloudSession>,
        registry: DeviceRegistry,
    ) -> Self {
        Self {
            inner: Arc::new(ThumbnailInner {
                mqtt,
                cloud,
                registry,
                jobs: ThumbnailJobs::new(),
                fetch_permits: Semaphore::new(MAX_CONCURRENT_THUMBNAIL_FETCHES),
            }),
        }
    }

    pub(crate) async fn thumbnail(
        &self,
        requested_device_id: Option<&str>,
    ) -> Result<ThumbnailStatus> {
        let Some(device_id) = self.select_device_id(requested_device_id)? else {
            return Ok(ThumbnailStatus::Missing("no device selected".to_owned()));
        };

        self.refresh_device(&device_id).await?;
        Ok(self.inner.jobs.status(&device_id).await)
    }

    pub(crate) async fn watch_task_changes(&self, mut shutdown: ShutdownReceiver) {
        let mut changes = self.inner.mqtt.subscribe();
        let mut jobs = JoinSet::new();
        let mut running_jobs = HashMap::new();
        self.refresh_changed_devices().await;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    jobs.abort_all();
                    while let Some(result) = jobs.join_next_with_id().await {
                        log_thumbnail_job_result(result);
                    }
                    return;
                }
                received = changes.recv() => {
                    if received.is_err() {
                        changes = self.inner.mqtt.subscribe();
                    }
                    self.refresh_changed_devices().await;
                }
                received = self.inner.jobs.next_job() => {
                    let Some(job) = received else {
                        warn!("thumbnail job queue closed");
                        return;
                    };
                    let service = self.clone();
                    let tracked_job = job.clone();
                    let handle = jobs.spawn(async move {
                        service.run_fetch_job(job).await;
                    });
                    running_jobs.insert(handle.id(), tracked_job);
                }
                Some(result) = jobs.join_next_with_id(), if !jobs.is_empty() => {
                    self.handle_thumbnail_job_result(result, &mut running_jobs).await;
                }
            }
        }
    }

    async fn refresh_changed_devices(&self) {
        let snapshot = self.inner.mqtt.snapshot().await;
        let order = JobOrder::new(snapshot.revision);
        for device in self.inner.registry.devices() {
            let device_id = device.id.as_str();
            if let Err(error) = self
                .refresh_snapshot_device(device_id, snapshot.devices.get(device_id), order)
                .await
            {
                warn!(
                    device_id,
                    error = %error_chain(&error),
                    "failed to schedule print thumbnail refresh"
                );
            }
        }
    }

    async fn refresh_device(&self, device_id: &str) -> Result<()> {
        let snapshot = self.inner.mqtt.snapshot().await;
        self.refresh_snapshot_device(
            device_id,
            snapshot.devices.get(device_id),
            JobOrder::new(snapshot.revision),
        )
        .await
    }

    async fn refresh_snapshot_device(
        &self,
        device_id: &str,
        state: Option<&MqttDeviceState>,
        order: JobOrder,
    ) -> Result<()> {
        let Some(state) = state else {
            self.inner.jobs.clear(device_id, order).await;
            return Ok(());
        };
        let Some(task) = TaskKey::from_state(state) else {
            self.inner.jobs.clear(device_id, order).await;
            return Ok(());
        };
        self.schedule_fetch(device_id, &state.report, task, order)
            .await
    }

    async fn schedule_fetch(
        &self,
        device_id: &str,
        report: &PrinterStatus,
        task: TaskKey,
        order: JobOrder,
    ) -> Result<()> {
        let scheduled = self
            .inner
            .jobs
            .schedule(device_id.to_owned(), task, report.clone(), order)
            .await;
        if let Some(job) = scheduled {
            self.enqueue_job(device_id, job).await
        } else {
            Ok(())
        }
    }

    async fn run_fetch_job(&self, job: ThumbnailJob) {
        let device_id = job.device_id.as_str();
        match self.inner.jobs.start(&job).await {
            JobStart::Fetch => {}
            JobStart::StartPending(next_job) => {
                self.enqueue_pending_job(device_id, next_job).await;
                return;
            }
            JobStart::Stale => return,
        }

        let Ok(_permit) = self.inner.fetch_permits.acquire().await else {
            self.finish_failed_job(
                job,
                "thumbnail fetch concurrency limiter is closed".to_owned(),
            )
            .await;
            return;
        };

        let (status, retry_after) = match source::fetch_thumbnail(
            self.inner.cloud.as_ref(),
            &self.inner.registry,
            device_id,
            &job.report,
        )
        .await
        {
            Ok(ThumbnailStatus::Ready(image)) => {
                debug!(device_id, "cached print thumbnail");
                (ThumbnailStatus::Ready(image), None)
            }
            Ok(ThumbnailStatus::Loading(message)) => {
                debug!(device_id, message, "print thumbnail is not ready yet");
                (
                    ThumbnailStatus::Loading(message),
                    Some(Instant::now() + LOADING_RETRY_DELAY),
                )
            }
            Ok(ThumbnailStatus::Missing(message)) => (
                ThumbnailStatus::Missing(message),
                Some(Instant::now() + MISSING_RETRY_DELAY),
            ),
            Ok(ThumbnailStatus::Unavailable(message)) => (
                ThumbnailStatus::Unavailable(message),
                Some(Instant::now() + MISSING_RETRY_DELAY),
            ),
            Err(error) => {
                let message = error_chain(&error);
                warn!(
                    device_id,
                    error = %message,
                    "print thumbnail is unavailable"
                );
                (
                    ThumbnailStatus::Unavailable(message),
                    Some(Instant::now() + MISSING_RETRY_DELAY),
                )
            }
        };

        match self.inner.jobs.finish(&job, status, retry_after).await {
            JobCompletion::Store => {}
            JobCompletion::StartPending(next_job) => {
                self.enqueue_pending_job(device_id, next_job).await;
            }
            JobCompletion::Stale => {}
        }
    }

    async fn enqueue_pending_job(&self, device_id: &str, job: Box<ThumbnailJob>) {
        if let Err(error) = self.enqueue_job(device_id, *job).await {
            warn!(
                device_id,
                error = %error_chain(&error),
                "failed to enqueue pending print thumbnail refresh"
            );
        }
    }

    async fn enqueue_job(&self, device_id: &str, job: ThumbnailJob) -> Result<()> {
        let task = job.task.clone();
        let order = job.order;
        if let Err(error) = self.inner.jobs.enqueue(job) {
            self.inner
                .jobs
                .mark_enqueue_failed(
                    device_id,
                    task,
                    order,
                    error.to_string(),
                    Instant::now() + MISSING_RETRY_DELAY,
                )
                .await;
            return Err(error);
        }

        Ok(())
    }

    async fn handle_thumbnail_job_result(
        &self,
        result: std::result::Result<(Id, ()), JoinError>,
        running_jobs: &mut HashMap<Id, ThumbnailJob>,
    ) {
        match result {
            Ok((id, ())) => {
                running_jobs.remove(&id);
            }
            Err(error) if error.is_cancelled() => {
                running_jobs.remove(&error.id());
                debug!("thumbnail worker task cancelled");
            }
            Err(error) => {
                let job = running_jobs.remove(&error.id());
                error!(%error, "thumbnail worker task failed");
                if let Some(job) = job {
                    self.finish_failed_job(job, error.to_string()).await;
                }
            }
        }
    }

    async fn finish_failed_job(&self, job: ThumbnailJob, message: String) {
        let device_id = job.device_id.clone();
        let retry_after = Instant::now() + MISSING_RETRY_DELAY;
        match self
            .inner
            .jobs
            .finish(
                &job,
                ThumbnailStatus::Unavailable(message),
                Some(retry_after),
            )
            .await
        {
            JobCompletion::Store | JobCompletion::Stale => {}
            JobCompletion::StartPending(next_job) => {
                self.enqueue_pending_job(&device_id, next_job).await;
            }
        }
    }

    fn select_device_id(&self, requested_device_id: Option<&str>) -> Result<Option<String>> {
        let requested_device_id = requested_device_id
            .map(str::trim)
            .filter(|device_id| !device_id.is_empty());
        if let Some(device_id) = requested_device_id {
            ensure!(
                self.inner.registry.get(device_id).is_some(),
                "device `{device_id}` is not known"
            );
            return Ok(Some(device_id.to_owned()));
        }

        Ok(self
            .inner
            .registry
            .first()
            .map(|entry| entry.id().to_owned()))
    }
}

fn log_thumbnail_job_result(result: std::result::Result<(Id, ()), JoinError>) {
    match result {
        Ok((_, ())) => {}
        Err(error) if error.is_cancelled() => {
            debug!("thumbnail worker task cancelled");
        }
        Err(error) => {
            error!(%error, "thumbnail worker task failed");
        }
    }
}
