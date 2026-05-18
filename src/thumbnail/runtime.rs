use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context, Result};
use tokio::task::{Id, JoinError, JoinSet};
use tracing::{debug, error, warn};

use crate::{
    bambu::PrinterStatus, cloud::CloudSession, devices::DeviceRegistry, mqtt::MqttRuntime,
    service::ShutdownReceiver,
};

use super::{
    cache::TaskKey,
    cloud, error_chain,
    jobs::{JobCompletion, JobOrder, JobSchedule, JobStart, ThumbnailJob, ThumbnailJobs},
    local, ThumbnailStatus,
};

const LOADING_RETRY_DELAY: Duration = Duration::from_secs(2);
const MISSING_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(crate) struct ThumbnailRuntime {
    inner: Arc<ThumbnailInner>,
}

struct ThumbnailInner {
    mqtt: MqttRuntime,
    cloud: Option<CloudSession>,
    registry: DeviceRegistry,
    jobs: ThumbnailJobs,
}

impl ThumbnailRuntime {
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
            }),
        }
    }

    pub(crate) async fn thumbnail(
        &self,
        requested_device_id: Option<&str>,
    ) -> Result<ThumbnailStatus> {
        let Some(device_id) = self.select_device_id(requested_device_id).await? else {
            return Ok(ThumbnailStatus::Missing("no device selected".to_owned()));
        };

        self.refresh_device(&device_id).await?;
        Ok(self.inner.jobs.status(&device_id).await)
    }

    pub(crate) async fn watch_task_changes(&self, mut shutdown: ShutdownReceiver) {
        let mut changes = self.inner.mqtt.subscribe();
        let mut job_rx = self.inner.jobs.receiver.lock().await;
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
                Some(job) = job_rx.recv() => {
                    let runtime = self.clone();
                    let tracked_job = job.clone();
                    let handle = jobs.spawn(async move {
                        runtime.run_fetch_job(job).await;
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
        let states = snapshot.devices;
        for device in self.inner.registry.devices() {
            let device_id = device.id.as_str();
            let Some(state) = states.get(device_id) else {
                self.inner
                    .jobs
                    .clear(device_id, JobOrder::new(snapshot.revision))
                    .await;
                continue;
            };
            let Some(task) = TaskKey::from_state(state) else {
                self.inner
                    .jobs
                    .clear(device_id, JobOrder::new(snapshot.revision))
                    .await;
                continue;
            };
            if let Err(error) = self
                .schedule_fetch(
                    device_id,
                    &state.report,
                    task,
                    JobOrder::new(snapshot.revision),
                )
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
        let states = snapshot.devices;
        let Some(state) = states.get(device_id) else {
            self.inner
                .jobs
                .clear(device_id, JobOrder::new(snapshot.revision))
                .await;
            return Ok(());
        };
        let Some(task) = TaskKey::from_state(state) else {
            self.inner
                .jobs
                .clear(device_id, JobOrder::new(snapshot.revision))
                .await;
            return Ok(());
        };
        self.schedule_fetch(
            device_id,
            &state.report,
            task,
            JobOrder::new(snapshot.revision),
        )
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
            .schedule(device_id.to_owned(), task.clone(), report.clone(), order)
            .await;
        if matches!(scheduled, JobSchedule::Unchanged) {
            return Ok(());
        }

        if let JobSchedule::Start(job) = scheduled {
            if let Err(error) = self.inner.jobs.send(*job) {
                self.inner
                    .jobs
                    .send_failed(
                        device_id,
                        task,
                        order,
                        error.to_string(),
                        Instant::now() + MISSING_RETRY_DELAY,
                    )
                    .await;
                return Err(error);
            }
        }

        Ok(())
    }

    async fn run_fetch_job(&self, job: ThumbnailJob) {
        let device_id = job.device_id.as_str();
        match self.inner.jobs.start(&job).await {
            JobStart::Fetch => {}
            JobStart::StartPending(next_job) => {
                self.send_pending_job(device_id, next_job).await;
                return;
            }
            JobStart::Stale => return,
        }

        let (status, retry_after) = match self.fetch_thumbnail(device_id, &job.report).await {
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
            Err(error) => {
                let message = error_chain(&error);
                warn!(
                    device_id,
                    error = %message,
                    "print thumbnail is unavailable"
                );
                (
                    ThumbnailStatus::Missing(message),
                    Some(Instant::now() + MISSING_RETRY_DELAY),
                )
            }
        };

        match self.inner.jobs.finish(&job, status, retry_after).await {
            JobCompletion::Store => {}
            JobCompletion::StartPending(next_job) => {
                self.send_pending_job(device_id, next_job).await;
            }
            JobCompletion::Stale => {}
        }
    }

    async fn send_pending_job(&self, device_id: &str, job: Box<ThumbnailJob>) {
        let pending_task = job.task.clone();
        let pending_order = job.order;
        if let Err(error) = self.inner.jobs.send(*job) {
            self.inner
                .jobs
                .send_failed(
                    device_id,
                    pending_task,
                    pending_order,
                    error.to_string(),
                    Instant::now() + MISSING_RETRY_DELAY,
                )
                .await;
            warn!(
                device_id,
                error = %error_chain(&error),
                "failed to schedule pending print thumbnail refresh"
            );
        }
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
            .finish(&job, ThumbnailStatus::Missing(message), Some(retry_after))
            .await
        {
            JobCompletion::Store | JobCompletion::Stale => {}
            JobCompletion::StartPending(next_job) => {
                self.send_pending_job(&device_id, next_job).await;
            }
        }
    }

    async fn fetch_thumbnail(
        &self,
        device_id: &str,
        report: &PrinterStatus,
    ) -> Result<ThumbnailStatus> {
        let entry = self
            .inner
            .registry
            .get(device_id)
            .with_context(|| format!("device `{device_id}` is not known"))?;

        if let Some(local) = entry.local() {
            return local::fetch_thumbnail(device_id, local, report).await;
        }
        if entry.has_cloud_mqtt() {
            return cloud::fetch_thumbnail(self.inner.cloud.as_ref(), device_id, report)
                .await
                .map(ThumbnailStatus::Ready);
        }

        anyhow::bail!("device `{device_id}` has no thumbnail data source")
    }

    async fn select_device_id(&self, requested_device_id: Option<&str>) -> Result<Option<String>> {
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
