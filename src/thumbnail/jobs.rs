use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use anyhow::{anyhow, Result};
use tokio::sync::{mpsc, Mutex};
use tracing::debug;

use crate::bambu::PrinterStatus;

pub(super) use super::job_state::{JobCompletion, JobOrder, JobSchedule, JobStart, ThumbnailJob};
use super::{
    cache::TaskKey,
    job_state::{JobToken, ThumbnailJobState},
    ThumbnailStatus,
};

pub(super) struct ThumbnailJobs {
    queue_tx: mpsc::UnboundedSender<ThumbnailJob>,
    queue_rx: Mutex<mpsc::UnboundedReceiver<ThumbnailJob>>,
    state: Mutex<ThumbnailJobState>,
    next_token: AtomicU64,
}

impl ThumbnailJobs {
    pub(super) fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            queue_tx: sender,
            queue_rx: Mutex::new(receiver),
            state: Mutex::new(ThumbnailJobState::default()),
            next_token: AtomicU64::new(1),
        }
    }

    pub(super) async fn status(&self, device_id: &str) -> ThumbnailStatus {
        let status = {
            let state = self.state.lock().await;
            state.status(device_id)
        };
        match &status {
            ThumbnailStatus::Ready(_) => {}
            ThumbnailStatus::Loading(error) => {
                debug!(device_id, error, "thumbnail is loading");
            }
            ThumbnailStatus::Missing(error) => {
                debug!(device_id, error, "thumbnail is unavailable");
            }
        }
        status
    }

    pub(super) async fn schedule(
        &self,
        device_id: String,
        task: TaskKey,
        report: PrinterStatus,
        order: JobOrder,
    ) -> JobSchedule {
        let job = ThumbnailJob::new(device_id, task, report, order, self.next_token());
        let mut state = self.state.lock().await;
        state.schedule(job)
    }

    pub(super) async fn clear(&self, device_id: &str, order: JobOrder) {
        let mut state = self.state.lock().await;
        state.clear(device_id, order);
    }

    pub(super) async fn start(&self, job: &ThumbnailJob) -> JobStart {
        let mut state = self.state.lock().await;
        state.start(job)
    }

    pub(super) async fn finish(
        &self,
        job: &ThumbnailJob,
        status: ThumbnailStatus,
        retry_after: Option<Instant>,
    ) -> JobCompletion {
        let mut state = self.state.lock().await;
        state.finish(job, status, retry_after)
    }

    pub(super) async fn mark_enqueue_failed(
        &self,
        device_id: &str,
        task: TaskKey,
        order: JobOrder,
        message: String,
        retry_after: Instant,
    ) {
        let mut state = self.state.lock().await;
        state.mark_enqueue_failed(device_id, task, order, message, retry_after);
    }

    pub(super) async fn next_job(&self) -> Option<ThumbnailJob> {
        self.queue_rx.lock().await.recv().await
    }

    pub(super) fn enqueue(&self, job: ThumbnailJob) -> Result<()> {
        self.queue_tx
            .send(job)
            .map_err(|_| anyhow!("thumbnail job queue is closed"))
    }

    fn next_token(&self) -> JobToken {
        JobToken::new(self.next_token.fetch_add(1, Ordering::Relaxed))
    }
}
