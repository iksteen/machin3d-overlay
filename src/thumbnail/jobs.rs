use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use anyhow::{anyhow, Result};
use tokio::sync::{mpsc, Mutex};
use tracing::debug;

use crate::bambu::PrinterStatus;

use super::{cache::TaskKey, ThumbnailStatus};

pub(super) struct ThumbnailJobs {
    sender: mpsc::UnboundedSender<ThumbnailJob>,
    pub(super) receiver: Mutex<mpsc::UnboundedReceiver<ThumbnailJob>>,
    state: Mutex<ThumbnailJobState>,
    next_token: AtomicU64,
}

pub(super) struct ThumbnailJob {
    pub(super) device_id: String,
    pub(super) task: TaskKey,
    pub(super) report: PrinterStatus,
    pub(super) order: JobOrder,
    token: JobToken,
}

pub(super) enum JobSchedule {
    Start(Box<ThumbnailJob>),
    Pending,
    Unchanged,
}

pub(super) enum JobStart {
    Fetch,
    StartPending(Box<ThumbnailJob>),
    Stale,
}

pub(super) enum JobCompletion {
    Store,
    StartPending(Box<ThumbnailJob>),
    Stale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct JobOrder(u64);

#[derive(Default)]
struct ThumbnailJobState {
    entries: HashMap<String, ThumbnailEntry>,
    devices: HashMap<String, DeviceJobState>,
    latest_orders: HashMap<String, JobOrder>,
}

#[derive(Debug, Clone)]
struct ThumbnailEntry {
    task: TaskKey,
    status: ThumbnailStatus,
    retry_after: Option<Instant>,
    order: JobOrder,
}

struct DeviceJobState {
    running: TaskKey,
    token: JobToken,
    order: JobOrder,
    started: bool,
    pending: Option<ThumbnailJob>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct JobToken(u64);

impl ThumbnailJobs {
    pub(super) fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            sender,
            receiver: Mutex::new(receiver),
            state: Mutex::new(ThumbnailJobState::default()),
            next_token: AtomicU64::new(1),
        }
    }

    pub(super) async fn status(&self, device_id: &str) -> ThumbnailStatus {
        let state = self.state.lock().await;
        match state.entries.get(device_id).map(|entry| &entry.status) {
            Some(status @ ThumbnailStatus::Ready(_)) => status.clone(),
            Some(status @ ThumbnailStatus::Loading(error)) => {
                debug!(device_id, error, "thumbnail is loading");
                status.clone()
            }
            Some(status @ ThumbnailStatus::Missing(error)) => {
                debug!(device_id, error, "thumbnail is unavailable");
                status.clone()
            }
            None => ThumbnailStatus::Missing("thumbnail is not available".to_owned()),
        }
    }

    pub(super) async fn schedule(
        &self,
        device_id: String,
        task: TaskKey,
        report: PrinterStatus,
        order: JobOrder,
    ) -> JobSchedule {
        let job = ThumbnailJob {
            device_id,
            task,
            report,
            order,
            token: self.next_token(),
        };
        let mut state = self.state.lock().await;
        if state.is_stale(&job.device_id, job.order) {
            return JobSchedule::Unchanged;
        }
        state.remember_order(&job.device_id, job.order);
        let existing_entry_covers_task = state
            .entries
            .get_mut(&job.device_id)
            .is_some_and(|entry| entry.cover_or_refresh_order(&job.task, job.order));
        if existing_entry_covers_task {
            if let Some(device) = state.devices.get_mut(&job.device_id) {
                if device.running == job.task {
                    if !device.started && job.order > device.order {
                        device.order = device.order.max(job.order);
                        device.pending = Some(job);
                        return JobSchedule::Pending;
                    }
                    return JobSchedule::Unchanged;
                }
                if let Some(pending) = device
                    .pending
                    .as_mut()
                    .filter(|pending| pending.task == job.task)
                {
                    if job.order > pending.order {
                        device.order = device.order.max(job.order);
                        *pending = job;
                    }
                    return JobSchedule::Unchanged;
                }
            }
            return JobSchedule::Unchanged;
        }

        state.entries.insert(
            job.device_id.clone(),
            ThumbnailEntry {
                task: job.task.clone(),
                status: ThumbnailStatus::Loading("print thumbnail is loading".to_owned()),
                retry_after: None,
                order: job.order,
            },
        );

        if let Some(device) = state.devices.get_mut(&job.device_id) {
            let job_is_newer_than_running = job.order > device.order;
            device.order = device.order.max(job.order);
            if device.running == job.task {
                if !device.started && job_is_newer_than_running {
                    device.pending = Some(job);
                    return JobSchedule::Pending;
                }
                if device.pending.is_some() {
                    device.pending = None;
                    return JobSchedule::Pending;
                }
                return JobSchedule::Unchanged;
            }
            if device
                .pending
                .as_ref()
                .is_some_and(|pending| pending.task == job.task)
            {
                device.pending = Some(job);
                return JobSchedule::Unchanged;
            }

            device.pending = Some(job);
            return JobSchedule::Pending;
        }

        state.devices.insert(
            job.device_id.clone(),
            DeviceJobState {
                running: job.task.clone(),
                token: job.token,
                order: job.order,
                started: false,
                pending: None,
            },
        );
        JobSchedule::Start(Box::new(job))
    }

    pub(super) async fn clear(&self, device_id: &str, order: JobOrder) {
        let mut state = self.state.lock().await;
        if state.is_stale(device_id, order) {
            return;
        }
        state.remember_order(device_id, order);
        state.devices.remove(device_id);
        state.entries.remove(device_id);
    }

    pub(super) async fn start(&self, job: &ThumbnailJob) -> JobStart {
        let mut state = self.state.lock().await;
        let Some(device) = state.devices.get_mut(&job.device_id) else {
            return JobStart::Stale;
        };
        if device.running != job.task || device.token != job.token {
            return JobStart::Stale;
        }
        if device.started {
            return JobStart::Stale;
        }

        if let Some(pending) = device.pending.take() {
            device.running = pending.task.clone();
            device.token = pending.token;
            device.order = pending.order;
            device.started = false;
            JobStart::StartPending(Box::new(pending))
        } else {
            device.started = true;
            JobStart::Fetch
        }
    }

    pub(super) async fn finish(
        &self,
        job: &ThumbnailJob,
        status: ThumbnailStatus,
        retry_after: Option<Instant>,
    ) -> JobCompletion {
        let mut state = self.state.lock().await;
        let Some(device) = state.devices.get_mut(&job.device_id) else {
            return JobCompletion::Stale;
        };
        if device.running != job.task || device.token != job.token {
            return JobCompletion::Stale;
        }

        if let Some(pending) = device.pending.take() {
            device.running = pending.task.clone();
            device.token = pending.token;
            device.order = pending.order;
            device.started = false;
            JobCompletion::StartPending(Box::new(pending))
        } else {
            state.devices.remove(&job.device_id);
            if let Some(entry) = state.entries.get_mut(&job.device_id) {
                if entry.task == job.task {
                    entry.status = status;
                    entry.retry_after = retry_after;
                }
            }
            JobCompletion::Store
        }
    }

    pub(super) async fn send_failed(
        &self,
        device_id: &str,
        task: TaskKey,
        order: JobOrder,
        message: String,
        retry_after: Instant,
    ) {
        let mut state = self.state.lock().await;
        if state.is_stale(device_id, order) {
            return;
        }
        state.remember_order(device_id, order);
        state.devices.remove(device_id);
        state.entries.insert(
            device_id.to_owned(),
            ThumbnailEntry {
                task,
                status: ThumbnailStatus::Missing(message),
                retry_after: Some(retry_after),
                order,
            },
        );
    }

    pub(super) fn send(&self, job: ThumbnailJob) -> Result<()> {
        self.sender
            .send(job)
            .map_err(|_| anyhow!("thumbnail worker queue is closed"))
    }

    fn next_token(&self) -> JobToken {
        JobToken(self.next_token.fetch_add(1, Ordering::Relaxed))
    }
}

impl ThumbnailJobState {
    fn is_stale(&self, device_id: &str, order: JobOrder) -> bool {
        self.latest_orders
            .get(device_id)
            .copied()
            .is_some_and(|latest| order < latest)
    }

    fn remember_order(&mut self, device_id: &str, order: JobOrder) {
        self.latest_orders
            .entry(device_id.to_owned())
            .and_modify(|latest| *latest = (*latest).max(order))
            .or_insert(order);
    }
}

impl ThumbnailEntry {
    fn cover_or_refresh_order(&mut self, task: &TaskKey, order: JobOrder) -> bool {
        if !self.covers(task, Instant::now()) {
            return false;
        }
        self.order = self.order.max(order);
        true
    }

    fn covers(&self, task: &TaskKey, now: Instant) -> bool {
        self.task == *task && self.blocks_refresh(now)
    }

    fn blocks_refresh(&self, now: Instant) -> bool {
        match self.status {
            ThumbnailStatus::Ready(_) => true,
            ThumbnailStatus::Loading(_) if self.retry_after.is_none() => true,
            ThumbnailStatus::Loading(_) | ThumbnailStatus::Missing(_) => self
                .retry_after
                .is_some_and(|retry_after| retry_after > now),
        }
    }
}

impl JobOrder {
    pub(super) fn new(snapshot_revision: u64) -> Self {
        Self(snapshot_revision)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{JobCompletion, JobOrder, JobSchedule, JobStart, ThumbnailJob, ThumbnailJobs};
    use crate::bambu::PrinterStatus;
    use crate::thumbnail::cache::TaskKey;
    use crate::thumbnail::{ThumbnailImage, ThumbnailStatus};

    async fn schedule(jobs: &ThumbnailJobs, device_id: &str, task: &TaskKey) -> JobSchedule {
        schedule_revision(jobs, device_id, task, 1).await
    }

    async fn schedule_revision(
        jobs: &ThumbnailJobs,
        device_id: &str,
        task: &TaskKey,
        revision: u64,
    ) -> JobSchedule {
        schedule_order(jobs, device_id, task, JobOrder::new(revision)).await
    }

    async fn schedule_order(
        jobs: &ThumbnailJobs,
        device_id: &str,
        task: &TaskKey,
        order: JobOrder,
    ) -> JobSchedule {
        jobs.schedule(
            device_id.to_owned(),
            task.clone(),
            PrinterStatus::default(),
            order,
        )
        .await
    }

    async fn start_job(jobs: &ThumbnailJobs, device_id: &str, task: &TaskKey) -> ThumbnailJob {
        match schedule(jobs, device_id, task).await {
            JobSchedule::Start(job) => *job,
            JobSchedule::Pending => panic!("expected new running job, got pending job"),
            JobSchedule::Unchanged => panic!("expected new running job, got unchanged job"),
        }
    }

    async fn finish(jobs: &ThumbnailJobs, job: &ThumbnailJob) -> JobCompletion {
        jobs.finish(job, ThumbnailStatus::Missing("missing".to_owned()), None)
            .await
    }

    async fn finish_ready(jobs: &ThumbnailJobs, job: &ThumbnailJob) -> JobCompletion {
        jobs.finish(
            job,
            ThumbnailStatus::Ready(ThumbnailImage {
                content_type: "image/png".to_owned(),
                bytes: Bytes::from_static(&[1, 2, 3]),
            }),
            None,
        )
        .await
    }

    #[tokio::test]
    async fn jobs_keep_only_one_running_task_per_device() {
        let jobs = ThumbnailJobs::new();
        let old_task = TaskKey::for_test("old");
        let new_task = TaskKey::for_test("new");

        let old_job = start_job(&jobs, "printer-a", &old_task).await;
        assert!(matches!(
            schedule(&jobs, "printer-a", &old_task).await,
            JobSchedule::Unchanged
        ));
        assert!(matches!(jobs.start(&old_job).await, JobStart::Fetch));

        assert!(matches!(
            schedule(&jobs, "printer-a", &new_task).await,
            JobSchedule::Pending
        ));

        let next = match finish(&jobs, &old_job).await {
            JobCompletion::StartPending(next) => next,
            JobCompletion::Store => panic!("expected pending job, got store completion"),
            JobCompletion::Stale => panic!("expected pending job, got stale completion"),
        };
        assert_eq!(next.task, new_task);

        assert!(matches!(jobs.start(&next).await, JobStart::Fetch));
        assert!(matches!(finish(&jobs, &next).await, JobCompletion::Store));
        assert!(matches!(finish(&jobs, &next).await, JobCompletion::Stale));
    }

    #[tokio::test]
    async fn scheduling_running_task_clears_obsolete_pending_job() {
        let jobs = ThumbnailJobs::new();
        let old_task = TaskKey::for_test("old");
        let new_task = TaskKey::for_test("new");

        let old_job = start_job(&jobs, "printer-a", &old_task).await;
        assert!(matches!(jobs.start(&old_job).await, JobStart::Fetch));
        assert!(matches!(
            schedule(&jobs, "printer-a", &new_task).await,
            JobSchedule::Pending
        ));

        assert!(matches!(
            schedule(&jobs, "printer-a", &old_task).await,
            JobSchedule::Pending
        ));

        assert!(matches!(
            finish(&jobs, &old_job).await,
            JobCompletion::Store
        ));
        assert!(matches!(
            finish(&jobs, &old_job).await,
            JobCompletion::Stale
        ));
    }

    #[tokio::test]
    async fn cleared_job_does_not_match_new_job_for_same_task() {
        let jobs = ThumbnailJobs::new();
        let task = TaskKey::for_test("task");

        let old_job = start_job(&jobs, "printer-a", &task).await;
        jobs.clear("printer-a", JobOrder::new(2)).await;
        let new_job = match schedule_revision(&jobs, "printer-a", &task, 3).await {
            JobSchedule::Start(job) => *job,
            JobSchedule::Pending => panic!("expected new running job, got pending job"),
            JobSchedule::Unchanged => panic!("expected new running job, got unchanged job"),
        };

        assert!(matches!(jobs.start(&old_job).await, JobStart::Stale));
        assert!(matches!(
            finish(&jobs, &old_job).await,
            JobCompletion::Stale
        ));

        assert!(matches!(jobs.start(&new_job).await, JobStart::Fetch));
        assert!(matches!(
            finish(&jobs, &new_job).await,
            JobCompletion::Store
        ));
        assert!(matches!(
            finish(&jobs, &new_job).await,
            JobCompletion::Stale
        ));
    }

    #[tokio::test]
    async fn stale_queued_job_promotes_pending_without_fetching() {
        let jobs = ThumbnailJobs::new();
        let old_task = TaskKey::for_test("old");
        let new_task = TaskKey::for_test("new");

        let old_job = start_job(&jobs, "printer-a", &old_task).await;
        assert!(matches!(
            schedule(&jobs, "printer-a", &new_task).await,
            JobSchedule::Pending
        ));

        let next = match jobs.start(&old_job).await {
            JobStart::StartPending(next) => next,
            JobStart::Fetch => panic!("expected pending job promotion, got fetch"),
            JobStart::Stale => panic!("expected pending job promotion, got stale job"),
        };
        assert_eq!(next.task, new_task);
        assert!(matches!(
            finish(&jobs, &old_job).await,
            JobCompletion::Stale
        ));

        assert!(matches!(jobs.start(&next).await, JobStart::Fetch));
        assert!(matches!(finish(&jobs, &next).await, JobCompletion::Store));
    }

    #[tokio::test]
    async fn snapshot_revision_prevents_older_report_from_superseding_newer_task() {
        let jobs = ThumbnailJobs::new();
        let newer_task = TaskKey::for_test("newer");
        let older_task = TaskKey::for_test("older");

        assert!(matches!(
            schedule_revision(&jobs, "printer-a", &newer_task, 2).await,
            JobSchedule::Start(_)
        ));
        assert!(matches!(
            schedule_revision(&jobs, "printer-a", &older_task, 1).await,
            JobSchedule::Unchanged
        ));
    }

    #[tokio::test]
    async fn clear_keeps_latest_order_to_reject_older_snapshots() {
        let jobs = ThumbnailJobs::new();
        let active_task = TaskKey::for_test("active");

        assert!(matches!(
            schedule_revision(&jobs, "printer-a", &active_task, 1).await,
            JobSchedule::Start(_)
        ));
        jobs.clear("printer-a", JobOrder::new(2)).await;

        assert!(matches!(
            schedule_revision(&jobs, "printer-a", &active_task, 1).await,
            JobSchedule::Unchanged
        ));
        assert!(matches!(
            jobs.status("printer-a").await,
            ThumbnailStatus::Missing(message) if message == "thumbnail is not available"
        ));
    }

    #[tokio::test]
    async fn clear_allows_later_snapshot_for_same_device() {
        let jobs = ThumbnailJobs::new();
        let old_task = TaskKey::for_test("old");
        let new_task = TaskKey::for_test("new");

        assert!(matches!(
            schedule_revision(&jobs, "printer-a", &old_task, 1).await,
            JobSchedule::Start(_)
        ));
        jobs.clear("printer-a", JobOrder::new(2)).await;

        assert!(matches!(
            schedule_revision(&jobs, "printer-a", &new_task, 3).await,
            JobSchedule::Start(_)
        ));
    }

    #[tokio::test]
    async fn newer_snapshot_promotes_pending_task() {
        let jobs = ThumbnailJobs::new();
        let old_task = TaskKey::for_test("old");
        let new_task = TaskKey::for_test("new");

        assert!(matches!(
            schedule_revision(&jobs, "printer-a", &old_task, 1).await,
            JobSchedule::Start(_)
        ));
        assert!(matches!(
            schedule_revision(&jobs, "printer-a", &new_task, 2).await,
            JobSchedule::Pending
        ));
    }

    #[tokio::test]
    async fn newer_report_for_same_task_does_not_stale_running_job() {
        let jobs = ThumbnailJobs::new();
        let task = TaskKey::for_test("task");

        let job = match schedule_revision(&jobs, "printer-a", &task, 1).await {
            JobSchedule::Start(job) => *job,
            JobSchedule::Pending => panic!("expected start, got pending"),
            JobSchedule::Unchanged => panic!("expected start, got unchanged"),
        };
        assert!(matches!(jobs.start(&job).await, JobStart::Fetch));
        assert!(matches!(
            schedule_revision(&jobs, "printer-a", &task, 2).await,
            JobSchedule::Unchanged
        ));

        assert!(matches!(
            finish_ready(&jobs, &job).await,
            JobCompletion::Store
        ));
        assert!(matches!(
            jobs.status("printer-a").await,
            ThumbnailStatus::Ready(image) if image.bytes.as_ref() == [1, 2, 3]
        ));
    }

    #[tokio::test]
    async fn newer_report_for_queued_same_task_replaces_queued_fetch() {
        let jobs = ThumbnailJobs::new();
        let task = TaskKey::for_test("task");

        let queued = match schedule_revision(&jobs, "printer-a", &task, 1).await {
            JobSchedule::Start(job) => *job,
            JobSchedule::Pending => panic!("expected start, got pending"),
            JobSchedule::Unchanged => panic!("expected start, got unchanged"),
        };
        assert!(matches!(
            schedule_revision(&jobs, "printer-a", &task, 2).await,
            JobSchedule::Pending
        ));

        let next = match jobs.start(&queued).await {
            JobStart::StartPending(next) => next,
            JobStart::Fetch => panic!("expected pending replacement, got fetch"),
            JobStart::Stale => panic!("expected pending replacement, got stale job"),
        };
        assert_eq!(next.order, JobOrder::new(2));
    }

    #[tokio::test]
    async fn reverted_task_replaces_unstarted_original_fetch() {
        let jobs = ThumbnailJobs::new();
        let task_a = TaskKey::for_test("task-a");
        let task_b = TaskKey::for_test("task-b");

        let queued_a = match schedule_revision(&jobs, "printer-a", &task_a, 1).await {
            JobSchedule::Start(job) => *job,
            JobSchedule::Pending => panic!("expected start, got pending"),
            JobSchedule::Unchanged => panic!("expected start, got unchanged"),
        };
        assert!(matches!(
            schedule_revision(&jobs, "printer-a", &task_b, 2).await,
            JobSchedule::Pending
        ));
        assert!(matches!(
            schedule_revision(&jobs, "printer-a", &task_a, 3).await,
            JobSchedule::Pending
        ));

        let next = match jobs.start(&queued_a).await {
            JobStart::StartPending(next) => next,
            JobStart::Fetch => panic!("expected replacement job, got fetch"),
            JobStart::Stale => panic!("expected replacement job, got stale"),
        };
        assert_eq!(next.task, task_a);
        assert_eq!(next.order, JobOrder::new(3));
    }
}
