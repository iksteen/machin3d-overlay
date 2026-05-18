use std::{collections::HashMap, time::Instant};

use crate::bambu::PrinterStatus;

use super::{cache::TaskKey, ThumbnailStatus};

#[derive(Clone, Debug)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct JobToken(u64);

#[derive(Default)]
pub(super) struct ThumbnailJobState {
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

impl ThumbnailJobState {
    pub(super) fn status(&self, device_id: &str) -> ThumbnailStatus {
        self.entries
            .get(device_id)
            .map(|entry| entry.status.clone())
            .unwrap_or_else(|| ThumbnailStatus::Missing("thumbnail is not available".to_owned()))
    }

    pub(super) fn schedule(&mut self, job: ThumbnailJob) -> JobSchedule {
        if self.is_stale(&job.device_id, job.order) {
            return JobSchedule::Unchanged;
        }
        self.remember_order(&job.device_id, job.order);
        let existing_entry_covers_task = self
            .entries
            .get_mut(&job.device_id)
            .is_some_and(|entry| entry.cover_or_refresh_order(&job.task, job.order));
        if existing_entry_covers_task {
            if let Some(device) = self.devices.get_mut(&job.device_id) {
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

        self.entries.insert(
            job.device_id.clone(),
            ThumbnailEntry {
                task: job.task.clone(),
                status: ThumbnailStatus::Loading("print thumbnail is loading".to_owned()),
                retry_after: None,
                order: job.order,
            },
        );

        if let Some(device) = self.devices.get_mut(&job.device_id) {
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

        self.devices.insert(
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

    pub(super) fn clear(&mut self, device_id: &str, order: JobOrder) {
        if self.is_stale(device_id, order) {
            return;
        }
        self.remember_order(device_id, order);
        self.devices.remove(device_id);
        self.entries.remove(device_id);
    }

    pub(super) fn start(&mut self, job: &ThumbnailJob) -> JobStart {
        let Some(device) = self.devices.get_mut(&job.device_id) else {
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

    pub(super) fn finish(
        &mut self,
        job: &ThumbnailJob,
        status: ThumbnailStatus,
        retry_after: Option<Instant>,
    ) -> JobCompletion {
        let Some(device) = self.devices.get_mut(&job.device_id) else {
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
            self.devices.remove(&job.device_id);
            if let Some(entry) = self.entries.get_mut(&job.device_id) {
                if entry.task == job.task {
                    entry.status = status;
                    entry.retry_after = retry_after;
                }
            }
            JobCompletion::Store
        }
    }

    pub(super) fn send_failed(
        &mut self,
        device_id: &str,
        task: TaskKey,
        order: JobOrder,
        message: String,
        retry_after: Instant,
    ) {
        if self.is_stale(device_id, order) {
            return;
        }
        self.remember_order(device_id, order);
        self.devices.remove(device_id);
        self.entries.insert(
            device_id.to_owned(),
            ThumbnailEntry {
                task,
                status: ThumbnailStatus::Missing(message),
                retry_after: Some(retry_after),
                order,
            },
        );
    }

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

impl ThumbnailJob {
    pub(super) fn new(
        device_id: String,
        task: TaskKey,
        report: PrinterStatus,
        order: JobOrder,
        token: JobToken,
    ) -> Self {
        Self {
            device_id,
            task,
            report,
            order,
            token,
        }
    }
}

impl JobOrder {
    pub(super) fn new(snapshot_revision: u64) -> Self {
        Self(snapshot_revision)
    }
}

impl JobToken {
    pub(super) fn new(value: u64) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        JobCompletion, JobOrder, JobSchedule, JobStart, JobToken, ThumbnailJob, ThumbnailJobState,
    };
    use crate::bambu::PrinterStatus;
    use crate::thumbnail::cache::TaskKey;
    use crate::thumbnail::{ThumbnailImage, ThumbnailStatus};

    struct TestJobs {
        state: ThumbnailJobState,
        next_token: u64,
    }

    impl TestJobs {
        fn new() -> Self {
            Self {
                state: ThumbnailJobState::default(),
                next_token: 1,
            }
        }

        fn schedule(&mut self, device_id: &str, task: &TaskKey) -> JobSchedule {
            self.schedule_revision(device_id, task, 1)
        }

        fn schedule_revision(
            &mut self,
            device_id: &str,
            task: &TaskKey,
            revision: u64,
        ) -> JobSchedule {
            self.schedule_order(device_id, task, JobOrder::new(revision))
        }

        fn schedule_order(
            &mut self,
            device_id: &str,
            task: &TaskKey,
            order: JobOrder,
        ) -> JobSchedule {
            let token = JobToken::new(self.next_token);
            self.next_token += 1;
            self.state.schedule(ThumbnailJob::new(
                device_id.to_owned(),
                task.clone(),
                PrinterStatus::default(),
                order,
                token,
            ))
        }

        fn clear(&mut self, device_id: &str, order: JobOrder) {
            self.state.clear(device_id, order);
        }

        fn start(&mut self, job: &ThumbnailJob) -> JobStart {
            self.state.start(job)
        }

        fn finish(&mut self, job: &ThumbnailJob) -> JobCompletion {
            self.state
                .finish(job, ThumbnailStatus::Missing("missing".to_owned()), None)
        }

        fn finish_ready(&mut self, job: &ThumbnailJob) -> JobCompletion {
            self.state.finish(
                job,
                ThumbnailStatus::Ready(ThumbnailImage {
                    content_type: "image/png".to_owned(),
                    bytes: Bytes::from_static(&[1, 2, 3]),
                }),
                None,
            )
        }

        fn status(&self, device_id: &str) -> ThumbnailStatus {
            self.state.status(device_id)
        }

        fn start_job(&mut self, device_id: &str, task: &TaskKey) -> ThumbnailJob {
            match self.schedule(device_id, task) {
                JobSchedule::Start(job) => *job,
                JobSchedule::Pending => panic!("expected new running job, got pending job"),
                JobSchedule::Unchanged => panic!("expected new running job, got unchanged job"),
            }
        }
    }

    #[test]
    fn jobs_keep_only_one_running_task_per_device() {
        let mut jobs = TestJobs::new();
        let old_task = TaskKey::for_test("old");
        let new_task = TaskKey::for_test("new");

        let old_job = jobs.start_job("printer-a", &old_task);
        assert!(matches!(
            jobs.schedule("printer-a", &old_task),
            JobSchedule::Unchanged
        ));
        assert!(matches!(jobs.start(&old_job), JobStart::Fetch));

        assert!(matches!(
            jobs.schedule("printer-a", &new_task),
            JobSchedule::Pending
        ));

        let next = match jobs.finish(&old_job) {
            JobCompletion::StartPending(next) => next,
            JobCompletion::Store => panic!("expected pending job, got store completion"),
            JobCompletion::Stale => panic!("expected pending job, got stale completion"),
        };
        assert_eq!(next.task, new_task);

        assert!(matches!(jobs.start(&next), JobStart::Fetch));
        assert!(matches!(jobs.finish(&next), JobCompletion::Store));
        assert!(matches!(jobs.finish(&next), JobCompletion::Stale));
    }

    #[test]
    fn scheduling_running_task_clears_obsolete_pending_job() {
        let mut jobs = TestJobs::new();
        let old_task = TaskKey::for_test("old");
        let new_task = TaskKey::for_test("new");

        let old_job = jobs.start_job("printer-a", &old_task);
        assert!(matches!(jobs.start(&old_job), JobStart::Fetch));
        assert!(matches!(
            jobs.schedule("printer-a", &new_task),
            JobSchedule::Pending
        ));

        assert!(matches!(
            jobs.schedule("printer-a", &old_task),
            JobSchedule::Pending
        ));

        assert!(matches!(jobs.finish(&old_job), JobCompletion::Store));
        assert!(matches!(jobs.finish(&old_job), JobCompletion::Stale));
    }

    #[test]
    fn cleared_job_does_not_match_new_job_for_same_task() {
        let mut jobs = TestJobs::new();
        let task = TaskKey::for_test("task");

        let old_job = jobs.start_job("printer-a", &task);
        jobs.clear("printer-a", JobOrder::new(2));
        let new_job = match jobs.schedule_revision("printer-a", &task, 3) {
            JobSchedule::Start(job) => *job,
            JobSchedule::Pending => panic!("expected new running job, got pending job"),
            JobSchedule::Unchanged => panic!("expected new running job, got unchanged job"),
        };

        assert!(matches!(jobs.start(&old_job), JobStart::Stale));
        assert!(matches!(jobs.finish(&old_job), JobCompletion::Stale));

        assert!(matches!(jobs.start(&new_job), JobStart::Fetch));
        assert!(matches!(jobs.finish(&new_job), JobCompletion::Store));
        assert!(matches!(jobs.finish(&new_job), JobCompletion::Stale));
    }

    #[test]
    fn stale_queued_job_promotes_pending_without_fetching() {
        let mut jobs = TestJobs::new();
        let old_task = TaskKey::for_test("old");
        let new_task = TaskKey::for_test("new");

        let old_job = jobs.start_job("printer-a", &old_task);
        assert!(matches!(
            jobs.schedule("printer-a", &new_task),
            JobSchedule::Pending
        ));

        let next = match jobs.start(&old_job) {
            JobStart::StartPending(next) => next,
            JobStart::Fetch => panic!("expected pending job promotion, got fetch"),
            JobStart::Stale => panic!("expected pending job promotion, got stale job"),
        };
        assert_eq!(next.task, new_task);
        assert!(matches!(jobs.finish(&old_job), JobCompletion::Stale));

        assert!(matches!(jobs.start(&next), JobStart::Fetch));
        assert!(matches!(jobs.finish(&next), JobCompletion::Store));
    }

    #[test]
    fn snapshot_revision_prevents_older_report_from_superseding_newer_task() {
        let mut jobs = TestJobs::new();
        let newer_task = TaskKey::for_test("newer");
        let older_task = TaskKey::for_test("older");

        assert!(matches!(
            jobs.schedule_revision("printer-a", &newer_task, 2),
            JobSchedule::Start(_)
        ));
        assert!(matches!(
            jobs.schedule_revision("printer-a", &older_task, 1),
            JobSchedule::Unchanged
        ));
    }

    #[test]
    fn clear_keeps_latest_order_to_reject_older_snapshots() {
        let mut jobs = TestJobs::new();
        let active_task = TaskKey::for_test("active");

        assert!(matches!(
            jobs.schedule_revision("printer-a", &active_task, 1),
            JobSchedule::Start(_)
        ));
        jobs.clear("printer-a", JobOrder::new(2));

        assert!(matches!(
            jobs.schedule_revision("printer-a", &active_task, 1),
            JobSchedule::Unchanged
        ));
        assert!(matches!(
            jobs.status("printer-a"),
            ThumbnailStatus::Missing(message) if message == "thumbnail is not available"
        ));
    }

    #[test]
    fn clear_allows_later_snapshot_for_same_device() {
        let mut jobs = TestJobs::new();
        let old_task = TaskKey::for_test("old");
        let new_task = TaskKey::for_test("new");

        assert!(matches!(
            jobs.schedule_revision("printer-a", &old_task, 1),
            JobSchedule::Start(_)
        ));
        jobs.clear("printer-a", JobOrder::new(2));

        assert!(matches!(
            jobs.schedule_revision("printer-a", &new_task, 3),
            JobSchedule::Start(_)
        ));
    }

    #[test]
    fn newer_snapshot_promotes_pending_task() {
        let mut jobs = TestJobs::new();
        let old_task = TaskKey::for_test("old");
        let new_task = TaskKey::for_test("new");

        assert!(matches!(
            jobs.schedule_revision("printer-a", &old_task, 1),
            JobSchedule::Start(_)
        ));
        assert!(matches!(
            jobs.schedule_revision("printer-a", &new_task, 2),
            JobSchedule::Pending
        ));
    }

    #[test]
    fn newer_report_for_same_task_does_not_stale_running_job() {
        let mut jobs = TestJobs::new();
        let task = TaskKey::for_test("task");

        let job = match jobs.schedule_revision("printer-a", &task, 1) {
            JobSchedule::Start(job) => *job,
            JobSchedule::Pending => panic!("expected start, got pending"),
            JobSchedule::Unchanged => panic!("expected start, got unchanged"),
        };
        assert!(matches!(jobs.start(&job), JobStart::Fetch));
        assert!(matches!(
            jobs.schedule_revision("printer-a", &task, 2),
            JobSchedule::Unchanged
        ));

        assert!(matches!(jobs.finish_ready(&job), JobCompletion::Store));
        assert!(matches!(
            jobs.status("printer-a"),
            ThumbnailStatus::Ready(image) if image.bytes.as_ref() == [1, 2, 3]
        ));
    }

    #[test]
    fn failed_running_job_replaces_loading_status() {
        let mut jobs = TestJobs::new();
        let task = TaskKey::for_test("task");

        let job = jobs.start_job("printer-a", &task);
        assert!(matches!(jobs.start(&job), JobStart::Fetch));
        assert!(matches!(
            jobs.status("printer-a"),
            ThumbnailStatus::Loading(message) if message == "print thumbnail is loading"
        ));

        assert!(matches!(
            jobs.state.finish(
                &job,
                ThumbnailStatus::Missing("worker panicked".to_owned()),
                Some(std::time::Instant::now())
            ),
            JobCompletion::Store
        ));
        assert!(matches!(
            jobs.status("printer-a"),
            ThumbnailStatus::Missing(message) if message == "worker panicked"
        ));
    }

    #[test]
    fn newer_report_for_queued_same_task_replaces_queued_fetch() {
        let mut jobs = TestJobs::new();
        let task = TaskKey::for_test("task");

        let queued = match jobs.schedule_revision("printer-a", &task, 1) {
            JobSchedule::Start(job) => *job,
            JobSchedule::Pending => panic!("expected start, got pending"),
            JobSchedule::Unchanged => panic!("expected start, got unchanged"),
        };
        assert!(matches!(
            jobs.schedule_revision("printer-a", &task, 2),
            JobSchedule::Pending
        ));

        let next = match jobs.start(&queued) {
            JobStart::StartPending(next) => next,
            JobStart::Fetch => panic!("expected pending replacement, got fetch"),
            JobStart::Stale => panic!("expected pending replacement, got stale job"),
        };
        assert_eq!(next.order, JobOrder::new(2));
    }

    #[test]
    fn reverted_task_replaces_unstarted_original_fetch() {
        let mut jobs = TestJobs::new();
        let task_a = TaskKey::for_test("task-a");
        let task_b = TaskKey::for_test("task-b");

        let queued_a = match jobs.schedule_revision("printer-a", &task_a, 1) {
            JobSchedule::Start(job) => *job,
            JobSchedule::Pending => panic!("expected start, got pending"),
            JobSchedule::Unchanged => panic!("expected start, got unchanged"),
        };
        assert!(matches!(
            jobs.schedule_revision("printer-a", &task_b, 2),
            JobSchedule::Pending
        ));
        assert!(matches!(
            jobs.schedule_revision("printer-a", &task_a, 3),
            JobSchedule::Pending
        ));

        let next = match jobs.start(&queued_a) {
            JobStart::StartPending(next) => next,
            JobStart::Fetch => panic!("expected replacement job, got fetch"),
            JobStart::Stale => panic!("expected replacement job, got stale"),
        };
        assert_eq!(next.task, task_a);
        assert_eq!(next.order, JobOrder::new(3));
    }
}
