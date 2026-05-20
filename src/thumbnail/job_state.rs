use std::{collections::HashMap, time::Instant};

use crate::{bambu::PrinterStatus, mqtt::MqttDeviceState};

use super::{trimmed, ThumbnailStatus};

/// Identifies the active print task on a device. Two thumbnail jobs with the
/// same `TaskKey` are about the same print job; jobs with different keys are
/// for distinct prints. The key combines task id, filename, task name, start
/// time, and print type so that any change to the active print invalidates
/// cached state and schedules a fresh thumbnail fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TaskKey(String);

impl TaskKey {
    pub(super) fn from_state(state: &MqttDeviceState) -> Option<Self> {
        state
            .is_active_task()
            .then(|| Self::from_report(&state.report))
            .flatten()
    }

    #[cfg(test)]
    pub(super) fn for_test(value: &str) -> Self {
        Self(value.to_owned())
    }

    fn from_report(report: &PrinterStatus) -> Option<Self> {
        let task_id = trimmed(report.task_id.as_deref());
        let filename = trimmed(report.filename.as_deref());
        let task_name = trimmed(report.task_name.as_deref());
        if task_id.is_none() && filename.is_none() && task_name.is_none() {
            return None;
        }

        Some(Self(format!(
            "{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}",
            task_id.unwrap_or_default(),
            filename.unwrap_or_default(),
            task_name.unwrap_or_default(),
            trimmed(report.start_time.as_deref()).unwrap_or_default(),
            trimmed(report.print_type.as_deref()).unwrap_or_default()
        )))
    }
}

#[derive(Clone, Debug)]
pub(super) struct ThumbnailJob {
    pub(super) device_id: String,
    pub(super) task: TaskKey,
    pub(super) report: PrinterStatus,
    pub(super) order: JobOrder,
    id: JobId,
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

/// Identifies a specific `ThumbnailJob` instance through the schedule → start
/// → finish pipeline, so a worker can confirm the active job still matches the
/// one it dequeued. Distinct from `JobOrder`: a `JobId` is a monotonic
/// per-instance handle and has no relationship to MQTT snapshot revisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct JobId(u64);

#[derive(Default)]
pub(super) struct ThumbnailJobState {
    devices: HashMap<String, DeviceThumbnailState>,
}

#[derive(Debug, Clone)]
struct CachedThumbnailStatus {
    task: TaskKey,
    status: ThumbnailStatus,
    retry_after: Option<Instant>,
    order: JobOrder,
}

#[derive(Default)]
struct DeviceThumbnailState {
    cached_status: Option<CachedThumbnailStatus>,
    // A device has at most one active fetch. A newer task waits here and is
    // promoted when the active fetch starts or finishes.
    active_job: Option<ActiveThumbnailJob>,
    // MQTT snapshots are processed by revision so older reports cannot restore
    // stale loading or cached thumbnail state after a newer clear/task change.
    latest_snapshot_order: Option<JobOrder>,
}

struct ActiveThumbnailJob {
    task: TaskKey,
    id: JobId,
    order: JobOrder,
    started: bool,
    pending_job: Option<ThumbnailJob>,
}

impl ThumbnailJobState {
    pub(super) fn status(&self, device_id: &str) -> ThumbnailStatus {
        self.devices
            .get(device_id)
            .and_then(DeviceThumbnailState::status)
            .unwrap_or_else(|| ThumbnailStatus::Missing("thumbnail is not available".to_owned()))
    }

    pub(super) fn schedule(&mut self, job: ThumbnailJob) -> Option<ThumbnailJob> {
        self.devices
            .entry(job.device_id.clone())
            .or_default()
            .schedule(job)
    }

    pub(super) fn clear(&mut self, device_id: &str, order: JobOrder) {
        self.devices
            .entry(device_id.to_owned())
            .or_default()
            .clear(order);
    }

    pub(super) fn start(&mut self, job: &ThumbnailJob) -> JobStart {
        self.devices
            .get_mut(&job.device_id)
            .map(|device| device.start(job))
            .unwrap_or(JobStart::Stale)
    }

    pub(super) fn finish(
        &mut self,
        job: &ThumbnailJob,
        status: ThumbnailStatus,
        retry_after: Option<Instant>,
    ) -> JobCompletion {
        self.devices
            .get_mut(&job.device_id)
            .map(|device| device.finish(job, status, retry_after))
            .unwrap_or(JobCompletion::Stale)
    }

    pub(super) fn mark_enqueue_failed(
        &mut self,
        device_id: &str,
        task: TaskKey,
        order: JobOrder,
        message: String,
        retry_after: Instant,
    ) {
        self.devices
            .entry(device_id.to_owned())
            .or_default()
            .mark_enqueue_failed(task, order, message, retry_after);
    }
}

impl DeviceThumbnailState {
    fn status(&self) -> Option<ThumbnailStatus> {
        self.cached_status
            .as_ref()
            .map(|entry| entry.status.clone())
    }

    fn schedule(&mut self, job: ThumbnailJob) -> Option<ThumbnailJob> {
        if self.is_older_than_latest_snapshot(job.order) {
            return None;
        }
        self.remember_snapshot_order(job.order);

        if self.cached_status_covers_task(&job) {
            if let Some(active_job) = &mut self.active_job {
                if active_job.task == job.task {
                    if !active_job.started && job.order > active_job.order {
                        active_job.order = active_job.order.max(job.order);
                        active_job.pending_job = Some(job);
                    }
                    return None;
                }
                if let Some(pending) = active_job
                    .pending_job
                    .as_mut()
                    .filter(|pending| pending.task == job.task)
                {
                    if job.order > pending.order {
                        active_job.order = active_job.order.max(job.order);
                        *pending = job;
                    }
                    return None;
                }
            }
            return None;
        }

        self.cached_status = Some(CachedThumbnailStatus::loading(job.task.clone(), job.order));

        let Some(active_job) = &mut self.active_job else {
            self.active_job = Some(ActiveThumbnailJob::new(&job));
            return Some(job);
        };

        let job_is_newer_than_active = job.order > active_job.order;
        active_job.order = active_job.order.max(job.order);
        if active_job.task == job.task {
            if !active_job.started && job_is_newer_than_active {
                active_job.pending_job = Some(job);
                return None;
            }
            if active_job.pending_job.is_some() {
                active_job.pending_job = None;
                return None;
            }
            return None;
        }
        if active_job
            .pending_job
            .as_ref()
            .is_some_and(|pending| pending.task == job.task)
        {
            active_job.pending_job = Some(job);
            return None;
        }

        active_job.pending_job = Some(job);
        None
    }

    fn clear(&mut self, order: JobOrder) {
        if self.is_older_than_latest_snapshot(order) {
            return;
        }
        self.remember_snapshot_order(order);
        self.active_job = None;
        self.cached_status = None;
    }

    fn start(&mut self, job: &ThumbnailJob) -> JobStart {
        let Some(active_job) = &mut self.active_job else {
            return JobStart::Stale;
        };
        active_job.start(job)
    }

    fn finish(
        &mut self,
        job: &ThumbnailJob,
        status: ThumbnailStatus,
        retry_after: Option<Instant>,
    ) -> JobCompletion {
        let Some(active_job) = &mut self.active_job else {
            return JobCompletion::Stale;
        };
        match active_job.finish(job) {
            ActiveJobCompletion::Store => {
                self.active_job = None;
                if let Some(entry) = self
                    .cached_status
                    .as_mut()
                    .filter(|entry| entry.task == job.task)
                {
                    entry.status = status;
                    entry.retry_after = retry_after;
                }
                JobCompletion::Store
            }
            ActiveJobCompletion::StartPending(job) => JobCompletion::StartPending(job),
            ActiveJobCompletion::Stale => JobCompletion::Stale,
        }
    }

    fn mark_enqueue_failed(
        &mut self,
        task: TaskKey,
        order: JobOrder,
        message: String,
        retry_after: Instant,
    ) {
        if self.is_older_than_latest_snapshot(order) {
            return;
        }
        self.remember_snapshot_order(order);
        self.active_job = None;
        self.cached_status = Some(CachedThumbnailStatus {
            task,
            status: ThumbnailStatus::Unavailable(message),
            retry_after: Some(retry_after),
            order,
        });
    }

    fn cached_status_covers_task(&mut self, job: &ThumbnailJob) -> bool {
        self.cached_status
            .as_mut()
            .is_some_and(|entry| entry.cover_or_refresh_order(&job.task, job.order))
    }

    fn is_older_than_latest_snapshot(&self, order: JobOrder) -> bool {
        self.latest_snapshot_order
            .is_some_and(|latest| order < latest)
    }

    fn remember_snapshot_order(&mut self, order: JobOrder) {
        self.latest_snapshot_order = Some(
            self.latest_snapshot_order
                .map_or(order, |latest| latest.max(order)),
        );
    }
}

enum ActiveJobCompletion {
    Store,
    StartPending(Box<ThumbnailJob>),
    Stale,
}

impl ActiveThumbnailJob {
    fn new(job: &ThumbnailJob) -> Self {
        Self {
            task: job.task.clone(),
            id: job.id,
            order: job.order,
            started: false,
            pending_job: None,
        }
    }

    fn start(&mut self, job: &ThumbnailJob) -> JobStart {
        if self.task != job.task || self.id != job.id {
            return JobStart::Stale;
        }
        if self.started {
            return JobStart::Stale;
        }

        if let Some(pending) = self.pending_job.take() {
            self.replace_with(&pending);
            JobStart::StartPending(Box::new(pending))
        } else {
            self.started = true;
            JobStart::Fetch
        }
    }

    fn finish(&mut self, job: &ThumbnailJob) -> ActiveJobCompletion {
        if self.task != job.task || self.id != job.id {
            return ActiveJobCompletion::Stale;
        }

        if let Some(pending) = self.pending_job.take() {
            self.replace_with(&pending);
            ActiveJobCompletion::StartPending(Box::new(pending))
        } else {
            ActiveJobCompletion::Store
        }
    }

    fn replace_with(&mut self, job: &ThumbnailJob) {
        self.task = job.task.clone();
        self.id = job.id;
        self.order = job.order;
        self.started = false;
    }
}

impl CachedThumbnailStatus {
    fn loading(task: TaskKey, order: JobOrder) -> Self {
        Self {
            task,
            status: ThumbnailStatus::Loading("print thumbnail is loading".to_owned()),
            retry_after: None,
            order,
        }
    }

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
            ThumbnailStatus::Loading(_)
            | ThumbnailStatus::Missing(_)
            | ThumbnailStatus::Unavailable(_) => self
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
        id: JobId,
    ) -> Self {
        Self {
            device_id,
            task,
            report,
            order,
            id,
        }
    }
}

impl JobOrder {
    pub(super) fn new(snapshot_revision: u64) -> Self {
        Self(snapshot_revision)
    }
}

impl JobId {
    pub(super) fn new(value: u64) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{JobCompletion, JobId, JobOrder, JobStart, TaskKey, ThumbnailJob, ThumbnailJobState};
    use crate::bambu::PrinterStatus;
    use crate::{
        live::{ConnectionStatus, DeviceConnection},
        mqtt::MqttDeviceState,
    };
    use crate::thumbnail::{ThumbnailImage, ThumbnailStatus};

    #[test]
    fn task_key_tracks_the_active_print_identity() {
        let report = PrinterStatus {
            task_id: Some("task-1".to_owned()),
            filename: Some("cube.3mf".to_owned()),
            task_name: Some("Cube".to_owned()),
            start_time: Some("2026-01-01".to_owned()),
            ..PrinterStatus::default()
        };

        assert!(TaskKey::from_report(&report).is_some());
        assert_eq!(TaskKey::from_report(&PrinterStatus::default()), None);
    }

    #[test]
    fn task_key_ignores_inactive_live_state() {
        let state = MqttDeviceState::from_report(PrinterStatus {
            status: Some("FINISH".to_owned()),
            task_id: Some("task-1".to_owned()),
            filename: Some("cube.3mf".to_owned()),
            task_name: Some("Cube".to_owned()),
            ..PrinterStatus::default()
        });

        assert_eq!(TaskKey::from_state(&state), None);
    }

    #[test]
    fn task_key_ignores_stale_live_state() {
        let state = MqttDeviceState::from_snapshot(
            PrinterStatus {
                status: Some("RUNNING".to_owned()),
                task_id: Some("task-1".to_owned()),
                filename: Some("cube.3mf".to_owned()),
                task_name: Some("Cube".to_owned()),
                ..PrinterStatus::default()
            },
            None,
            DeviceConnection {
                key: Some("printer-a".to_owned()),
                status: ConnectionStatus::Disconnected,
                error: Some("disconnected".to_owned()),
            },
        );

        assert_eq!(TaskKey::from_state(&state), None);
    }

    struct TestJobs {
        state: ThumbnailJobState,
        next_id: u64,
    }

    impl TestJobs {
        fn new() -> Self {
            Self {
                state: ThumbnailJobState::default(),
                next_id: 1,
            }
        }

        fn schedule(&mut self, device_id: &str, task: &TaskKey) -> Option<ThumbnailJob> {
            self.schedule_revision(device_id, task, 1)
        }

        fn schedule_revision(
            &mut self,
            device_id: &str,
            task: &TaskKey,
            revision: u64,
        ) -> Option<ThumbnailJob> {
            self.schedule_order(device_id, task, JobOrder::new(revision))
        }

        fn schedule_order(
            &mut self,
            device_id: &str,
            task: &TaskKey,
            order: JobOrder,
        ) -> Option<ThumbnailJob> {
            let id = JobId::new(self.next_id);
            self.next_id += 1;
            self.state.schedule(ThumbnailJob::new(
                device_id.to_owned(),
                task.clone(),
                PrinterStatus::default(),
                order,
                id,
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
            self.schedule(device_id, task)
                .expect("expected new running job")
        }
    }

    #[test]
    fn jobs_keep_only_one_running_task_per_device() {
        let mut jobs = TestJobs::new();
        let old_task = TaskKey::for_test("old");
        let new_task = TaskKey::for_test("new");

        let old_job = jobs.start_job("printer-a", &old_task);
        assert!(jobs.schedule("printer-a", &old_task).is_none());
        assert!(matches!(jobs.start(&old_job), JobStart::Fetch));

        assert!(jobs.schedule("printer-a", &new_task).is_none());

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
        assert!(jobs.schedule("printer-a", &new_task).is_none());

        assert!(jobs.schedule("printer-a", &old_task).is_none());

        assert!(matches!(jobs.finish(&old_job), JobCompletion::Store));
        assert!(matches!(jobs.finish(&old_job), JobCompletion::Stale));
    }

    #[test]
    fn cleared_job_does_not_match_new_job_for_same_task() {
        let mut jobs = TestJobs::new();
        let task = TaskKey::for_test("task");

        let old_job = jobs.start_job("printer-a", &task);
        jobs.clear("printer-a", JobOrder::new(2));
        let new_job = jobs
            .schedule_revision("printer-a", &task, 3)
            .expect("expected new running job");

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
        assert!(jobs.schedule("printer-a", &new_task).is_none());

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

        assert!(jobs
            .schedule_revision("printer-a", &newer_task, 2)
            .is_some());
        assert!(jobs
            .schedule_revision("printer-a", &older_task, 1)
            .is_none());
    }

    #[test]
    fn clear_rejects_older_snapshots() {
        let mut jobs = TestJobs::new();
        let active_task = TaskKey::for_test("active");

        assert!(jobs
            .schedule_revision("printer-a", &active_task, 1)
            .is_some());
        jobs.clear("printer-a", JobOrder::new(2));

        assert!(jobs
            .schedule_revision("printer-a", &active_task, 1)
            .is_none());
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

        assert!(jobs.schedule_revision("printer-a", &old_task, 1).is_some());
        jobs.clear("printer-a", JobOrder::new(2));

        assert!(jobs.schedule_revision("printer-a", &new_task, 3).is_some());
    }

    #[test]
    fn newer_snapshot_queues_pending_task() {
        let mut jobs = TestJobs::new();
        let old_task = TaskKey::for_test("old");
        let new_task = TaskKey::for_test("new");

        assert!(jobs.schedule_revision("printer-a", &old_task, 1).is_some());
        assert!(jobs.schedule_revision("printer-a", &new_task, 2).is_none());
    }

    #[test]
    fn newer_report_for_same_task_does_not_stale_running_job() {
        let mut jobs = TestJobs::new();
        let task = TaskKey::for_test("task");

        let job = jobs
            .schedule_revision("printer-a", &task, 1)
            .expect("expected start");
        assert!(matches!(jobs.start(&job), JobStart::Fetch));
        assert!(jobs.schedule_revision("printer-a", &task, 2).is_none());

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

        let queued = jobs
            .schedule_revision("printer-a", &task, 1)
            .expect("expected start");
        assert!(jobs.schedule_revision("printer-a", &task, 2).is_none());

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

        let queued_a = jobs
            .schedule_revision("printer-a", &task_a, 1)
            .expect("expected start");
        assert!(jobs.schedule_revision("printer-a", &task_b, 2).is_none());
        assert!(jobs.schedule_revision("printer-a", &task_a, 3).is_none());

        let next = match jobs.start(&queued_a) {
            JobStart::StartPending(next) => next,
            JobStart::Fetch => panic!("expected replacement job, got fetch"),
            JobStart::Stale => panic!("expected replacement job, got stale"),
        };
        assert_eq!(next.task, task_a);
        assert_eq!(next.order, JobOrder::new(3));
    }
}
