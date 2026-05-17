use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::task::{AbortHandle, JoinHandle};
use tracing::{debug, error};

pub(super) struct VideoWorkerHandle {
    abort: AbortHandle,
    pub(super) finished: Arc<AtomicBool>,
}

pub(super) struct VideoWorkerTask {
    pub(super) device_id: String,
    pub(super) finished: Arc<AtomicBool>,
    pub(super) handle: JoinHandle<()>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct VideoWorkerExit {
    device_id: String,
    status: VideoWorkerExitStatus,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum VideoWorkerExitStatus {
    Stopped,
    Cancelled,
    Panicked(String),
    Failed(String),
}

impl VideoWorkerHandle {
    pub(super) fn new(abort: AbortHandle, finished: Arc<AtomicBool>) -> Self {
        Self { abort, finished }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    pub(super) fn abort(&self) {
        self.abort.abort();
    }
}

impl Drop for VideoWorkerHandle {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

impl VideoWorkerExit {
    pub(super) fn log(self) {
        match self.status {
            VideoWorkerExitStatus::Stopped => {
                debug!(device_id = %self.device_id, "video worker stopped");
            }
            VideoWorkerExitStatus::Cancelled => {
                debug!(device_id = %self.device_id, "video worker cancelled");
            }
            VideoWorkerExitStatus::Panicked(error) => {
                error!(
                    device_id = %self.device_id,
                    error = %error,
                    "video worker panicked"
                );
            }
            VideoWorkerExitStatus::Failed(error) => {
                error!(
                    device_id = %self.device_id,
                    error = %error,
                    "video worker failed"
                );
            }
        }
    }
}

pub(super) async fn observe_worker(task: VideoWorkerTask) -> VideoWorkerExit {
    let status = match task.handle.await {
        Ok(()) => VideoWorkerExitStatus::Stopped,
        Err(error) if error.is_cancelled() => VideoWorkerExitStatus::Cancelled,
        Err(error) if error.is_panic() => VideoWorkerExitStatus::Panicked(error.to_string()),
        Err(error) => VideoWorkerExitStatus::Failed(error.to_string()),
    };
    task.finished.store(true, Ordering::SeqCst);
    VideoWorkerExit {
        device_id: task.device_id,
        status,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use super::{observe_worker, VideoWorkerExitStatus, VideoWorkerTask};

    #[tokio::test]
    async fn observed_worker_records_normal_exit() {
        let finished = Arc::new(AtomicBool::new(false));
        let exit = observe_worker(VideoWorkerTask {
            device_id: "printer-a".to_owned(),
            finished: Arc::clone(&finished),
            handle: tokio::spawn(async {}),
        })
        .await;

        assert_eq!(exit.device_id, "printer-a");
        assert_eq!(exit.status, VideoWorkerExitStatus::Stopped);
        assert!(finished.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn observed_worker_records_cancellation() {
        let finished = Arc::new(AtomicBool::new(false));
        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        handle.abort();

        let exit = observe_worker(VideoWorkerTask {
            device_id: "printer-a".to_owned(),
            finished: Arc::clone(&finished),
            handle,
        })
        .await;

        assert_eq!(exit.status, VideoWorkerExitStatus::Cancelled);
        assert!(finished.load(Ordering::SeqCst));
    }
}
