use std::{future::Future, time::Duration};

use anyhow::{anyhow, Result};
use tokio::{
    sync::watch,
    task::{AbortHandle, JoinSet},
};
use tracing::{info, warn};

pub(crate) struct ServiceTasks {
    aborts: Vec<AbortHandle>,
    monitors: JoinSet<TaskExit>,
}

#[derive(Clone)]
pub(crate) struct Shutdown {
    sender: watch::Sender<bool>,
}

pub(crate) struct ShutdownReceiver {
    receiver: watch::Receiver<bool>,
}

struct TaskExit {
    name: String,
    status: TaskExitStatus,
}

enum TaskExitStatus {
    Finished,
    Cancelled,
    Panicked(String),
    Failed(String),
}

impl ServiceTasks {
    pub(crate) fn new() -> Self {
        Self {
            aborts: Vec::new(),
            monitors: JoinSet::new(),
        }
    }

    pub(crate) fn spawn<F>(&mut self, name: impl Into<String>, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let name = name.into();
        let handle = tokio::spawn(future);
        self.aborts.push(handle.abort_handle());
        self.monitors.spawn(async move {
            let status = match handle.await {
                Ok(()) => TaskExitStatus::Finished,
                Err(error) if error.is_cancelled() => TaskExitStatus::Cancelled,
                Err(error) if error.is_panic() => TaskExitStatus::Panicked(error.to_string()),
                Err(error) => TaskExitStatus::Failed(error.to_string()),
            };
            TaskExit { name, status }
        });
    }

    pub(crate) async fn wait_for_failure(&mut self) -> Result<()> {
        match self.monitors.join_next().await {
            Some(Ok(exit)) => Err(exit.unexpected_error()),
            Some(Err(error)) => Err(anyhow!("background task monitor failed: {error}")),
            None => std::future::pending::<Result<()>>().await,
        }
    }

    pub(crate) async fn shutdown(&mut self, grace: Duration) -> Result<()> {
        match tokio::time::timeout(grace, self.wait_for_completion()).await {
            Ok(result) => result,
            Err(_) => {
                warn!(
                    timeout_ms = grace.as_millis(),
                    "background tasks did not stop during shutdown grace period; aborting"
                );
                self.abort_all();
                match tokio::time::timeout(grace, self.wait_for_completion()).await {
                    Ok(result) => result,
                    Err(_) => {
                        warn!(
                            timeout_ms = grace.as_millis(),
                            "background task monitors did not finish after abort"
                        );
                        Ok(())
                    }
                }
            }
        }
    }

    async fn wait_for_completion(&mut self) -> Result<()> {
        while let Some(result) = self.monitors.join_next().await {
            match result {
                Ok(exit) => exit.shutdown_result()?,
                Err(error) => return Err(anyhow!("background task monitor failed: {error}")),
            }
        }
        self.aborts.clear();
        Ok(())
    }

    fn abort_all(&mut self) {
        for abort in &self.aborts {
            abort.abort();
        }
        self.aborts.clear();
    }
}

impl Drop for ServiceTasks {
    fn drop(&mut self) {
        self.abort_all();
    }
}

impl TaskExit {
    fn unexpected_error(self) -> anyhow::Error {
        match self.status {
            TaskExitStatus::Finished => {
                anyhow!("background task `{}` exited unexpectedly", self.name)
            }
            TaskExitStatus::Cancelled => {
                anyhow!("background task `{}` was cancelled unexpectedly", self.name)
            }
            TaskExitStatus::Panicked(error) => {
                anyhow!("background task `{}` panicked: {error}", self.name)
            }
            TaskExitStatus::Failed(error) => {
                anyhow!("background task `{}` failed: {error}", self.name)
            }
        }
    }

    fn shutdown_result(self) -> Result<()> {
        match self.status {
            TaskExitStatus::Finished | TaskExitStatus::Cancelled => Ok(()),
            TaskExitStatus::Panicked(error) => {
                Err(anyhow!("background task `{}` panicked: {error}", self.name))
            }
            TaskExitStatus::Failed(error) => {
                Err(anyhow!("background task `{}` failed: {error}", self.name))
            }
        }
    }
}

impl Shutdown {
    pub(crate) fn new() -> Self {
        let (sender, _) = watch::channel(false);
        Self { sender }
    }

    pub(crate) fn trigger(&self) {
        let _ = self.sender.send(true);
    }

    pub(crate) fn subscribe(&self) -> ShutdownReceiver {
        ShutdownReceiver {
            receiver: self.sender.subscribe(),
        }
    }
}

impl ShutdownReceiver {
    pub(crate) async fn cancelled(&mut self) {
        if *self.receiver.borrow() {
            return;
        }
        while self.receiver.changed().await.is_ok() {
            if *self.receiver.borrow() {
                return;
            }
        }
    }
}

pub(crate) async fn wait_for_process_shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to install Ctrl+C shutdown handler");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                warn!(%error, "failed to install SIGTERM shutdown handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    info!("shutdown signal received");
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::{ServiceTasks, Shutdown};

    struct NotifyOnDrop(Option<oneshot::Sender<()>>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn reports_exited_background_task() {
        let mut tasks = ServiceTasks::new();
        tasks.spawn("test task", async {});

        let error = tasks.wait_for_failure().await.unwrap_err();

        assert!(error.to_string().contains("test task"));
        assert!(error.to_string().contains("exited unexpectedly"));
    }

    #[tokio::test]
    async fn aborts_background_tasks_on_drop() {
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let mut tasks = ServiceTasks::new();
        tasks.spawn("waiting task", async move {
            let _notify = NotifyOnDrop(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });

        started_rx.await.expect("task should start");
        drop(tasks);

        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("task should be aborted")
            .expect("drop notification should be sent");
    }

    #[tokio::test]
    async fn shutdown_waits_for_cooperative_tasks() {
        let shutdown = Shutdown::new();
        let mut receiver = shutdown.subscribe();
        let mut tasks = ServiceTasks::new();
        tasks.spawn("cooperative task", async move {
            receiver.cancelled().await;
        });

        shutdown.trigger();

        tokio::time::timeout(
            Duration::from_secs(1),
            tasks.shutdown(Duration::from_secs(1)),
        )
        .await
        .expect("shutdown should not hang")
        .expect("cooperative task should stop cleanly");
    }

    #[tokio::test]
    async fn shutdown_aborts_tasks_after_grace_period() {
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let mut tasks = ServiceTasks::new();
        tasks.spawn("stuck task", async move {
            let _notify = NotifyOnDrop(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });

        started_rx.await.expect("task should start");

        tasks
            .shutdown(Duration::from_millis(1))
            .await
            .expect("aborted task should be accepted during shutdown");
        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("task should be aborted after grace timeout")
            .expect("drop notification should be sent");
    }

    #[tokio::test]
    async fn shutdown_notifies_receivers() {
        let shutdown = Shutdown::new();
        let mut receiver = shutdown.subscribe();

        shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(1), receiver.cancelled())
            .await
            .expect("shutdown receiver should be notified");
    }
}
