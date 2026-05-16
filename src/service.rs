use std::future::Future;

use anyhow::{anyhow, Result};
use tokio::task::{AbortHandle, JoinSet};

pub(crate) struct ServiceTasks {
    aborts: Vec<AbortHandle>,
    monitors: JoinSet<TaskFailure>,
}

struct TaskFailure {
    name: String,
    message: String,
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
            let message = match handle.await {
                Ok(()) => "exited unexpectedly".to_owned(),
                Err(error) if error.is_cancelled() => "was cancelled unexpectedly".to_owned(),
                Err(error) if error.is_panic() => format!("panicked: {error}"),
                Err(error) => format!("failed: {error}"),
            };
            TaskFailure { name, message }
        });
    }

    pub(crate) async fn wait_for_failure(&mut self) -> Result<()> {
        match self.monitors.join_next().await {
            Some(Ok(failure)) => Err(anyhow!(
                "background task `{}` {}",
                failure.name,
                failure.message
            )),
            Some(Err(error)) => Err(anyhow!("background task monitor failed: {error}")),
            None => std::future::pending::<Result<()>>().await,
        }
    }
}

impl Drop for ServiceTasks {
    fn drop(&mut self) {
        for abort in &self.aborts {
            abort.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::ServiceTasks;

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
}
