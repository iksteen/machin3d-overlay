//! One worker per Moonraker device. Each worker maintains a
//! WebSocket session, decodes `notify_status_update` events into a
//! `PrinterReport`, and publishes into the shared `LiveStateStore`. Workers
//! reconnect with exponential backoff on transport errors.

use std::time::{Duration, Instant};

use anyhow::Result;
use tracing::{debug, warn};

use crate::{
    devices::DeviceRegistry,
    live::{ConnectionStatus, DeviceConnection, LiveStateStore},
    service::{ServiceTasks, Shutdown, ShutdownReceiver},
};

use super::{
    client::MoonrakerSession,
    metadata::{self, JobMetadata},
    report::{apply_job_metadata, to_live, EtaTracker},
    MoonrakerEndpoint,
};

const RETRY_INITIAL: Duration = Duration::from_secs(2);
const RETRY_MAX: Duration = Duration::from_secs(30);
/// Moonraker parses a gcode's metadata asynchronously, so a job can start
/// before its metadata exists. Retry on a slow cadence rather than hammering
/// the file API on every status update.
const METADATA_RETRY: Duration = Duration::from_secs(30);

pub(crate) fn spawn(
    live: LiveStateStore,
    registry: &DeviceRegistry,
    tasks: &mut ServiceTasks,
    shutdown: &Shutdown,
) {
    for (entry, snap) in registry.moonraker_entries() {
        let device_id = entry.id().to_owned();
        let endpoint = snap.endpoint.clone();
        let live = live.clone();
        let connection_key = format!("moonraker-{device_id}");
        let task_name = format!("Moonraker ({device_id})");
        tasks.spawn_with_shutdown(shutdown, task_name, move |shutdown| {
            run_device(device_id, endpoint, live, connection_key, shutdown)
        });
    }
}

async fn run_device(
    device_id: String,
    endpoint: MoonrakerEndpoint,
    live: LiveStateStore,
    connection_key: String,
    mut shutdown: ShutdownReceiver,
) {
    let mut delay = RETRY_INITIAL;
    loop {
        live.set_device_connection(
            &device_id,
            DeviceConnection {
                key: Some(connection_key.clone()),
                status: ConnectionStatus::Connecting,
                error: None,
            },
        )
        .await;

        match attempt_session(&device_id, &endpoint, &live, &connection_key, &mut shutdown).await {
            SessionResult::Shutdown => return,
            SessionResult::Closed => {
                live.set_device_connection(
                    &device_id,
                    DeviceConnection {
                        key: Some(connection_key.clone()),
                        status: ConnectionStatus::Disconnected,
                        error: None,
                    },
                )
                .await;
                delay = RETRY_INITIAL;
            }
            SessionResult::Failed(error) => {
                let message = format!("{error:#}");
                warn!(device_id = %device_id, error = %message, "Moonraker session failed");
                live.set_device_connection(
                    &device_id,
                    DeviceConnection {
                        key: Some(connection_key.clone()),
                        status: ConnectionStatus::Disconnected,
                        error: Some(message),
                    },
                )
                .await;
                if sleep_or_shutdown(delay, &mut shutdown).await {
                    return;
                }
                delay = (delay + delay / 2).min(RETRY_MAX);
            }
        }
    }
}

enum SessionResult {
    Shutdown,
    Closed,
    Failed(anyhow::Error),
}

async fn attempt_session(
    device_id: &str,
    endpoint: &MoonrakerEndpoint,
    live: &LiveStateStore,
    connection_key: &str,
    shutdown: &mut ShutdownReceiver,
) -> SessionResult {
    let session = tokio::select! {
        result = MoonrakerSession::connect(endpoint) => result,
        _ = shutdown.cancelled() => return SessionResult::Shutdown,
    };
    let mut session = match session {
        Ok(session) => session,
        Err(error) => return SessionResult::Failed(error),
    };

    // Held across the session: the ETA is re-derived only when progress moves,
    // and the slicer metadata is fetched once per job rather than per update.
    let mut eta = EtaTracker::default();
    let mut job = JobFacts::default();

    if let Err(error) = publish(
        device_id,
        endpoint,
        live,
        connection_key,
        &session.status(),
        &mut eta,
        &mut job,
    )
    .await
    {
        return SessionResult::Failed(error);
    }

    loop {
        let next = tokio::select! {
            next = session.next_status() => next,
            _ = shutdown.cancelled() => return SessionResult::Shutdown,
        };
        let status = match next {
            Ok(Some(status)) => status,
            Ok(None) => return SessionResult::Closed,
            Err(error) => return SessionResult::Failed(error),
        };
        if let Err(error) = publish(
            device_id,
            endpoint,
            live,
            connection_key,
            &status,
            &mut eta,
            &mut job,
        )
        .await
        {
            return SessionResult::Failed(error);
        }
    }
}

async fn publish(
    device_id: &str,
    endpoint: &MoonrakerEndpoint,
    live: &LiveStateStore,
    connection_key: &str,
    status: &serde_json::Map<String, serde_json::Value>,
    eta: &mut EtaTracker,
    job: &mut JobFacts,
) -> Result<()> {
    let mut report = to_live(status, eta);
    if let Some(metadata) = job
        .for_current_job(device_id, endpoint, report.filename.as_deref())
        .await
    {
        apply_job_metadata(&mut report, metadata);
    }
    live.publish_report(
        device_id,
        report,
        DeviceConnection {
            key: Some(connection_key.to_owned()),
            status: ConnectionStatus::Connected,
            error: None,
        },
    )
    .await;
    Ok(())
}

/// The slicer metadata for whatever job the printer is running, fetched once
/// per job. Cleared when the printer moves on to a different file.
#[derive(Default)]
struct JobFacts {
    filename: Option<String>,
    metadata: Option<JobMetadata>,
    last_attempt: Option<Instant>,
}

impl JobFacts {
    async fn for_current_job(
        &mut self,
        device_id: &str,
        endpoint: &MoonrakerEndpoint,
        filename: Option<&str>,
    ) -> Option<&JobMetadata> {
        let filename = filename?;
        if self.filename.as_deref() != Some(filename) {
            self.filename = Some(filename.to_owned());
            self.metadata = None;
            self.last_attempt = None;
        }
        let due = self
            .last_attempt
            .is_none_or(|attempt| attempt.elapsed() >= METADATA_RETRY);
        if self.metadata.is_none() && due {
            self.last_attempt = Some(Instant::now());
            match metadata::fetch(endpoint, filename).await {
                Ok(Some(metadata)) => self.metadata = Some(metadata),
                Ok(None) => debug!(
                    device_id = %device_id,
                    filename = %filename,
                    "Moonraker has not parsed this job's metadata yet"
                ),
                Err(error) => warn!(
                    device_id = %device_id,
                    filename = %filename,
                    error = %format!("{error:#}"),
                    "could not fetch Moonraker job metadata"
                ),
            }
        }
        self.metadata.as_ref()
    }
}

async fn sleep_or_shutdown(delay: Duration, shutdown: &mut ShutdownReceiver) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        _ = shutdown.cancelled() => true,
    }
}
