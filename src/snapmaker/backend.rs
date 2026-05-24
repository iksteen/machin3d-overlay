//! One Moonraker worker per Snapmaker device. Each worker maintains a
//! WebSocket session, decodes `notify_status_update` events into a
//! `PrinterReport`, and publishes into the shared `LiveStateStore`. Workers
//! reconnect with exponential backoff on transport errors.

use std::time::Duration;

use anyhow::Result;
use tracing::warn;

use crate::{
    devices::DeviceRegistry,
    live::{ConnectionStatus, DeviceConnection, LiveStateStore},
    service::{ServiceTasks, Shutdown, ShutdownReceiver},
};

use super::{moonraker::MoonrakerSession, report::to_live, SnapmakerEndpoint};

const RETRY_INITIAL: Duration = Duration::from_secs(2);
const RETRY_MAX: Duration = Duration::from_secs(30);

pub(crate) fn spawn(
    live: LiveStateStore,
    registry: &DeviceRegistry,
    tasks: &mut ServiceTasks,
    shutdown: &Shutdown,
) {
    for (entry, snap) in registry.snapmaker_entries() {
        let device_id = entry.id().to_owned();
        let endpoint = snap.endpoint.clone();
        let live = live.clone();
        let connection_key = format!("snap-{device_id}");
        let task_name = format!("snapmaker Moonraker ({device_id})");
        tasks.spawn_with_shutdown(shutdown, task_name, move |shutdown| {
            run_device(device_id, endpoint, live, connection_key, shutdown)
        });
    }
}

async fn run_device(
    device_id: String,
    endpoint: SnapmakerEndpoint,
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
                warn!(device_id = %device_id, error = %message, "Snapmaker session failed");
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
    endpoint: &SnapmakerEndpoint,
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

    if let Err(error) = publish(device_id, live, connection_key, &session.status()).await {
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
        if let Err(error) = publish(device_id, live, connection_key, &status).await {
            return SessionResult::Failed(error);
        }
    }
}

async fn publish(
    device_id: &str,
    live: &LiveStateStore,
    connection_key: &str,
    status: &serde_json::Map<String, serde_json::Value>,
) -> Result<()> {
    let report = to_live(status);
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

async fn sleep_or_shutdown(delay: Duration, shutdown: &mut ShutdownReceiver) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        _ = shutdown.cancelled() => true,
    }
}
