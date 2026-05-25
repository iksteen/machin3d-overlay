use std::time::Duration;

use anyhow::Result;
use tokio::io::{self, AsyncWriteExt};

use super::{session::ReportSession, MqttTarget};

pub(crate) async fn monitor_target(target: MqttTarget) -> Result<()> {
    let mut delay = Duration::from_secs(2);
    loop {
        tokio::select! {
            result = run_monitor_once(&target) => {
                match result {
                    Ok(()) => delay = Duration::from_secs(2),
                    Err(error) => {
                        target.warn_disconnect(&error, "MQTT monitor disconnected");
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }

        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
        delay = (delay + delay / 2).min(Duration::from_secs(30));
    }
}

async fn run_monitor_once(target: &MqttTarget) -> Result<()> {
    let mut session = ReportSession::connect(target).await?;
    let mut stdout = io::stdout();
    while let Some(event) = session.next().await? {
        stdout.write_all(&event.payload).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }
    Ok(())
}
