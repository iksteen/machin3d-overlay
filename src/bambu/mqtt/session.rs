use std::collections::HashSet;

use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, EventLoop, Packet, QoS};
use serde::Serialize;
use tracing::debug;

use super::MqttTarget;

#[derive(Serialize)]
struct PushAllRequest {
    pushing: PushAllCommand,
}

#[derive(Serialize)]
struct PushAllCommand {
    sequence_id: String,
    command: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    push_target: Option<u8>,
}

pub(super) struct ReportSession {
    eventloop: EventLoop,
    topics: HashSet<String>,
}

pub(super) struct ReportEvent {
    pub(super) topic: String,
    pub(super) payload: Vec<u8>,
    pub(super) retained: bool,
}

impl ReportSession {
    pub(super) async fn connect(target: &MqttTarget) -> Result<Self> {
        let device_ids = target.device_ids();
        let topics = device_ids
            .iter()
            .map(|device_id| format!("device/{device_id}/report"))
            .collect::<HashSet<_>>();
        let options = target.options()?;
        let (client, eventloop) = AsyncClient::new(options, 32);

        for device_id in &device_ids {
            subscribe_report(&client, device_id)
                .await
                .with_context(|| format!("failed to subscribe to {device_id}"))?;
        }
        for (sequence_id, device_id) in device_ids.iter().enumerate() {
            request_pushall(&client, device_id, pushall(target, sequence_id.to_string()))
                .await
                .with_context(|| format!("failed to request pushall for {device_id}"))?;
        }

        Ok(Self { eventloop, topics })
    }

    pub(super) async fn next(&mut self) -> Result<Option<ReportEvent>> {
        loop {
            match self.eventloop.poll().await? {
                Event::Incoming(Packet::Publish(publish))
                    if self.topics.contains(&publish.topic) =>
                {
                    return Ok(Some(ReportEvent {
                        topic: publish.topic,
                        payload: publish.payload.to_vec(),
                        retained: publish.retain,
                    }));
                }
                Event::Incoming(Packet::Publish(publish)) => {
                    debug!(topic = %publish.topic, "ignoring unexpected MQTT topic");
                }
                Event::Incoming(Packet::Disconnect) => return Ok(None),
                _ => {}
            }
        }
    }
}

async fn subscribe_report(client: &AsyncClient, device_id: &str) -> Result<()> {
    client
        .subscribe(format!("device/{device_id}/report"), QoS::AtMostOnce)
        .await?;
    Ok(())
}

async fn request_pushall(
    client: &AsyncClient,
    device_id: &str,
    request: PushAllRequest,
) -> Result<()> {
    client
        .publish(
            format!("device/{device_id}/request"),
            QoS::AtMostOnce,
            false,
            serde_json::to_vec(&request)?,
        )
        .await?;
    Ok(())
}

fn pushall(target: &MqttTarget, sequence_id: String) -> PushAllRequest {
    match target {
        MqttTarget::Cloud { .. } => cloud_pushall(sequence_id),
        MqttTarget::Local(_) => local_pushall(),
    }
}

fn cloud_pushall(sequence_id: String) -> PushAllRequest {
    PushAllRequest {
        pushing: PushAllCommand {
            sequence_id,
            command: "pushall",
            version: None,
            push_target: None,
        },
    }
}

fn local_pushall() -> PushAllRequest {
    PushAllRequest {
        pushing: PushAllCommand {
            sequence_id: "0".to_owned(),
            command: "pushall",
            version: Some(1),
            push_target: Some(1),
        },
    }
}
