//! Minimal Moonraker JSON-RPC over WebSocket client.
//!
//! On connect, sends `printer.objects.subscribe` for the printer objects we
//! consume in `report.rs`. The subscribe response is decoded as the initial
//! status, then `notify_status_update` messages merge into a cached status
//! map and yield a fresh snapshot to the caller.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Map, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};
use tracing::debug;

use super::SnapmakerEndpoint;

const SUBSCRIBE_OBJECTS: &[&str] = &[
    "print_stats",
    "display_status",
    "extruder",
    "extruder1",
    "extruder2",
    "extruder3",
    "heater_bed",
    "virtual_sdcard",
    "print_task_config",
    "gcode_move",
];

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(super) struct MoonrakerSession {
    socket: Socket,
    status: Map<String, Value>,
    next_request_id: u64,
}

impl MoonrakerSession {
    pub(super) async fn connect(endpoint: &SnapmakerEndpoint) -> Result<Self> {
        let url = format!("ws://{}:{}/websocket", endpoint.host, endpoint.port);
        let request = url
            .as_str()
            .into_client_request()
            .with_context(|| format!("invalid Moonraker WebSocket URL `{url}`"))?;
        let (socket, _response) = connect_async(request)
            .await
            .with_context(|| format!("failed to connect to Moonraker at {url}"))?;
        debug!(host = %endpoint.host, port = endpoint.port, "moonraker connected");
        let mut session = Self {
            socket,
            status: Map::new(),
            next_request_id: 1,
        };
        let initial = session.send_subscribe().await?;
        session.merge_status(&initial);
        Ok(session)
    }

    /// Current merged status map. Cloned because the caller (the report
    /// converter) wants an owned snapshot it can pass around.
    pub(super) fn status(&self) -> Map<String, Value> {
        self.status.clone()
    }

    /// Block until the next status update arrives. Returns `None` if the
    /// connection closed normally.
    pub(super) async fn next_status(&mut self) -> Result<Option<Map<String, Value>>> {
        loop {
            let Some(message) = self.socket.next().await else {
                return Ok(None);
            };
            let message = message.context("Moonraker WS read failed")?;
            let text = match message {
                Message::Text(text) => text,
                Message::Binary(_) | Message::Ping(_) | Message::Pong(_) => continue,
                Message::Close(_) => return Ok(None),
                Message::Frame(_) => continue,
            };
            let value: Value = serde_json::from_str(&text)
                .context("Moonraker WS sent message that is not valid JSON")?;
            if value.get("method").and_then(Value::as_str) != Some("notify_status_update") {
                continue;
            }
            let Some(update) = value
                .get("params")
                .and_then(Value::as_array)
                .and_then(|array| array.first())
                .and_then(Value::as_object)
                .cloned()
            else {
                continue;
            };
            self.merge_status(&update);
            return Ok(Some(self.status.clone()));
        }
    }

    async fn send_subscribe(&mut self) -> Result<Map<String, Value>> {
        let id = self.next_request_id;
        self.next_request_id += 1;
        let objects: Map<String, Value> = SUBSCRIBE_OBJECTS
            .iter()
            .map(|name| ((*name).to_owned(), Value::Null))
            .collect();
        let request = json!({
            "jsonrpc": "2.0",
            "method": "printer.objects.subscribe",
            "params": { "objects": objects },
            "id": id,
        });
        self.socket
            .send(Message::Text(request.to_string()))
            .await
            .context("failed to send Moonraker subscribe request")?;

        while let Some(message) = self.socket.next().await {
            let message = message.context("Moonraker WS read failed")?;
            let text = match message {
                Message::Text(text) => text,
                Message::Binary(_) | Message::Ping(_) | Message::Pong(_) => continue,
                Message::Close(_) => bail!("Moonraker closed before subscribe response"),
                Message::Frame(_) => continue,
            };
            let value: Value = serde_json::from_str(&text)
                .context("Moonraker WS sent message that is not valid JSON")?;
            if value.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            if let Some(error) = value.get("error") {
                bail!("Moonraker subscribe error: {error}");
            }
            let status = value
                .get("result")
                .and_then(|result| result.get("status"))
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            return Ok(status);
        }
        bail!("Moonraker closed before subscribe response");
    }

    fn merge_status(&mut self, update: &Map<String, Value>) {
        for (key, value) in update {
            match (self.status.get_mut(key), value) {
                (Some(existing), Value::Object(patch)) if existing.is_object() => {
                    let existing = existing.as_object_mut().expect("checked is_object");
                    for (subkey, subvalue) in patch {
                        existing.insert(subkey.clone(), subvalue.clone());
                    }
                }
                _ => {
                    self.status.insert(key.clone(), value.clone());
                }
            }
        }
    }
}

/// Pull a string field from a nested object in the status map.
pub(super) fn get_string<'a>(
    status: &'a Map<String, Value>,
    object: &str,
    field: &str,
) -> Option<&'a str> {
    status.get(object)?.get(field)?.as_str()
}

pub(super) fn get_f64(status: &Map<String, Value>, object: &str, field: &str) -> Option<f64> {
    status.get(object)?.get(field)?.as_f64()
}

/// Read a nested field from `print_stats.info.<field>` (a sub-object that Moonraker
/// uses for layer counts).
pub(super) fn get_print_info_i64(status: &Map<String, Value>, field: &str) -> Option<i64> {
    status
        .get("print_stats")?
        .get("info")?
        .get(field)?
        .as_i64()
}

/// Collect each `extruder`, `extruder1`, ..., `extruderN` object that the printer
/// reports, keyed by zero-based index.
pub(super) fn extruders(status: &Map<String, Value>) -> HashMap<usize, &Value> {
    let mut extruders = HashMap::new();
    for (key, value) in status {
        if let Some(index) = extruder_index(key) {
            extruders.insert(index, value);
        }
    }
    extruders
}

fn extruder_index(name: &str) -> Option<usize> {
    if name == "extruder" {
        return Some(0);
    }
    name.strip_prefix("extruder")?.parse::<usize>().ok()
}
