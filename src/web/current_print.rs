use std::{convert::Infallible, time::Duration};

use anyhow::Result;
use async_stream::stream;
use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Json,
};
use serde_json::json;

use crate::{device_summary::summarize_devices, devices::DeviceRegistry, mqtt::MqttRuntime};

mod payload;

use payload::CurrentPrintPayload;

use super::AppState;

#[derive(Clone)]
pub(super) struct CurrentPrintService {
    registry: DeviceRegistry,
    mqtt: MqttRuntime,
}

impl CurrentPrintService {
    pub(super) fn new(registry: DeviceRegistry, mqtt: MqttRuntime) -> Self {
        Self { registry, mqtt }
    }

    async fn payload(&self) -> Result<CurrentPrintPayload> {
        let snapshot = self.mqtt.snapshot().await;
        let devices = summarize_devices(
            self.registry.devices(),
            &snapshot.devices,
            &snapshot.connections,
        );

        Ok(CurrentPrintPayload::success(snapshot.status, devices))
    }
}

pub(super) async fn current_print(State(state): State<AppState>) -> Response {
    match state.current_print.payload().await {
        Ok(payload) => Json(payload).into_response(),
        Err(error) => {
            let payload = CurrentPrintPayload::error(error.to_string(), state.mqtt.status().await);
            (StatusCode::BAD_GATEWAY, Json(payload)).into_response()
        }
    }
}

pub(super) async fn current_print_events(
    State(state): State<AppState>,
) -> Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>> {
    let mut changes = state.mqtt.subscribe();
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut shutdown = state.shutdown.subscribe();
    let stream = stream! {
        yield Ok(current_print_event(&state).await);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
                received = changes.recv() => {
                    if received.is_err() {
                        changes = state.mqtt.subscribe();
                    }
                }
            }
            yield Ok(current_print_event(&state).await);
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn current_print_event(state: &AppState) -> Event {
    let payload = match state.current_print.payload().await {
        Ok(payload) => serde_json::to_string(&payload),
        Err(error) => {
            let payload = CurrentPrintPayload::error(error.to_string(), state.mqtt.status().await);
            serde_json::to_string(&payload)
        }
    }
    .unwrap_or_else(|error| json!({"ok": false, "error": error.to_string()}).to_string());

    Event::default().event("current-print").data(payload)
}
