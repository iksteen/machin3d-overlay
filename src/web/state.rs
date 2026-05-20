//! Shared state handed to every Axum handler.
//!
//! `AppState` keeps the runtime services (MQTT, video, thumbnail, device
//! registry, shutdown, current-print snapshot builder) behind private fields
//! and exposes intent-shaped methods. Sibling handler modules cannot reach
//! `state.video`, `state.thumbnail`, etc. directly — the only doorway is the
//! method surface defined here. The intent is that future changes to those
//! services do not need a sweep across the web layer, and that handler tests
//! depend only on the surface this module documents.

use std::collections::HashSet;

use anyhow::Result;
use tokio::sync::broadcast;

use crate::{
    device_summary::summarize_devices,
    devices::DeviceRegistry,
    mqtt::{MqttRuntime, MqttStatusPayload},
    service::{Shutdown, ShutdownReceiver},
    thumbnail::{ThumbnailService, ThumbnailStatus},
    video::{VideoStreams, VideoSubscription},
};

use super::current_print::CurrentPrintPayload;

#[derive(Clone)]
pub(crate) struct AppState {
    mqtt: MqttRuntime,
    video: VideoStreams,
    thumbnail: ThumbnailService,
    devices: DeviceRegistry,
    shutdown: Shutdown,
}

impl AppState {
    pub(crate) fn new(
        mqtt: MqttRuntime,
        registry: DeviceRegistry,
        video: VideoStreams,
        thumbnail: ThumbnailService,
        shutdown: Shutdown,
    ) -> Self {
        Self {
            mqtt,
            video,
            thumbnail,
            devices: registry,
            shutdown,
        }
    }

    pub(super) async fn current_print_payload(&self) -> Result<CurrentPrintPayload> {
        let snapshot = self.mqtt.live_snapshot().await;
        let devices =
            summarize_devices(self.devices.devices(), &snapshot.devices, &snapshot.connections);
        Ok(CurrentPrintPayload::success(snapshot.status, devices))
    }

    pub(super) async fn mqtt_status(&self) -> MqttStatusPayload {
        self.mqtt.status().await
    }

    pub(super) fn mqtt_changes(&self) -> broadcast::Receiver<()> {
        self.mqtt.subscribe()
    }

    pub(super) fn shutdown_receiver(&self) -> ShutdownReceiver {
        self.shutdown.subscribe()
    }

    pub(super) async fn subscribe_video(
        &self,
        device_id: Option<&str>,
    ) -> Result<VideoSubscription> {
        self.video.subscribe(device_id).await
    }

    pub(super) async fn known_video_device_ids(&self) -> HashSet<String> {
        self.video.known_device_ids().await
    }

    pub(super) async fn thumbnail_status(
        &self,
        device_id: Option<&str>,
    ) -> Result<ThumbnailStatus> {
        self.thumbnail.thumbnail(device_id).await
    }

    pub(super) fn devices(&self) -> &DeviceRegistry {
        &self.devices
    }

    pub(super) fn known_device_id<'a>(&'a self, device_id: &str) -> Option<&'a str> {
        let device_id = device_id.trim();
        self.devices.get(device_id).map(|entry| entry.id())
    }

    pub(super) fn default_device_id(&self) -> Option<&str> {
        self.devices.first().map(|entry| entry.id())
    }
}
