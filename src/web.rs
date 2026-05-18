use std::net::SocketAddr;

mod current_print;
mod devices;
mod overlay_page;
mod paths;
mod thumbnail;
mod video;

use anyhow::{Context, Result};
use axum::{routing::get, Router};
use tokio::net::TcpListener;
use tracing::info;

use crate::{
    devices::DeviceRegistry, local::Endpoint, mqtt::MqttRuntime, service::Shutdown,
    thumbnail::ThumbnailService, video::VideoStreams,
};

use self::current_print::CurrentPrintService;

#[derive(Clone)]
pub(crate) struct AppState {
    current_print: CurrentPrintService,
    mqtt: MqttRuntime,
    video: VideoStreams,
    thumbnail: ThumbnailService,
    devices: DeviceRegistry,
    shutdown: Shutdown,
}

pub(crate) async fn serve_http(bind: Endpoint, state: AppState, shutdown: Shutdown) -> Result<()> {
    let app = Router::new()
        .route("/", get(overlay_page::horizontal_overlay))
        .route("/horizontal", get(overlay_page::horizontal_overlay))
        .route(
            "/devices/{device_id}/horizontal",
            get(overlay_page::horizontal_device_overlay),
        )
        .route("/vertical", get(overlay_page::vertical_overlay))
        .route(
            "/devices/{device_id}/vertical",
            get(overlay_page::vertical_device_overlay),
        )
        .route("/api/devices", get(devices::known_devices))
        .route("/api/current-print", get(current_print::current_print))
        .route(
            "/api/current-print/events",
            get(current_print::current_print_events),
        )
        .route("/thumbnail", get(thumbnail::thumbnail))
        .route(
            "/devices/{device_id}/thumbnail",
            get(thumbnail::device_thumbnail),
        )
        .route("/video.mjpeg", get(video::video_mjpeg))
        .route(
            "/devices/{device_id}/video.mjpeg",
            get(video::device_video_mjpeg),
        )
        .route("/static/{file}", get(overlay_page::static_asset))
        .with_state(state);

    let bind = bind.to_string();
    let address: SocketAddr = bind
        .parse()
        .with_context(|| format!("invalid bind address {bind}"))?;
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind {address}"))?;
    let mut shutdown = shutdown.subscribe();
    info!(%address, "serving Bambu overlay");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await
        .context("HTTP server failed")
}

pub(crate) fn app_state(
    mqtt: MqttRuntime,
    registry: DeviceRegistry,
    video: VideoStreams,
    thumbnail: ThumbnailService,
    shutdown: Shutdown,
) -> AppState {
    let devices = registry.clone();
    let current_print = CurrentPrintService::new(registry.clone(), mqtt.clone());

    AppState {
        current_print,
        mqtt,
        video,
        thumbnail,
        devices,
        shutdown,
    }
}
