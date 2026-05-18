use std::net::SocketAddr;

mod current_print;
mod devices;
mod mjpeg;
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
    thumbnail::ThumbnailRuntime, video::VideoRuntime,
};

use self::current_print::CurrentPrintService;

#[derive(Clone)]
pub(crate) struct AppState {
    current_print: CurrentPrintService,
    mqtt: MqttRuntime,
    video: VideoRuntime,
    thumbnail: ThumbnailRuntime,
    devices: DeviceRegistry,
    shutdown: Shutdown,
}

pub(crate) async fn serve_http(bind: Endpoint, state: AppState, shutdown: Shutdown) -> Result<()> {
    let app = Router::new()
        .route("/", get(overlay_page::horizontal_overlay))
        .route("/overlay", get(overlay_page::horizontal_overlay))
        .route("/vertical", get(overlay_page::vertical_overlay))
        .route("/api/devices", get(devices::known_devices))
        .route("/api/current-print", get(current_print::current_print))
        .route(
            "/api/current-print/events",
            get(current_print::current_print_events),
        )
        .route("/api/thumbnail", get(thumbnail::thumbnail))
        .route("/api/video.mjpeg", get(video::video_mjpeg))
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
    video: VideoRuntime,
    thumbnail: ThumbnailRuntime,
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
