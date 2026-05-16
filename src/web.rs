use std::net::SocketAddr;

mod current_print;
mod devices;
mod mjpeg;
mod overlay_page;
mod thumbnail;
mod video;

use anyhow::{Context, Result};
use axum::{routing::get, Router};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::{
    devices::{DeviceRegistry, ResolvedVideoEndpoints},
    local::Endpoint,
    mqtt::MqttRuntime,
    thumbnail::ThumbnailRuntime,
    video::VideoRuntime,
};

use self::current_print::CurrentPrintService;

#[derive(Clone)]
pub(crate) struct AppState {
    current_print: CurrentPrintService,
    mqtt: MqttRuntime,
    video: VideoRuntime,
    thumbnail: ThumbnailRuntime,
    devices: DeviceRegistry,
}

pub(crate) async fn serve_http(bind: Endpoint, state: AppState) -> Result<()> {
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
    info!(%address, "serving Bambu overlay");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("HTTP server failed")
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to install Ctrl+C shutdown handler");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                warn!(%error, "failed to install SIGTERM shutdown handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    info!("shutdown signal received");
}

pub(crate) fn app_state(
    mqtt: MqttRuntime,
    registry: DeviceRegistry,
    video_endpoints: ResolvedVideoEndpoints,
    thumbnail: ThumbnailRuntime,
) -> Result<AppState> {
    let devices = registry.clone();
    let current_print = CurrentPrintService::new(registry.clone(), mqtt.clone());
    let video = VideoRuntime::new(
        registry,
        video_endpoints.endpoints,
        video_endpoints.endpoint_map,
    )?;

    Ok(AppState {
        current_print,
        mqtt,
        video,
        thumbnail,
        devices,
    })
}
