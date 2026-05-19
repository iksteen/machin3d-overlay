use std::net::SocketAddr;

mod current_print;
mod devices;
mod overlay_page;
mod paths;
mod thumbnail;
mod video;

use anyhow::{Context, Result};
use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
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
    let app = router(state);

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

fn router(state: AppState) -> Router {
    Router::new()
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
        .with_state(state)
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

fn known_device_id<'a>(state: &'a AppState, device_id: &str) -> Option<&'a str> {
    let device_id = device_id.trim();
    state.devices.get(device_id).map(|entry| entry.id())
}

fn default_device_id(state: &AppState) -> Option<&str> {
    state.devices.first().map(|entry| entry.id())
}

fn device_not_found(device_id: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        format!("device `{}` is not known", device_id.trim()),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    use crate::{
        bambu::CloudDevice, devices::DeviceRegistry, mqtt::MqttRuntime, secret::Secret,
        service::Shutdown, thumbnail::ThumbnailService, video::VideoStreams,
    };

    use super::{app_state, router};

    #[tokio::test]
    async fn device_layout_route_decodes_device_path_segment() {
        let response = request("/devices/printer%20a%2F1/horizontal").await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains(r#""selectedDeviceId":"printer a/1""#));
    }

    #[tokio::test]
    async fn media_routes_return_not_found_for_unknown_devices() {
        for path in [
            "/devices/missing/thumbnail",
            "/devices/missing/video.mjpeg",
            "/devices/missing/horizontal",
        ] {
            let response = request(path).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[tokio::test]
    async fn old_api_media_routes_are_not_registered() {
        for path in ["/api/thumbnail", "/api/video.mjpeg"] {
            let response = request(path).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
    }

    async fn request(path: &str) -> axum::response::Response {
        router(test_state())
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    fn test_state() -> super::AppState {
        let mqtt = MqttRuntime::new();
        let registry = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer a/1".to_owned()),
                access_code: Some(Secret::new("12345678".to_owned())),
                ..CloudDevice::default()
            }],
            Vec::new(),
        );
        let video = VideoStreams::new(registry.clone(), HashMap::new()).unwrap();
        let thumbnail = ThumbnailService::new(mqtt.clone(), None, registry.clone());
        app_state(mqtt, registry, video, thumbnail, Shutdown::new())
    }
}
