use std::net::SocketAddr;

mod current_print;
mod devices;
mod overlay_page;
mod paths;
mod state;
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

use crate::{local::Endpoint, service::Shutdown};

pub(crate) use state::AppState;

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
        bambu::CloudDevice, devices::DeviceRegistry, live::LiveStateStore, mqtt::MqttRuntime,
        secret::Secret, service::Shutdown, thumbnail::ThumbnailService, video::VideoStreams,
    };

    use super::{router, AppState};

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

    fn test_state() -> AppState {
        let live = LiveStateStore::new();
        let mqtt = MqttRuntime::new(live.clone());
        let registry = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer a/1".to_owned()),
                access_code: Some(Secret::new("12345678".to_owned())),
                ..CloudDevice::default()
            }],
            Vec::new(),
        );
        let (video, _video_events) =
            VideoStreams::new(registry.clone(), HashMap::new(), Shutdown::new()).unwrap();
        let thumbnail =
            ThumbnailService::new(mqtt.clone(), live.clone(), None, registry.clone());
        AppState::new(live, registry, video, thumbnail, Shutdown::new())
    }
}
