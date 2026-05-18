use std::convert::Infallible;

use async_stream::stream;
use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use serde::Deserialize;
use tracing::warn;

use super::AppState;

const MJPEG_BOUNDARY: &str = "frame";

#[derive(Debug, Deserialize)]
pub(super) struct VideoQuery {
    device: Option<String>,
}

pub(super) async fn video_mjpeg(
    State(state): State<AppState>,
    Query(query): Query<VideoQuery>,
) -> Response {
    let subscription = match state.video.subscribe(query.device.as_deref()).await {
        Ok(subscription) => subscription,
        Err(error) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                error.to_string(),
            )
                .into_response();
        }
    };

    let stream = stream! {
        let mut subscription = subscription;
        let mut shutdown = state.shutdown.subscribe();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                received = subscription.recv() => {
                    match received {
                        Ok(frame) => yield Ok::<Bytes, Infallible>(mjpeg_part(&frame)),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "MJPEG video client lagged behind");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    };

    (
        [
            (header::CONTENT_TYPE, mjpeg_content_type()),
            (header::CACHE_CONTROL, "no-store".to_owned()),
            (header::PRAGMA, "no-cache".to_owned()),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}

fn mjpeg_content_type() -> String {
    format!("multipart/x-mixed-replace; boundary={MJPEG_BOUNDARY}")
}

fn mjpeg_part(frame: &[u8]) -> Bytes {
    let header = format!(
        "--{MJPEG_BOUNDARY}\r\nContent-Type: image/jpeg\r\nContent-Length: {}\r\n\r\n",
        frame.len()
    );
    let mut part = Vec::with_capacity(header.len() + frame.len() + 2);
    part.extend_from_slice(header.as_bytes());
    part.extend_from_slice(frame);
    part.extend_from_slice(b"\r\n");
    Bytes::from(part)
}

#[cfg(test)]
mod tests {
    use super::mjpeg_part;

    #[test]
    fn mjpeg_part_contains_boundary_headers_and_frame() {
        let part = mjpeg_part(&[0xff, 0xd8, 0xff, 0xd9]);

        assert!(
            part.starts_with(b"--frame\r\nContent-Type: image/jpeg\r\nContent-Length: 4\r\n\r\n")
        );
        assert!(part.ends_with(&[0xff, 0xd8, 0xff, 0xd9, b'\r', b'\n']));
    }
}
