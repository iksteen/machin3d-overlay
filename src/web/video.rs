use std::convert::Infallible;

use async_stream::stream;
use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tracing::warn;

use super::{mjpeg, AppState};

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
                        Ok(frame) => yield Ok::<bytes::Bytes, Infallible>(mjpeg::part(&frame)),
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
            (header::CONTENT_TYPE, mjpeg::content_type()),
            (header::CACHE_CONTROL, "no-store".to_owned()),
            (header::PRAGMA, "no-cache".to_owned()),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}
