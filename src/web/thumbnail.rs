use axum::{
    extract::{Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use crate::thumbnail::ThumbnailStatus;

use super::AppState;

#[derive(Debug, Deserialize)]
pub(super) struct ThumbnailQuery {
    device: Option<String>,
    task: Option<String>,
}

pub(super) async fn thumbnail(
    State(state): State<AppState>,
    Query(query): Query<ThumbnailQuery>,
) -> Response {
    match state
        .thumbnail
        .thumbnail(query.device.as_deref(), query.task.as_deref())
        .await
    {
        Ok(ThumbnailStatus::Ready(image)) => {
            let content_type = HeaderValue::from_str(&image.content_type)
                .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
            (
                [
                    (header::CONTENT_TYPE, content_type),
                    (header::CACHE_CONTROL, HeaderValue::from_static("no-store")),
                    (header::PRAGMA, HeaderValue::from_static("no-cache")),
                ],
                image.bytes,
            )
                .into_response()
        }
        Ok(ThumbnailStatus::Loading(message)) => (
            StatusCode::ACCEPTED,
            [
                (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
                (header::CACHE_CONTROL, "no-store"),
                (header::PRAGMA, "no-cache"),
                (header::RETRY_AFTER, "2"),
            ],
            message,
        )
            .into_response(),
        Ok(ThumbnailStatus::Missing(message)) => (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            message,
        )
            .into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            error.to_string(),
        )
            .into_response(),
    }
}
