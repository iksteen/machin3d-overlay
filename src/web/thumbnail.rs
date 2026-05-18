use axum::{
    extract::{Query, State},
    http::{header, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use crate::thumbnail::ThumbnailStatus;

use super::AppState;

#[derive(Debug, Deserialize)]
pub(super) struct ThumbnailQuery {
    device: Option<String>,
}

pub(super) async fn thumbnail(
    State(state): State<AppState>,
    Query(query): Query<ThumbnailQuery>,
) -> Response {
    match state.thumbnail.thumbnail(query.device.as_deref()).await {
        Ok(ThumbnailStatus::Ready(image)) => {
            let content_type = HeaderValue::from_str(&image.content_type)
                .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
            (thumbnail_headers(content_type), image.bytes).into_response()
        }
        Ok(ThumbnailStatus::Loading(message)) => {
            (StatusCode::ACCEPTED, loading_headers(), message).into_response()
        }
        Ok(ThumbnailStatus::Missing(message)) => (
            StatusCode::NOT_FOUND,
            thumbnail_headers(HeaderValue::from_static("text/plain; charset=utf-8")),
            message,
        )
            .into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            thumbnail_headers(HeaderValue::from_static("text/plain; charset=utf-8")),
            error.to_string(),
        )
            .into_response(),
    }
}

fn thumbnail_headers(content_type: HeaderValue) -> [(HeaderName, HeaderValue); 4] {
    [
        (header::CONTENT_TYPE, content_type),
        (
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-store, no-cache, max-age=0, must-revalidate"),
        ),
        (header::PRAGMA, HeaderValue::from_static("no-cache")),
        (header::EXPIRES, HeaderValue::from_static("0")),
    ]
}

fn loading_headers() -> [(HeaderName, HeaderValue); 5] {
    [
        (
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        ),
        (
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-store, no-cache, max-age=0, must-revalidate"),
        ),
        (header::PRAGMA, HeaderValue::from_static("no-cache")),
        (header::EXPIRES, HeaderValue::from_static("0")),
        (header::RETRY_AFTER, HeaderValue::from_static("2")),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thumbnail_headers_prevent_client_side_caching() {
        let headers = thumbnail_headers(HeaderValue::from_static("image/png"));

        assert_eq!(header_value(&headers, &header::CONTENT_TYPE), "image/png");
        assert_no_cache_headers(&headers);
    }

    #[test]
    fn loading_headers_prevent_client_side_caching_and_retry() {
        let headers = loading_headers();

        assert_eq!(
            header_value(&headers, &header::CONTENT_TYPE),
            "text/plain; charset=utf-8"
        );
        assert_no_cache_headers(&headers);
        assert_eq!(header_value(&headers, &header::RETRY_AFTER), "2");
    }

    fn assert_no_cache_headers<const N: usize>(headers: &[(HeaderName, HeaderValue); N]) {
        assert_eq!(
            header_value(headers, &header::CACHE_CONTROL),
            "no-store, no-cache, max-age=0, must-revalidate"
        );
        assert_eq!(header_value(headers, &header::PRAGMA), "no-cache");
        assert_eq!(header_value(headers, &header::EXPIRES), "0");
    }

    fn header_value<'a, const N: usize>(
        headers: &'a [(HeaderName, HeaderValue); N],
        name: &HeaderName,
    ) -> &'a str {
        headers
            .iter()
            .find(|(header_name, _)| header_name == name)
            .and_then(|(_, value)| value.to_str().ok())
            .unwrap()
    }
}
