//! Moonraker thumbnail fetcher.
//!
//! Moonraker exposes each uploaded gcode's embedded thumbnails via the
//! `server/files/metadata` JSON, which lists `relative_path` entries served
//! from the gcodes root. We pick the largest preview (typically 300x300) and
//! download it.

use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use serde::Deserialize;
use url::Url;

use crate::moonraker::MoonrakerEndpoint;

use super::{image_content_type, ThumbnailImage, ThumbnailStatus, MAX_THUMBNAIL_SIZE};

const METADATA_TIMEOUT: Duration = Duration::from_secs(8);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) async fn fetch_thumbnail(
    endpoint: &MoonrakerEndpoint,
    filename: &str,
) -> Result<ThumbnailStatus> {
    let metadata_url = metadata_url(endpoint, filename)?;
    let client = reqwest::Client::builder()
        .timeout(METADATA_TIMEOUT)
        .build()
        .context("failed to build Moonraker thumbnail HTTP client")?;
    let response = client
        .get(metadata_url.clone())
        .send()
        .await
        .with_context(|| format!("failed to fetch Moonraker metadata at {metadata_url}"))?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(ThumbnailStatus::Missing(format!(
            "Moonraker has no metadata for `{filename}` yet"
        )));
    }
    let response = response
        .error_for_status()
        .with_context(|| format!("Moonraker metadata at {metadata_url} returned an error"))?;
    let body: MetadataResponse = response
        .json()
        .await
        .with_context(|| format!("Moonraker metadata at {metadata_url} was not valid JSON"))?;

    let Some(largest) = pick_thumbnail(&body.result.thumbnails) else {
        return Ok(ThumbnailStatus::Missing(format!(
            "Moonraker metadata for `{filename}` lists no thumbnails"
        )));
    };
    let download_url = thumbnail_url(endpoint, &largest.relative_path)?;
    let downloaded = client
        .get(download_url.clone())
        .timeout(DOWNLOAD_TIMEOUT)
        .send()
        .await
        .with_context(|| format!("failed to download Moonraker thumbnail at {download_url}"))?
        .error_for_status()
        .with_context(|| {
            format!("Moonraker thumbnail download at {download_url} returned an error")
        })?;
    let content_type = downloaded
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let bytes = downloaded
        .bytes()
        .await
        .with_context(|| format!("failed to read Moonraker thumbnail bytes from {download_url}"))?;
    if bytes.len() > MAX_THUMBNAIL_SIZE {
        anyhow::bail!("Moonraker thumbnail at {download_url} exceeds {MAX_THUMBNAIL_SIZE} bytes");
    }
    Ok(ThumbnailStatus::Ready(ThumbnailImage {
        content_type: image_content_type(content_type.as_deref(), &bytes),
        bytes: Bytes::from(bytes.to_vec()),
    }))
}

fn metadata_url(endpoint: &MoonrakerEndpoint, filename: &str) -> Result<Url> {
    let mut url = base_url(endpoint, "server/files/metadata")?;
    url.query_pairs_mut().append_pair("filename", filename);
    Ok(url)
}

fn thumbnail_url(endpoint: &MoonrakerEndpoint, relative_path: &str) -> Result<Url> {
    let mut url = base_url(endpoint, "server/files/gcodes")?;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Moonraker URL cannot accept path segments"))?;
        for segment in relative_path.split('/').filter(|s| !s.is_empty()) {
            segments.push(segment);
        }
    }
    Ok(url)
}

fn base_url(endpoint: &MoonrakerEndpoint, path: &str) -> Result<Url> {
    Url::parse(&format!(
        "http://{host}:{port}/{path}",
        host = endpoint.host,
        port = endpoint.port,
    ))
    .with_context(|| format!("invalid Moonraker base URL for `{}`", endpoint.host))
}

fn pick_thumbnail(thumbnails: &[Thumbnail]) -> Option<&Thumbnail> {
    thumbnails
        .iter()
        .filter(|thumb| !thumb.relative_path.trim().is_empty())
        .max_by_key(|thumb| thumb.width.saturating_mul(thumb.height))
}

#[derive(Deserialize)]
struct MetadataResponse {
    result: MetadataResult,
}

#[derive(Deserialize)]
struct MetadataResult {
    #[serde(default)]
    thumbnails: Vec<Thumbnail>,
}

#[derive(Deserialize)]
struct Thumbnail {
    #[serde(default)]
    width: u32,
    #[serde(default)]
    height: u32,
    relative_path: String,
}

#[cfg(test)]
mod tests {
    use super::{pick_thumbnail, thumbnail_url, Thumbnail};
    use crate::moonraker::MoonrakerEndpoint;

    fn endpoint() -> MoonrakerEndpoint {
        MoonrakerEndpoint::new("192.168.0.120", 80)
    }

    fn thumb(width: u32, height: u32, path: &str) -> Thumbnail {
        Thumbnail {
            width,
            height,
            relative_path: path.to_owned(),
        }
    }

    #[test]
    fn pick_thumbnail_returns_the_largest_image() {
        let entries = vec![
            thumb(48, 48, ".thumbs/cube-48x48.png"),
            thumb(300, 300, ".thumbs/cube-300x300.png"),
            thumb(96, 96, ".thumbs/cube-96x96.png"),
        ];
        let picked = pick_thumbnail(&entries).expect("largest thumbnail is returned");
        assert_eq!(picked.width, 300);
        assert_eq!(picked.relative_path, ".thumbs/cube-300x300.png");
    }

    #[test]
    fn pick_thumbnail_skips_blank_paths() {
        let entries = vec![thumb(300, 300, "   "), thumb(48, 48, ".thumbs/cube.png")];
        let picked = pick_thumbnail(&entries).expect("the only usable thumbnail is returned");
        assert_eq!(picked.relative_path, ".thumbs/cube.png");
    }

    #[test]
    fn thumbnail_url_encodes_spaces_in_paths() {
        let url = thumbnail_url(
            &endpoint(),
            ".thumbs/Click Case - ORANGECON_PLA-300x300.png",
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "http://192.168.0.120/server/files/gcodes/.thumbs/Click%20Case%20-%20ORANGECON_PLA-300x300.png"
        );
    }
}
