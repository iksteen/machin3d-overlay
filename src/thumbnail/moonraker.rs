//! Moonraker thumbnail fetcher.
//!
//! Moonraker exposes each uploaded gcode's embedded thumbnails in its file
//! metadata (fetched by [`crate::moonraker::metadata`]) as `relative_path`
//! entries served from the gcodes root. We pick the largest preview
//! (typically 300x300) and download it.

use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;

use crate::moonraker::{
    metadata::{self, Thumbnail},
    MoonrakerEndpoint,
};

use super::{image_content_type, ThumbnailImage, ThumbnailStatus, MAX_THUMBNAIL_SIZE};

const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) async fn fetch_thumbnail(
    endpoint: &MoonrakerEndpoint,
    filename: &str,
) -> Result<ThumbnailStatus> {
    let Some(metadata) = metadata::fetch(endpoint, filename).await? else {
        return Ok(ThumbnailStatus::Missing(format!(
            "Moonraker has no metadata for `{filename}` yet"
        )));
    };
    let Some(largest) = pick_thumbnail(&metadata.thumbnails) else {
        return Ok(ThumbnailStatus::Missing(format!(
            "Moonraker metadata for `{filename}` lists no thumbnails"
        )));
    };
    let download_url = metadata::gcode_file_url(endpoint, &largest.relative_path)?;
    let downloaded = reqwest::Client::builder()
        .timeout(DOWNLOAD_TIMEOUT)
        .build()
        .context("failed to build Moonraker thumbnail HTTP client")?
        .get(download_url.clone())
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

fn pick_thumbnail(thumbnails: &[Thumbnail]) -> Option<&Thumbnail> {
    thumbnails
        .iter()
        .filter(|thumb| !thumb.relative_path.trim().is_empty())
        .max_by_key(|thumb| thumb.width.saturating_mul(thumb.height))
}

#[cfg(test)]
mod tests {
    use super::{pick_thumbnail, Thumbnail};

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
}
