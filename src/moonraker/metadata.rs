//! Slicer metadata for a gcode file, from Moonraker's file API.
//!
//! Moonraker parses each uploaded gcode once and caches what the slicer wrote
//! into it. That gives us the facts the live printer objects do not carry: the
//! slicer's own print-time estimate (a constant for the job, unlike anything
//! derived from progress), the filament weight, when the job started, and the
//! embedded preview images.
//!
//! Two consumers: the report converter ([`super::report`], for the print
//! facts) and the thumbnail fetcher ([`crate::thumbnail`], for the previews).

use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use url::Url;

use super::MoonrakerEndpoint;

const METADATA_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct JobMetadata {
    /// The slicer's print-time estimate, in seconds.
    #[serde(default)]
    pub(crate) estimated_time: Option<f64>,
    /// Unix timestamp of when this file's current job started.
    #[serde(default)]
    pub(crate) print_start_time: Option<f64>,
    /// Total filament for the job, in grams.
    #[serde(default)]
    pub(crate) filament_weight_total: Option<f64>,
    #[serde(default)]
    pub(crate) thumbnails: Vec<Thumbnail>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Thumbnail {
    #[serde(default)]
    pub(crate) width: u32,
    #[serde(default)]
    pub(crate) height: u32,
    pub(crate) relative_path: String,
}

/// Fetch the metadata for `filename`. `Ok(None)` means Moonraker has no
/// metadata for that file *yet* — it parses asynchronously after upload, so a
/// job can start before its metadata exists, and the caller should retry.
pub(crate) async fn fetch(
    endpoint: &MoonrakerEndpoint,
    filename: &str,
) -> Result<Option<JobMetadata>> {
    let url = metadata_url(endpoint, filename)?;
    let client = reqwest::Client::builder()
        .timeout(METADATA_TIMEOUT)
        .build()
        .context("failed to build Moonraker metadata HTTP client")?;
    let response = client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("failed to fetch Moonraker metadata at {url}"))?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let response = response
        .error_for_status()
        .with_context(|| format!("Moonraker metadata at {url} returned an error"))?;
    let body: MetadataResponse = response
        .json()
        .await
        .with_context(|| format!("Moonraker metadata at {url} was not valid JSON"))?;
    Ok(Some(body.result))
}

/// URL of a file under the gcodes root, e.g. an embedded thumbnail's
/// `relative_path`.
pub(crate) fn gcode_file_url(endpoint: &MoonrakerEndpoint, relative_path: &str) -> Result<Url> {
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

fn metadata_url(endpoint: &MoonrakerEndpoint, filename: &str) -> Result<Url> {
    let mut url = base_url(endpoint, "server/files/metadata")?;
    url.query_pairs_mut().append_pair("filename", filename);
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

#[derive(Deserialize)]
struct MetadataResponse {
    result: JobMetadata,
}

#[cfg(test)]
mod tests {
    use super::{gcode_file_url, metadata_url, JobMetadata};
    use crate::moonraker::MoonrakerEndpoint;

    fn endpoint() -> MoonrakerEndpoint {
        MoonrakerEndpoint::new("192.168.0.120", 80)
    }

    #[test]
    fn gcode_file_url_encodes_spaces_in_paths() {
        let url = gcode_file_url(
            &endpoint(),
            ".thumbs/Click Case - ORANGECON_PLA-300x300.png",
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "http://192.168.0.120/server/files/gcodes/.thumbs/Click%20Case%20-%20ORANGECON_PLA-300x300.png"
        );
    }

    #[test]
    fn metadata_url_carries_the_filename_as_a_query_pair() {
        let url = metadata_url(&endpoint(), "Some Print.gcode").unwrap();
        assert_eq!(
            url.as_str(),
            "http://192.168.0.120/server/files/metadata?filename=Some+Print.gcode"
        );
    }

    /// The U1 sends far more fields than we model, and a plain Klipper printer
    /// may send fewer; neither may break the parse.
    #[test]
    fn parses_the_fields_we_use_and_ignores_the_rest() {
        let metadata: JobMetadata = serde_json::from_value(serde_json::json!({
            "size": 44215935,
            "slicer": "OrcaSlicer",
            "estimated_time": 34916,
            "print_start_time": 1785156102.05,
            "filament_weight_total": 162.69,
            "nozzle_diameter": 0.4,
            "thumbnails": [{ "width": 300, "height": 300, "relative_path": ".thumbs/a.png" }]
        }))
        .unwrap();

        assert_eq!(metadata.estimated_time, Some(34916.0));
        assert_eq!(metadata.filament_weight_total, Some(162.69));
        assert_eq!(metadata.thumbnails[0].relative_path, ".thumbs/a.png");
    }

    #[test]
    fn parses_metadata_without_any_of_our_fields() {
        let metadata: JobMetadata = serde_json::from_value(serde_json::json!({
            "size": 1024,
            "modified": 1785156100.0
        }))
        .unwrap();

        assert!(metadata.estimated_time.is_none());
        assert!(metadata.thumbnails.is_empty());
    }
}
