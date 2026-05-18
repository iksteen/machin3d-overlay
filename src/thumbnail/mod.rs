mod archive;
mod cache;
mod cloud;
mod job_state;
mod jobs;
mod local;
mod readiness;
mod runtime;

use std::io::Read;

use anyhow::{ensure, Context, Result};
use bytes::Bytes;

pub(crate) use runtime::ThumbnailRuntime;

const MAX_3MF_SIZE: usize = 512 * 1024 * 1024;
const MAX_THUMBNAIL_SIZE: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) enum ThumbnailStatus {
    Ready(ThumbnailImage),
    Loading(String),
    Missing(String),
}

#[derive(Debug, Clone)]
pub(crate) struct ThumbnailImage {
    pub(crate) content_type: String,
    pub(crate) bytes: Bytes,
}

fn image_content_type(content_type: Option<&str>, bytes: &[u8]) -> String {
    if bytes.starts_with(&[0xff, 0xd8]) {
        return "image/jpeg".to_owned();
    }
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return "image/png".to_owned();
    }
    if let Some(content_type) = content_type
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .filter(|value| {
            value.eq_ignore_ascii_case("image/png") || value.eq_ignore_ascii_case("image/jpeg")
        })
    {
        return content_type.to_ascii_lowercase();
    }
    "application/octet-stream".to_owned()
}

fn path_content_type(path: &str) -> Option<&'static str> {
    let path = path.to_ascii_lowercase();
    if path.ends_with(".png") {
        Some("image/png")
    } else if path.ends_with(".jpg") || path.ends_with(".jpeg") {
        Some("image/jpeg")
    } else {
        None
    }
}

fn trimmed(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn read_limited(reader: &mut dyn Read, limit: usize, label: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed to read {label}"))?;
        if read == 0 {
            break;
        }
        ensure!(
            bytes.len().saturating_add(read) <= limit,
            "{label} exceeds maximum supported size of {limit} bytes"
        );
        bytes.extend_from_slice(&buffer[..read]);
    }
    Ok(bytes)
}

fn error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}
