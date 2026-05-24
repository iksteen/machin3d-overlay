//! Per-device video source.
//!
//! Each vendor's video source carries everything its worker needs (host,
//! credentials, TLS connector, remembered endpoints). `VideoStreams` only
//! sees the enum; it never matches on `Backend` or reaches into vendor
//! fields. Dispatch into the actual worker lives in `VideoSource::run`.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::Mutex;
use tokio_native_tls::TlsConnector;

use crate::{
    backend::Backend,
    device_tls,
    devices::DeviceEntry,
    secret::Secret,
    service::ShutdownReceiver,
    snapmaker::{SnapMqttCreds, SnapmakerEndpoint},
};

use super::{
    connection::run_stream_worker, endpoint::VideoEndpoint, snapmaker::run_snapmaker_stream_worker,
    stream::DeviceVideoStream,
};

pub(crate) enum VideoSource {
    Bambu(BambuVideoSource),
    Snapmaker(SnapmakerVideoSource),
}

pub(crate) struct BambuVideoSource {
    pub(crate) device_id: String,
    pub(crate) endpoints: Vec<VideoEndpoint>,
    pub(crate) access_code: Secret<String>,
    pub(crate) tls: TlsConnector,
    pub(crate) remembered: Mutex<Option<VideoEndpoint>>,
}

pub(crate) struct SnapmakerVideoSource {
    pub(crate) device_id: String,
    pub(crate) endpoint: SnapmakerEndpoint,
    pub(crate) mtls: Option<SnapMqttCreds>,
}

impl VideoSource {
    pub(super) fn device_id(&self) -> &str {
        match self {
            VideoSource::Bambu(source) => &source.device_id,
            VideoSource::Snapmaker(source) => &source.device_id,
        }
    }

    pub(super) async fn run(
        self: Arc<Self>,
        stream: Arc<DeviceVideoStream>,
        shutdown: ShutdownReceiver,
    ) {
        match &*self {
            VideoSource::Bambu(source) => run_stream_worker(source, stream, shutdown).await,
            VideoSource::Snapmaker(source) => {
                run_snapmaker_stream_worker(source, stream, shutdown).await
            }
        }
    }
}

/// Build a `VideoSource` for `entry` if its backend has the configuration to
/// stream. Returns `None` when the device has no video capability (no
/// configured endpoint, missing credentials, etc.).
pub(crate) fn video_source_for(
    entry: &DeviceEntry,
    bambu_endpoints: Option<&Vec<VideoEndpoint>>,
    tls: &TlsConnector,
) -> Option<VideoSource> {
    match entry.backend() {
        Backend::Bambu => {
            let endpoints = bambu_endpoints?.clone();
            if endpoints.is_empty() {
                return None;
            }
            let access_code = entry.access_code()?;
            Some(VideoSource::Bambu(BambuVideoSource {
                device_id: entry.id().to_owned(),
                endpoints,
                access_code: Secret::new(access_code.to_owned()),
                tls: tls.clone(),
                remembered: Mutex::new(None),
            }))
        }
        Backend::Snapmaker => {
            let endpoint = entry.snapmaker_endpoint()?.clone();
            let mtls = entry.snapmaker_mtls().cloned();
            Some(VideoSource::Snapmaker(SnapmakerVideoSource {
                device_id: entry.id().to_owned(),
                endpoint,
                mtls,
            }))
        }
    }
}

/// Collect a `(device_id → VideoSource)` map from the registry. The Bambu
/// endpoint catalog is supplied separately because it is built from
/// `--bbl-video-device` flags plus startup probes; Snapmaker derives its URL
/// from the Moonraker endpoint already on the registry entry.
pub(crate) fn collect_sources(
    registry: &crate::devices::DeviceRegistry,
    bambu_endpoints: &std::collections::HashMap<String, Vec<VideoEndpoint>>,
) -> Result<std::collections::HashMap<String, Arc<VideoSource>>> {
    let tls = device_tls::tokio_connector().context("video TLS connector")?;
    let mut sources = std::collections::HashMap::new();
    for entry in registry.entries() {
        if let Some(source) = video_source_for(entry, bambu_endpoints.get(entry.id()), &tls) {
            sources.insert(entry.id().to_owned(), Arc::new(source));
        }
    }
    Ok(sources)
}
