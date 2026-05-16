use std::{
    collections::HashMap,
    io::{self, Cursor, Read, Seek},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use quick_xml::{
    events::{BytesStart, Event},
    reader::Reader as XmlReader,
    XmlVersion,
};
use suppaftp::{types::FileType, Mode, NativeTlsConnector, NativeTlsFtpStream};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, warn};
use zip::{result::ZipError, ZipArchive};

use crate::{
    bambu::{PrinterStatus, Task},
    cloud::CloudSession,
    device_tls,
    devices::{DeviceRegistry, DeviceSource},
    local::LocalDevice,
    mqtt::MqttRuntime,
};

const LOCAL_FTPS_PORT: u16 = 990;
const FTP_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_3MF_SIZE: usize = 512 * 1024 * 1024;
const MAX_THUMBNAIL_SIZE: usize = 32 * 1024 * 1024;
const CLOUD_TASK_LIMIT: usize = 10;
const LOADING_RETRY_DELAY: Duration = Duration::from_secs(2);
const MISSING_RETRY_DELAY: Duration = Duration::from_secs(30);
const ROOT_RELS_PATH: &str = "_rels/.rels";
const OPC_THUMBNAIL_REL: &str =
    "http://schemas.openxmlformats.org/package/2006/relationships/metadata/thumbnail";
const BAMBU_COVER_MIDDLE_REL: &str =
    "http://schemas.bambulab.com/package/2021/cover-thumbnail-middle";
const BAMBU_COVER_SMALL_REL: &str =
    "http://schemas.bambulab.com/package/2021/cover-thumbnail-small";
const THUMBNAIL_REL_PRIORITY: &[&str] = &[
    OPC_THUMBNAIL_REL,
    BAMBU_COVER_MIDDLE_REL,
    BAMBU_COVER_SMALL_REL,
];
const FALLBACK_THUMBNAIL_NAMES: &[&str] = &[
    "Metadata/thumbnail.png",
    "Metadata/thumbnail.jpg",
    "Metadata/thumbnail.jpeg",
    "Metadata/thumbnail_small.png",
    "Metadata/plate_1.png",
    "Metadata/plate_1_small.png",
    "Metadata/top_1.png",
    "Metadata/pick_1.png",
];

#[derive(Clone)]
pub(crate) struct ThumbnailRuntime {
    inner: Arc<ThumbnailInner>,
}

struct ThumbnailInner {
    mqtt: MqttRuntime,
    cloud: Option<CloudSession>,
    registry: DeviceRegistry,
    cache: RwLock<HashMap<String, ThumbnailEntry>>,
    fetch_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

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

#[derive(Debug, Clone)]
struct ThumbnailEntry {
    task: TaskKey,
    status: ThumbnailStatus,
    retry_after: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskKey(String);

impl ThumbnailRuntime {
    pub(crate) fn new(
        mqtt: MqttRuntime,
        cloud: Option<CloudSession>,
        registry: DeviceRegistry,
    ) -> Self {
        Self {
            inner: Arc::new(ThumbnailInner {
                mqtt,
                cloud,
                registry,
                cache: RwLock::new(HashMap::new()),
                fetch_locks: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub(crate) fn start(&self) {
        let runtime = self.clone();
        tokio::spawn(async move { runtime.watch_task_changes().await });
    }

    pub(crate) async fn thumbnail(
        &self,
        requested_device_id: Option<&str>,
        _requested_task: Option<&str>,
    ) -> Result<ThumbnailStatus> {
        let Some(device_id) = self.select_device_id(requested_device_id).await? else {
            return Ok(ThumbnailStatus::Missing("no device selected".to_owned()));
        };

        self.refresh_device(&device_id).await?;
        Ok(self.cached_status(&device_id).await)
    }

    async fn watch_task_changes(&self) {
        let mut changes = self.inner.mqtt.subscribe();
        self.refresh_changed_devices().await;
        loop {
            if changes.recv().await.is_err() {
                changes = self.inner.mqtt.subscribe();
            }
            self.refresh_changed_devices().await;
        }
    }

    async fn refresh_changed_devices(&self) {
        let reports = self.inner.mqtt.reports().await;
        for device in self.inner.registry.devices() {
            let device_id = device.id.as_str();
            let Some(report) = reports.get(device_id) else {
                self.clear_device(device_id).await;
                continue;
            };
            let Some(task) = TaskKey::from_report(report) else {
                self.clear_device(device_id).await;
                continue;
            };
            if self.cache_matches(device_id, &task).await {
                continue;
            }
            if let Err(error) = self.fetch_and_cache(device_id, report, task).await {
                warn!(
                    device_id,
                    error = %error_chain(&error),
                    "failed to refresh print thumbnail"
                );
            }
        }
    }

    async fn refresh_device(&self, device_id: &str) -> Result<()> {
        let reports = self.inner.mqtt.reports().await;
        let Some(report) = reports.get(device_id) else {
            self.clear_device(device_id).await;
            return Ok(());
        };
        let Some(task) = TaskKey::from_report(report) else {
            self.clear_device(device_id).await;
            return Ok(());
        };
        if self.cache_matches(device_id, &task).await {
            return Ok(());
        }
        self.fetch_and_cache(device_id, report, task).await
    }

    async fn fetch_and_cache(
        &self,
        device_id: &str,
        report: &PrinterStatus,
        task: TaskKey,
    ) -> Result<()> {
        let fetch_lock = self.fetch_lock(device_id).await;
        let _guard = fetch_lock.lock().await;

        if self.cache_matches(device_id, &task).await {
            return Ok(());
        }

        let (status, retry_after) = match self.fetch_thumbnail(device_id, report).await {
            Ok(ThumbnailStatus::Ready(image)) => {
                debug!(device_id, "cached print thumbnail");
                (ThumbnailStatus::Ready(image), None)
            }
            Ok(ThumbnailStatus::Loading(message)) => {
                debug!(device_id, message, "print thumbnail is not ready yet");
                (
                    ThumbnailStatus::Loading(message),
                    Some(Instant::now() + LOADING_RETRY_DELAY),
                )
            }
            Ok(ThumbnailStatus::Missing(message)) => (
                ThumbnailStatus::Missing(message),
                Some(Instant::now() + MISSING_RETRY_DELAY),
            ),
            Err(error) => {
                let message = error_chain(&error);
                warn!(
                    device_id,
                    error = %message,
                    "print thumbnail is unavailable"
                );
                (
                    ThumbnailStatus::Missing(message),
                    Some(Instant::now() + MISSING_RETRY_DELAY),
                )
            }
        };

        self.inner.cache.write().await.insert(
            device_id.to_owned(),
            ThumbnailEntry {
                task,
                status,
                retry_after,
            },
        );
        Ok(())
    }

    async fn fetch_thumbnail(
        &self,
        device_id: &str,
        report: &PrinterStatus,
    ) -> Result<ThumbnailStatus> {
        let device = self
            .inner
            .registry
            .get(device_id)
            .map(|entry| entry.device())
            .with_context(|| format!("device `{device_id}` is not known"))?;

        match device.source {
            DeviceSource::Cloud => self
                .fetch_cloud_thumbnail(device_id, report)
                .await
                .map(ThumbnailStatus::Ready),
            DeviceSource::Local => self.fetch_local_thumbnail(device_id, report).await,
        }
    }

    async fn fetch_cloud_thumbnail(
        &self,
        device_id: &str,
        report: &PrinterStatus,
    ) -> Result<ThumbnailImage> {
        let cloud = self
            .inner
            .cloud
            .as_ref()
            .context("cloud thumbnail lookup requires a Bambu Cloud token")?;
        let tasks = cloud
            .client
            .tasks(&cloud.access_token, CLOUD_TASK_LIMIT, Some(device_id))
            .await
            .with_context(|| {
                format!("failed to load Bambu Cloud tasks for device `{device_id}`")
            })?;
        let task = select_cloud_task(&tasks.hits, report).with_context(|| {
            format!("no matching Bambu Cloud task found for device `{device_id}`")
        })?;
        let cover = task
            .cover
            .as_deref()
            .map(str::trim)
            .filter(|cover| !cover.is_empty())
            .context("matching Bambu Cloud task does not include a thumbnail URL")?;
        let downloaded = cloud
            .client
            .download_bytes(cover, MAX_THUMBNAIL_SIZE)
            .await
            .with_context(|| format!("failed to download Bambu Cloud thumbnail `{cover}`"))?;

        Ok(ThumbnailImage {
            content_type: image_content_type(
                downloaded.content_type.as_deref(),
                downloaded.bytes.as_ref(),
            ),
            bytes: downloaded.bytes,
        })
    }

    async fn fetch_local_thumbnail(
        &self,
        device_id: &str,
        report: &PrinterStatus,
    ) -> Result<ThumbnailStatus> {
        let local = self
            .inner
            .registry
            .get(device_id)
            .and_then(|entry| entry.local())
            .with_context(|| format!("device `{device_id}` does not have a local endpoint"))?;
        if local_cloud_3mf_is_preparing(report) {
            return Ok(ThumbnailStatus::Loading(local_cloud_3mf_prepare_message(
                report,
            )));
        }
        let filename = report
            .filename
            .as_deref()
            .map(str::trim)
            .filter(|filename| !filename.is_empty())
            .context("MQTT report does not include gcode_file for local thumbnail lookup")?;
        match fetch_local_3mf_thumbnail(local, filename, report.print_type.as_deref()).await {
            Ok(image) => Ok(ThumbnailStatus::Ready(image)),
            Err(error) if local_cloud_3mf_may_still_be_preparing(report, &error) => {
                Ok(ThumbnailStatus::Loading(format!(
                    "{}: {}",
                    local_cloud_3mf_prepare_message(report),
                    error_chain(&error)
                )))
            }
            Err(error) => Err(error).with_context(|| {
                format!("failed to fetch thumbnail from `{filename}` on local device `{device_id}`")
            }),
        }
    }

    async fn select_device_id(&self, requested_device_id: Option<&str>) -> Result<Option<String>> {
        let requested_device_id = requested_device_id
            .map(str::trim)
            .filter(|device_id| !device_id.is_empty());
        if let Some(device_id) = requested_device_id {
            ensure!(
                self.inner.registry.get(device_id).is_some(),
                "device `{device_id}` is not known"
            );
            return Ok(Some(device_id.to_owned()));
        }

        Ok(self
            .inner
            .registry
            .first()
            .map(|entry| entry.id().to_owned()))
    }

    async fn cached_status(&self, device_id: &str) -> ThumbnailStatus {
        let cache = self.inner.cache.read().await;
        match cache.get(device_id).map(|entry| &entry.status) {
            Some(status @ ThumbnailStatus::Ready(_)) => status.clone(),
            Some(status @ ThumbnailStatus::Loading(error)) => {
                debug!(device_id, error, "thumbnail is loading");
                status.clone()
            }
            Some(status @ ThumbnailStatus::Missing(error)) => {
                debug!(device_id, error, "thumbnail is unavailable");
                status.clone()
            }
            None => ThumbnailStatus::Missing("thumbnail is not available".to_owned()),
        }
    }

    async fn cache_matches(&self, device_id: &str, task: &TaskKey) -> bool {
        let cache = self.inner.cache.read().await;
        let Some(entry) = cache.get(device_id) else {
            return false;
        };
        if entry.task != *task {
            return false;
        }
        match entry.status {
            ThumbnailStatus::Ready(_) => true,
            ThumbnailStatus::Loading(_) | ThumbnailStatus::Missing(_) => entry
                .retry_after
                .is_some_and(|retry_after| retry_after > Instant::now()),
        }
    }

    async fn clear_device(&self, device_id: &str) {
        self.inner.cache.write().await.remove(device_id);
    }

    async fn fetch_lock(&self, device_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.inner.fetch_locks.lock().await;
        locks
            .entry(device_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

impl TaskKey {
    fn from_report(report: &PrinterStatus) -> Option<Self> {
        let task_id = trimmed(report.task_id.as_deref());
        let filename = trimmed(report.filename.as_deref());
        let task_name = trimmed(report.task_name.as_deref());
        if task_id.is_none() && filename.is_none() && task_name.is_none() {
            return None;
        }

        Some(Self(format!(
            "{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}",
            task_id.unwrap_or_default(),
            filename.unwrap_or_default(),
            task_name.unwrap_or_default(),
            trimmed(report.start_time.as_deref()).unwrap_or_default(),
            trimmed(report.print_type.as_deref()).unwrap_or_default()
        )))
    }
}

fn select_cloud_task<'a>(tasks: &'a [Task], report: &PrinterStatus) -> Option<&'a Task> {
    let task_id = trimmed(report.task_id.as_deref());
    if let Some(task_id) = task_id {
        if let Some(task) = tasks
            .iter()
            .find(|task| trimmed(task.id.as_deref()) == Some(task_id))
        {
            return Some(task);
        }
    }

    let task_name = trimmed(report.task_name.as_deref());
    if let Some(task_name) = task_name {
        if let Some(task) = tasks.iter().find(|task| {
            task.display_title()
                .as_deref()
                .map(str::trim)
                .filter(|title| !title.is_empty())
                .is_some_and(|title| title == task_name)
        }) {
            return Some(task);
        }
    }

    let start_time = trimmed(report.start_time.as_deref());
    if let Some(start_time) = start_time {
        if let Some(task) = tasks
            .iter()
            .find(|task| trimmed(task.start_time.as_deref()) == Some(start_time))
        {
            return Some(task);
        }
    }

    None
}

fn local_cloud_3mf_is_preparing(report: &PrinterStatus) -> bool {
    is_cloud_print(report)
        && report
            .file_prepare_percent
            .is_some_and(|percent| percent < 100.0)
}

fn local_cloud_3mf_prepare_message(report: &PrinterStatus) -> String {
    match report.file_prepare_percent {
        Some(percent) => format!("printer is still preparing cloud 3MF ({percent:.0}%)"),
        None => "printer may still be preparing cloud 3MF".to_owned(),
    }
}

fn local_cloud_3mf_may_still_be_preparing(report: &PrinterStatus, error: &anyhow::Error) -> bool {
    is_cloud_print(report) && is_incomplete_3mf_error(error)
}

fn is_cloud_print(report: &PrinterStatus) -> bool {
    report
        .print_type
        .as_deref()
        .map(str::trim)
        .is_some_and(|print_type| print_type.eq_ignore_ascii_case("cloud"))
}

fn is_incomplete_3mf_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<ZipError>()
            .is_some_and(|error| matches!(error, ZipError::InvalidArchive(_) | ZipError::Io(_)))
            || cause
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::UnexpectedEof)
    })
}

async fn fetch_local_3mf_thumbnail(
    device: &LocalDevice,
    filename: &str,
    print_type: Option<&str>,
) -> Result<ThumbnailImage> {
    let device = device.clone();
    let filename = filename.to_owned();
    let print_type = print_type.map(str::to_owned);
    tokio::task::spawn_blocking(move || {
        fetch_local_3mf_thumbnail_blocking(&device, &filename, print_type.as_deref())
    })
    .await
    .context("local FTPS thumbnail task failed")?
}

fn fetch_local_3mf_thumbnail_blocking(
    device: &LocalDevice,
    filename: &str,
    print_type: Option<&str>,
) -> Result<ThumbnailImage> {
    let candidates = local_file_candidates(filename, print_type);
    if candidates.is_empty() {
        bail!("no local file candidates were generated");
    }

    // suppaftp exposes the post-handshake TCP stream, but not the peer certificate.
    // Probe before login so the access code is only sent to the expected device.
    verify_local_ftps_device_id(device)?;
    fetch_local_3mf_thumbnail_with_mode(device, &candidates, Mode::Passive)
}

fn fetch_local_3mf_thumbnail_with_mode(
    device: &LocalDevice,
    candidates: &[String],
    mode: Mode,
) -> Result<ThumbnailImage> {
    let mut errors = Vec::new();
    for path in candidates {
        match retrieve_thumbnail_from_candidate(device, mode, path) {
            Ok(image) => return Ok(image),
            Err(error) => {
                let message = error_chain(&error);
                debug!(
                    path,
                    error = %message,
                    "local FTPS thumbnail candidate failed"
                );
                errors.push(format!("`{path}`: {message}"));
            }
        }
    }
    bail!(
        "all local FTPS thumbnail candidates failed: {}",
        errors.join("; ")
    )
}

fn retrieve_thumbnail_from_candidate(
    device: &LocalDevice,
    mode: Mode,
    path: &str,
) -> Result<ThumbnailImage> {
    let mut client = connect_local_ftps(device, mode)?;
    retrieve_thumbnail(&mut client, path)
}

fn connect_local_ftps(device: &LocalDevice, mode: Mode) -> Result<NativeTlsFtpStream> {
    let address = local_ftps_address(device);
    let connector = NativeTlsConnector::from(device_tls::native_connector()?);
    let mut client = NativeTlsFtpStream::connect_secure_implicit(
        address.as_str(),
        connector,
        device.endpoint.host(),
    )
    .with_context(|| format!("failed to connect to local FTPS at {address}"))?;
    client
        .get_ref()
        .set_read_timeout(Some(FTP_TIMEOUT))
        .context("failed to set local FTPS read timeout")?;
    client
        .get_ref()
        .set_write_timeout(Some(FTP_TIMEOUT))
        .context("failed to set local FTPS write timeout")?;
    client.set_passive_nat_workaround(true);
    client.set_mode(mode);
    client
        .login("bblp", device.endpoint.access_code())
        .context("local FTPS login failed")?;
    client
        .transfer_type(FileType::Binary)
        .context("failed to set local FTPS binary transfer mode")?;
    Ok(client)
}

fn verify_local_ftps_device_id(device: &LocalDevice) -> Result<()> {
    let address = local_ftps_address(device);
    let address = resolve_socket_addr(&address)?;
    let tcp = TcpStream::connect_timeout(&address, FTP_TIMEOUT)
        .with_context(|| format!("failed to connect to local FTPS at {address}"))?;
    tcp.set_read_timeout(Some(FTP_TIMEOUT))
        .context("failed to set local FTPS preflight read timeout")?;
    tcp.set_write_timeout(Some(FTP_TIMEOUT))
        .context("failed to set local FTPS preflight write timeout")?;
    let socket = device_tls::native_connector()?
        .connect(device.endpoint.host(), tcp)
        .with_context(|| format!("failed local FTPS TLS handshake at {address}"))?;
    let certificate = socket
        .peer_certificate()
        .context("failed to read local FTPS certificate")?
        .context("local FTPS did not send a certificate")?;
    let certificate_device_id = device_tls::certificate_device_id(&certificate)
        .context("local FTPS certificate did not include a device ID")?;
    ensure!(
        certificate_device_id == device.id,
        "local FTPS certificate is for device `{certificate_device_id}`, not `{}`",
        device.id
    );
    Ok(())
}

fn retrieve_thumbnail(client: &mut NativeTlsFtpStream, path: &str) -> Result<ThumbnailImage> {
    let mut stream = client
        .retr_as_stream(path)
        .with_context(|| format!("local FTPS RETR `{path}` failed"))?;
    let archive = read_limited(&mut stream, MAX_3MF_SIZE, "local 3MF download")
        .with_context(|| format!("failed to download local 3MF `{path}`"))?;
    client
        .finalize_retr_stream(stream)
        .with_context(|| format!("failed to finalize local FTPS RETR `{path}`"))?;
    extract_bambu_3mf_thumbnail_archive(archive)
        .with_context(|| format!("failed to read thumbnail from local 3MF `{path}`"))
}

fn local_ftps_address(device: &LocalDevice) -> String {
    if device.endpoint.host().contains(':') {
        format!("[{}]:{LOCAL_FTPS_PORT}", device.endpoint.host())
    } else {
        format!("{}:{LOCAL_FTPS_PORT}", device.endpoint.host())
    }
}

fn resolve_socket_addr(address: &str) -> Result<SocketAddr> {
    address
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve local FTPS address `{address}`"))?
        .next()
        .with_context(|| format!("local FTPS address `{address}` did not resolve"))
}

fn extract_bambu_3mf_thumbnail_archive(archive: Vec<u8>) -> Result<ThumbnailImage> {
    let mut archive =
        ZipArchive::new(Cursor::new(archive)).context("failed to read local 3MF as ZIP archive")?;
    let thumbnail = select_thumbnail_entry(&mut archive)?
        .context("3MF did not include a supported thumbnail image")?;
    read_thumbnail_entry(&mut archive, &thumbnail)
}

fn select_thumbnail_entry<R: Read + Seek>(archive: &mut ZipArchive<R>) -> Result<Option<String>> {
    // 3MF stores the authoritative package thumbnail in the root relationship
    // file. Only fall back to file-name heuristics when that relationship is
    // absent, and keep those fallbacks explicitly ordered.
    if let Some(relationships) = read_archive_string(archive, ROOT_RELS_PATH)? {
        let relationships = parse_thumbnail_relationships(&relationships)?;
        for rel_type in THUMBNAIL_REL_PRIORITY {
            for relationship in relationships
                .iter()
                .filter(|relationship| relationship.rel_type == *rel_type)
            {
                let Some(target) = normalize_archive_path(&relationship.target) else {
                    continue;
                };
                if is_supported_thumbnail_entry(&target)
                    && archive.index_for_name(&target).is_some()
                {
                    return Ok(Some(target));
                }
            }
        }
    }

    for name in FALLBACK_THUMBNAIL_NAMES {
        if archive.index_for_name(name).is_some() {
            return Ok(Some((*name).to_owned()));
        }
    }

    let mut names = archive
        .file_names()
        .filter(|name| is_supported_thumbnail_entry(name))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    names.sort_unstable();
    Ok(names.into_iter().next())
}

fn read_thumbnail_entry<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    name: &str,
) -> Result<ThumbnailImage> {
    let mut file = archive
        .by_name(name)
        .with_context(|| format!("failed to open thumbnail entry `{name}`"))?;
    ensure!(
        file.size() <= MAX_THUMBNAIL_SIZE as u64,
        "thumbnail entry `{name}` exceeds maximum supported size of {MAX_THUMBNAIL_SIZE} bytes"
    );
    let bytes = read_limited(&mut file, MAX_THUMBNAIL_SIZE, "thumbnail entry data")
        .with_context(|| format!("failed to read thumbnail entry `{name}`"))?;
    ensure!(!bytes.is_empty(), "thumbnail entry `{name}` is empty");
    debug!(
        entry = %name,
        size = bytes.len(),
        "loaded thumbnail from local 3MF"
    );
    Ok(ThumbnailImage {
        content_type: image_content_type(path_content_type(name), &bytes),
        bytes: Bytes::from(bytes),
    })
}

fn read_archive_string<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    name: &str,
) -> Result<Option<String>> {
    let mut file = match archive.by_name(name) {
        Ok(file) => file,
        Err(ZipError::FileNotFound) => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to open archive entry `{name}`"))
        }
    };
    let mut text = String::new();
    file.read_to_string(&mut text)
        .with_context(|| format!("failed to read archive entry `{name}`"))?;
    Ok(Some(text))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ThumbnailRelationship {
    rel_type: String,
    target: String,
}

fn parse_thumbnail_relationships(xml: &str) -> Result<Vec<ThumbnailRelationship>> {
    let mut reader = XmlReader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut relationships = Vec::new();
    loop {
        match reader
            .read_event()
            .context("failed to parse 3MF relationships")?
        {
            Event::Empty(element) | Event::Start(element) => {
                if element.local_name().as_ref() == b"Relationship" {
                    if let Some(relationship) = parse_thumbnail_relationship(&reader, &element)? {
                        relationships.push(relationship);
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(relationships)
}

fn parse_thumbnail_relationship(
    reader: &XmlReader<&[u8]>,
    element: &BytesStart<'_>,
) -> Result<Option<ThumbnailRelationship>> {
    let mut rel_type = None;
    let mut target = None;
    for attribute in element.attributes().with_checks(false) {
        let attribute = attribute.context("failed to parse 3MF relationship attribute")?;
        let value = attribute
            .decoded_and_normalized_value(XmlVersion::Implicit1_0, reader.decoder())
            .context("failed to decode 3MF relationship attribute")?
            .into_owned();
        match attribute.key.as_ref() {
            b"Type" => rel_type = Some(value),
            b"Target" => target = Some(value),
            _ => {}
        }
    }
    Ok(match (rel_type, target) {
        (Some(rel_type), Some(target)) => Some(ThumbnailRelationship { rel_type, target }),
        _ => None,
    })
}

fn normalize_archive_path(path: &str) -> Option<String> {
    let path = path.trim().replace('\\', "/");
    let path = path.trim_start_matches('/');
    if path.is_empty() || path.contains('\0') || path.split('/').any(|part| part == "..") {
        return None;
    }
    Some(path.to_owned())
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

fn is_supported_thumbnail_entry(name: &str) -> bool {
    let normalized = name.replace('\\', "/").to_ascii_lowercase();
    match normalized.as_str() {
        "metadata/thumbnail.png"
        | "metadata/thumbnail.jpg"
        | "metadata/thumbnail.jpeg"
        | "metadata/thumbnail_small.png"
        | "metadata/plate_1.png"
        | "metadata/top_1.png" => true,
        _ if normalized.starts_with("metadata/")
            && (normalized.ends_with(".png")
                || normalized.ends_with(".jpg")
                || normalized.ends_with(".jpeg")) =>
        {
            true
        }
        _ if normalized.ends_with(".png")
            || normalized.ends_with(".jpg")
            || normalized.ends_with(".jpeg") =>
        {
            true
        }
        _ => false,
    }
}

fn local_file_candidates(filename: &str, print_type: Option<&str>) -> Vec<String> {
    let filename = filename.trim().replace('\\', "/");
    if filename.is_empty()
        || filename.contains('\0')
        || filename.contains('\r')
        || filename.contains('\n')
    {
        return Vec::new();
    }

    let relative = filename.trim_start_matches('/');
    let mut candidates = Vec::new();
    if filename.starts_with('/')
        || relative.starts_with("cache/")
        || relative.starts_with("sdcard/")
    {
        push_unique(&mut candidates, format!("/{relative}"));
    } else {
        match print_type_root(print_type) {
            Some(root) => push_unique(&mut candidates, format!("{root}/{relative}")),
            None => {
                push_unique(&mut candidates, format!("/cache/{relative}"));
                push_unique(&mut candidates, format!("/sdcard/{relative}"));
            }
        }
    }
    candidates
}

fn print_type_root(print_type: Option<&str>) -> Option<&'static str> {
    match print_type.map(str::trim) {
        Some(value) if value.eq_ignore_ascii_case("cloud") => Some("/cache"),
        Some(value) if value.eq_ignore_ascii_case("local") => Some("/sdcard"),
        _ => None,
    }
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
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

fn error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Cursor, Write},
        time::{Duration, Instant},
    };

    use super::{
        extract_bambu_3mf_thumbnail_archive, is_supported_thumbnail_entry,
        local_cloud_3mf_is_preparing, local_cloud_3mf_may_still_be_preparing,
        local_file_candidates, select_cloud_task, TaskKey, ThumbnailEntry, ThumbnailRuntime,
        ThumbnailStatus, BAMBU_COVER_MIDDLE_REL, OPC_THUMBNAIL_REL,
    };
    use crate::{
        bambu::{CloudDevice, PrinterStatus, Task},
        devices::DeviceRegistry,
        mqtt::MqttRuntime,
    };
    use zip::{result::ZipError, write::SimpleFileOptions, CompressionMethod, ZipWriter};

    #[test]
    fn task_key_tracks_the_active_print_identity() {
        let report = PrinterStatus {
            task_id: Some("task-1".to_owned()),
            filename: Some("cube.3mf".to_owned()),
            task_name: Some("Cube".to_owned()),
            start_time: Some("2026-01-01".to_owned()),
            ..PrinterStatus::default()
        };

        assert!(TaskKey::from_report(&report).is_some());
        assert_eq!(TaskKey::from_report(&PrinterStatus::default()), None);
    }

    #[tokio::test]
    async fn missing_thumbnail_cache_throttles_until_retry_time() {
        let runtime = ThumbnailRuntime::new(
            MqttRuntime::new(),
            None,
            DeviceRegistry::new(
                vec![CloudDevice {
                    id: Some("printer-a".to_owned()),
                    ..CloudDevice::default()
                }],
                Vec::new(),
            ),
        );
        let task = TaskKey("task".to_owned());
        runtime.inner.cache.write().await.insert(
            "printer-a".to_owned(),
            ThumbnailEntry {
                task: task.clone(),
                status: ThumbnailStatus::Missing("missing".to_owned()),
                retry_after: Some(Instant::now() + Duration::from_secs(30)),
            },
        );

        assert!(runtime.cache_matches("printer-a", &task).await);
    }

    #[test]
    fn local_cloud_3mf_prepare_percent_defers_thumbnail_fetch() {
        let report = PrinterStatus {
            print_type: Some("cloud".to_owned()),
            file_prepare_percent: Some(99.0),
            ..PrinterStatus::default()
        };
        assert!(local_cloud_3mf_is_preparing(&report));

        let report = PrinterStatus {
            print_type: Some("cloud".to_owned()),
            file_prepare_percent: Some(100.0),
            ..PrinterStatus::default()
        };
        assert!(!local_cloud_3mf_is_preparing(&report));

        let report = PrinterStatus {
            print_type: Some("local".to_owned()),
            file_prepare_percent: Some(99.0),
            ..PrinterStatus::default()
        };
        assert!(!local_cloud_3mf_is_preparing(&report));
    }

    #[test]
    fn invalid_cloud_3mf_is_treated_as_still_preparing() {
        let error = anyhow::Error::new(ZipError::InvalidArchive(
            "could not find central directory".into(),
        ));
        let report = PrinterStatus {
            print_type: Some("cloud".to_owned()),
            file_prepare_percent: Some(100.0),
            ..PrinterStatus::default()
        };
        assert!(local_cloud_3mf_may_still_be_preparing(&report, &error));

        let report = PrinterStatus {
            print_type: Some("local".to_owned()),
            file_prepare_percent: Some(100.0),
            ..PrinterStatus::default()
        };
        assert!(!local_cloud_3mf_may_still_be_preparing(&report, &error));
    }

    #[test]
    fn cloud_task_selection_prefers_task_id_then_title() {
        let tasks = vec![
            Task {
                id: Some("old".to_owned()),
                title: Some("Cube".to_owned()),
                ..Task::default()
            },
            Task {
                id: Some("task-1".to_owned()),
                title: Some("Other".to_owned()),
                ..Task::default()
            },
        ];
        let report = PrinterStatus {
            task_id: Some("task-1".to_owned()),
            task_name: Some("Cube".to_owned()),
            ..PrinterStatus::default()
        };

        assert_eq!(
            select_cloud_task(&tasks, &report).unwrap().id.as_deref(),
            Some("task-1")
        );

        let report = PrinterStatus {
            task_name: Some("Cube".to_owned()),
            ..PrinterStatus::default()
        };
        assert_eq!(
            select_cloud_task(&tasks, &report).unwrap().id.as_deref(),
            Some("old")
        );

        let report = PrinterStatus {
            start_time: Some("2026-01-01T10:00:00Z".to_owned()),
            ..PrinterStatus::default()
        };
        let tasks = vec![Task {
            id: Some("start-time-match".to_owned()),
            start_time: Some("2026-01-01T10:00:00Z".to_owned()),
            ..Task::default()
        }];
        assert_eq!(
            select_cloud_task(&tasks, &report).unwrap().id.as_deref(),
            Some("start-time-match")
        );

        let report = PrinterStatus {
            task_id: Some("missing".to_owned()),
            task_name: Some("No match".to_owned()),
            start_time: Some("no-match".to_owned()),
            ..PrinterStatus::default()
        };
        assert!(select_cloud_task(&tasks, &report).is_none());
    }

    #[test]
    fn local_file_candidates_try_print_cache_first() {
        assert_eq!(
            local_file_candidates("cube.3mf", None),
            vec!["/cache/cube.3mf", "/sdcard/cube.3mf"]
        );
        assert_eq!(
            local_file_candidates("cube.3mf", Some("cloud")),
            vec!["/cache/cube.3mf"]
        );
        assert_eq!(
            local_file_candidates("cube.3mf", Some("local")),
            vec!["/sdcard/cube.3mf"]
        );
        assert_eq!(
            local_file_candidates("/sdcard/cube.3mf", Some("cloud")),
            vec!["/sdcard/cube.3mf"]
        );
        assert_eq!(
            local_file_candidates("/cache/cube.3mf", Some("local")),
            vec!["/cache/cube.3mf"]
        );
    }

    #[test]
    fn archive_thumbnail_uses_root_thumbnail_relationship() {
        let thumbnail = b"\x89PNG\r\n\x1a\nthumbnail";
        let relationships = relationship_xml(&[(OPC_THUMBNAIL_REL, "/Metadata/plate_1.png")]);
        let archive = make_archive(&[
            ("_rels/.rels", relationships.as_bytes()),
            ("Metadata/pick_1.png", b"wrong"),
            ("Metadata/plate_1.png", thumbnail),
        ]);

        let image = extract_bambu_3mf_thumbnail_archive(archive).unwrap();

        assert_eq!(image.content_type, "image/png");
        assert_eq!(image.bytes.as_ref(), thumbnail);
    }

    #[test]
    fn supported_thumbnail_entry_recognizes_bambu_thumbnail_names() {
        assert!(is_supported_thumbnail_entry("Metadata/thumbnail.png"));
        assert!(is_supported_thumbnail_entry("Metadata/plate_1.png"));
        assert!(is_supported_thumbnail_entry("foo/model.png"));
        assert!(!is_supported_thumbnail_entry("Metadata/model.xml"));
    }

    #[test]
    fn archive_thumbnail_uses_bambu_middle_relationship() {
        let thumbnail = b"\x89PNG\r\n\x1a\nthumbnail";
        let relationships = relationship_xml(&[(BAMBU_COVER_MIDDLE_REL, "/Metadata/plate_2.png")]);
        let archive = make_archive(&[
            ("_rels/.rels", relationships.as_bytes()),
            ("Metadata/plate_1.png", b"wrong"),
            ("Metadata/plate_2.png", thumbnail),
        ]);

        let image = extract_bambu_3mf_thumbnail_archive(archive).unwrap();

        assert_eq!(image.content_type, "image/png");
        assert_eq!(image.bytes.as_ref(), thumbnail);
    }

    #[test]
    fn archive_thumbnail_falls_back_by_explicit_priority() {
        let thumbnail = b"\x89PNG\r\n\x1a\nthumbnail";
        let archive = make_archive(&[
            ("Metadata/top_1.png", b"wrong"),
            ("Metadata/plate_1.png", thumbnail),
        ]);

        let image = extract_bambu_3mf_thumbnail_archive(archive).unwrap();

        assert_eq!(image.content_type, "image/png");
        assert_eq!(image.bytes.as_ref(), thumbnail.as_slice());
    }

    #[test]
    fn archive_thumbnail_falls_back_to_sorted_supported_entries() {
        let archive = make_archive(&[("z/cover.png", b"wrong"), ("a/cover.png", b"right")]);

        let image = extract_bambu_3mf_thumbnail_archive(archive).unwrap();

        assert_eq!(image.bytes.as_ref(), b"right");
    }

    fn relationship_xml(relationships: &[(&str, &str)]) -> String {
        let mut xml = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">"#,
        );
        for (rel_type, target) in relationships {
            xml.push_str(&format!(
                r#"<Relationship Target="{target}" Id="rel" Type="{rel_type}"/>"#
            ));
        }
        xml.push_str("</Relationships>");
        xml
    }

    fn make_archive(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let cursor = Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        for (name, data) in entries {
            writer.start_file(*name, options).unwrap();
            writer.write_all(data).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }
}
