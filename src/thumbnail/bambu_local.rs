use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    time::Duration,
};

use anyhow::{bail, ensure, Context, Result};
use suppaftp::{types::FileType, Mode, NativeTlsConnector, NativeTlsFtpStream};
use tracing::debug;
use uuid::Uuid;
use zip::result::ZipError;

use crate::bambu::{device_tls, local::BambuLocalDevice, PrinterStatus};

use super::{
    archive::extract_bambu_3mf_thumbnail_reader, error_chain, ThumbnailImage, ThumbnailStatus,
    MAX_3MF_SIZE,
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

const LOCAL_FTPS_PORT: u16 = 990;
const FTP_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalThumbnailFailure {
    Cloud3mfStillPreparing,
    Unavailable,
}

pub(super) async fn fetch_thumbnail(
    device_id: &str,
    local: &BambuLocalDevice,
    report: &PrinterStatus,
) -> Result<ThumbnailStatus> {
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
    match fetch_local_3mf_thumbnail(local, filename).await {
        Ok(image) => Ok(ThumbnailStatus::Ready(image)),
        Err(error) => match classify_local_thumbnail_failure(report, &error) {
            LocalThumbnailFailure::Cloud3mfStillPreparing => Ok(ThumbnailStatus::Loading(format!(
                "{}: {}",
                local_cloud_3mf_prepare_message(report),
                error_chain(&error)
            ))),
            LocalThumbnailFailure::Unavailable => Err(error).with_context(|| {
                format!("failed to fetch thumbnail from `{filename}` on local device `{device_id}`")
            }),
        },
    }
}

fn classify_local_thumbnail_failure(
    report: &PrinterStatus,
    error: &anyhow::Error,
) -> LocalThumbnailFailure {
    if local_cloud_3mf_may_still_be_preparing(report, error) {
        LocalThumbnailFailure::Cloud3mfStillPreparing
    } else {
        LocalThumbnailFailure::Unavailable
    }
}

async fn fetch_local_3mf_thumbnail(
    device: &BambuLocalDevice,
    filename: &str,
) -> Result<ThumbnailImage> {
    let device = device.clone();
    let filename = filename.to_owned();
    tokio::task::spawn_blocking(move || fetch_local_3mf_thumbnail_blocking(&device, &filename))
        .await
        .context("local FTPS thumbnail task failed")?
}

fn fetch_local_3mf_thumbnail_blocking(
    device: &BambuLocalDevice,
    filename: &str,
) -> Result<ThumbnailImage> {
    let candidates = local_file_candidates(filename);
    if candidates.is_empty() {
        bail!("no local file candidates were generated");
    }

    // FTPS authenticates the printer via the BBL CA chain only; the cert CN
    // matching `device.id` is not re-checked here. suppaftp 8.0.3 does not
    // expose the peer certificate after the handshake, and the bambu device
    // certs are X.509 v1 which rules out swapping to rustls for a custom
    // verifier. The startup MQTT probe already verifies CN at startup; the
    // home-LAN threat model treats anything beyond that as out of scope.
    fetch_local_3mf_thumbnail_with_mode(device, &candidates, Mode::Passive)
}

fn fetch_local_3mf_thumbnail_with_mode(
    device: &BambuLocalDevice,
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
    device: &BambuLocalDevice,
    mode: Mode,
    path: &str,
) -> Result<ThumbnailImage> {
    let mut client = connect_local_ftps(device, mode)?;
    retrieve_thumbnail(&mut client, path)
}

fn connect_local_ftps(device: &BambuLocalDevice, mode: Mode) -> Result<NativeTlsFtpStream> {
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

fn retrieve_thumbnail(client: &mut NativeTlsFtpStream, path: &str) -> Result<ThumbnailImage> {
    let mut stream = client
        .retr_as_stream(path)
        .with_context(|| format!("local FTPS RETR `{path}` failed"))?;
    let mut archive = download_3mf_to_temp_file(&mut stream, MAX_3MF_SIZE, "local 3MF download")
        .with_context(|| format!("failed to download local 3MF `{path}`"))?;
    client
        .finalize_retr_stream(stream)
        .with_context(|| format!("failed to finalize local FTPS RETR `{path}`"))?;
    archive
        .file
        .seek(SeekFrom::Start(0))
        .with_context(|| format!("failed to rewind local 3MF `{path}`"))?;
    extract_bambu_3mf_thumbnail_reader(&mut archive.file)
        .with_context(|| format!("failed to read thumbnail from local 3MF `{path}`"))
}

fn download_3mf_to_temp_file(
    reader: &mut dyn Read,
    limit: usize,
    label: &str,
) -> Result<TempArchive> {
    let path = temporary_archive_path();
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);

    let mut file = options
        .open(&path)
        .with_context(|| format!("failed to create temporary local 3MF {}", path.display()))?;
    let mut bytes = 0_usize;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed to read {label}"))?;
        if read == 0 {
            break;
        }
        ensure!(
            bytes.saturating_add(read) <= limit,
            "{label} exceeds maximum supported size of {limit} bytes"
        );
        file.write_all(&buffer[..read])
            .with_context(|| format!("failed to write temporary local 3MF {}", path.display()))?;
        bytes += read;
    }
    file.flush()
        .with_context(|| format!("failed to flush temporary local 3MF {}", path.display()))?;
    Ok(TempArchive { file, path })
}

fn temporary_archive_path() -> PathBuf {
    env::temp_dir().join(format!("machin3d-overlay-{}.3mf", Uuid::new_v4()))
}

struct TempArchive {
    file: File,
    path: PathBuf,
}

impl Drop for TempArchive {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn local_ftps_address(device: &BambuLocalDevice) -> String {
    if device.endpoint.host().contains(':') {
        format!("[{}]:{LOCAL_FTPS_PORT}", device.endpoint.host())
    } else {
        format!("{}:{LOCAL_FTPS_PORT}", device.endpoint.host())
    }
}

fn local_file_candidates(filename: &str) -> Vec<String> {
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
    if filename.starts_with('/') || relative.starts_with("cache/") || relative.starts_with("model/")
    {
        push_unique(&mut candidates, format!("/{relative}"));
    } else {
        push_unique(&mut candidates, format!("/cache/{relative}"));
        push_unique(&mut candidates, format!("/{relative}"));
        push_unique(&mut candidates, format!("/model/{relative}"));
    }
    candidates
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
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

#[cfg(test)]
mod tests {
    use crate::bambu::PrinterStatus;
    use zip::result::ZipError;

    use super::{
        classify_local_thumbnail_failure, local_cloud_3mf_is_preparing,
        local_cloud_3mf_may_still_be_preparing, local_file_candidates, LocalThumbnailFailure,
    };

    #[test]
    fn local_file_candidates_try_cache_then_model() {
        assert_eq!(
            local_file_candidates("cube.3mf"),
            vec!["/cache/cube.3mf", "/cube.3mf", "/model/cube.3mf"]
        );
        assert_eq!(
            local_file_candidates("/model/cube.3mf"),
            vec!["/model/cube.3mf"]
        );
        assert_eq!(
            local_file_candidates("/cache/cube.3mf"),
            vec!["/cache/cube.3mf"]
        );
    }

    #[test]
    fn local_thumbnail_failure_classifies_incomplete_cloud_3mf_as_loading() {
        let error = anyhow::Error::new(ZipError::InvalidArchive(
            "could not find central directory".into(),
        ));
        let cloud_report = PrinterStatus {
            print_type: Some("cloud".to_owned()),
            file_prepare_percent: Some(100.0),
            ..PrinterStatus::default()
        };
        let local_report = PrinterStatus {
            print_type: Some("local".to_owned()),
            file_prepare_percent: Some(100.0),
            ..PrinterStatus::default()
        };

        assert_eq!(
            classify_local_thumbnail_failure(&cloud_report, &error),
            LocalThumbnailFailure::Cloud3mfStillPreparing
        );
        assert_eq!(
            classify_local_thumbnail_failure(&local_report, &error),
            LocalThumbnailFailure::Unavailable
        );
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
}
