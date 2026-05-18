use std::{
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    time::Duration,
};

use anyhow::{bail, ensure, Context, Result};
use suppaftp::{types::FileType, Mode, NativeTlsConnector, NativeTlsFtpStream};
use tracing::debug;

use crate::{bambu::PrinterStatus, device_tls, local::LocalDevice};

use super::{
    archive::extract_bambu_3mf_thumbnail_archive,
    error_chain, read_limited,
    readiness::{
        local_cloud_3mf_is_preparing, local_cloud_3mf_may_still_be_preparing,
        local_cloud_3mf_prepare_message,
    },
    ThumbnailImage, ThumbnailStatus, MAX_3MF_SIZE,
};

const LOCAL_FTPS_PORT: u16 = 990;
const FTP_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalThumbnailFailure {
    Cloud3mfStillPreparing,
    Unavailable,
}

pub(super) async fn fetch_thumbnail(
    device_id: &str,
    local: &LocalDevice,
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

async fn fetch_local_3mf_thumbnail(device: &LocalDevice, filename: &str) -> Result<ThumbnailImage> {
    let device = device.clone();
    let filename = filename.to_owned();
    tokio::task::spawn_blocking(move || fetch_local_3mf_thumbnail_blocking(&device, &filename))
        .await
        .context("local FTPS thumbnail task failed")?
}

fn fetch_local_3mf_thumbnail_blocking(
    device: &LocalDevice,
    filename: &str,
) -> Result<ThumbnailImage> {
    let candidates = local_file_candidates(filename);
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
        push_unique(&mut candidates, format!("/model/{relative}"));
    }
    candidates
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

#[cfg(test)]
mod tests {
    use crate::bambu::PrinterStatus;
    use zip::result::ZipError;

    use super::{classify_local_thumbnail_failure, local_file_candidates, LocalThumbnailFailure};

    #[test]
    fn local_file_candidates_try_cache_then_model() {
        assert_eq!(
            local_file_candidates("cube.3mf"),
            vec!["/cache/cube.3mf", "/model/cube.3mf"]
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
}
