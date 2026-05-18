use anyhow::{Context, Result};

use crate::{bambu::PrinterStatus, cloud::CloudSession, devices::DeviceRegistry};

use super::{cloud, local, ThumbnailStatus};

pub(super) async fn fetch_thumbnail(
    cloud: Option<&CloudSession>,
    registry: &DeviceRegistry,
    device_id: &str,
    report: &PrinterStatus,
) -> Result<ThumbnailStatus> {
    let entry = registry
        .get(device_id)
        .with_context(|| format!("device `{device_id}` is not known"))?;

    if let Some(local) = entry.local() {
        return local::fetch_thumbnail(device_id, local, report).await;
    }
    if entry.has_cloud_mqtt() {
        return cloud::fetch_thumbnail(cloud, device_id, report)
            .await
            .map(ThumbnailStatus::Ready);
    }

    anyhow::bail!("device `{device_id}` has no thumbnail data source")
}
