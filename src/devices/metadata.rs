use anyhow::Result;

use crate::{
    bambu::CloudDevice,
    cloud::{bound_cloud_devices, CloudSession},
};

pub(super) struct BindCatalog<'a> {
    cloud: Option<&'a CloudSession>,
    devices: Option<Vec<CloudDevice>>,
}

impl<'a> BindCatalog<'a> {
    pub(super) fn new(cloud: Option<&'a CloudSession>, devices: Option<Vec<CloudDevice>>) -> Self {
        Self { cloud, devices }
    }

    pub(super) async fn load_device_from_cloud(
        &mut self,
        device_id: &str,
    ) -> Result<Option<CloudDevice>> {
        if self.devices.is_none() {
            self.devices = Some(bound_cloud_devices(self.cloud).await?);
        }

        Ok(self
            .devices
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .find(|device| device.id.as_deref().map(str::trim) == Some(device_id))
            .cloned())
    }
}
