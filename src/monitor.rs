use anyhow::{Context, Result};
use tracing::info;

use crate::{
    cloud::{cloud_mqtt_startup, CloudSession},
    devices::{resolve_devices, DeviceRegistry, DeviceSource},
    local::{LocalDevice, LocalEndpointArg, MqttEndpoint},
    mqtt::{monitor_target, MqttTarget},
};

pub(crate) struct MonitorConfig {
    pub cloud_mqtt: MqttEndpoint,
    pub cloud_devices: Vec<String>,
    pub local_devices: Vec<LocalEndpointArg>,
    pub device: Option<String>,
}

enum MonitorTarget {
    Cloud(String),
    Local(LocalDevice),
}

impl MonitorTarget {
    fn device_id(&self) -> &str {
        match self {
            MonitorTarget::Cloud(device_id) => device_id,
            MonitorTarget::Local(device) => device.id.as_str(),
        }
    }
}

pub(crate) async fn monitor_mqtt(cloud: Option<CloudSession>, config: MonitorConfig) -> Result<()> {
    let devices = resolve_devices(
        cloud.as_ref(),
        &config.cloud_devices,
        &config.local_devices,
        &[],
    )
    .await?;
    let target = select_monitor_target(&devices, config.device.as_deref())?;
    info!(device_id = %target.device_id(), "monitoring MQTT events");

    match target {
        MonitorTarget::Cloud(device_id) => {
            let startup = cloud_mqtt_startup(
                cloud.as_ref(),
                &config.cloud_mqtt,
                std::slice::from_ref(&device_id),
            )?
            .context("cloud MQTT startup was skipped for selected cloud device")?;
            monitor_target(MqttTarget::cloud(
                startup.endpoint,
                startup.user_id,
                startup.access_token,
                vec![device_id],
            ))
            .await
        }
        MonitorTarget::Local(device) => monitor_target(MqttTarget::local(device)).await,
    }
}

fn select_monitor_target(
    registry: &DeviceRegistry,
    requested_device_id: Option<&str>,
) -> Result<MonitorTarget> {
    let requested_device_id = requested_device_id
        .map(str::trim)
        .filter(|device_id| !device_id.is_empty());
    let entry = match requested_device_id {
        Some(device_id) => registry
            .get(device_id)
            .with_context(|| format!("device `{device_id}` is not known"))?,
        None => registry.first().context("no devices are configured")?,
    };
    let device = entry.device();
    let device_id = entry.id();

    match device.source {
        DeviceSource::Cloud => Ok(MonitorTarget::Cloud(device_id.to_owned())),
        DeviceSource::Local => Ok(MonitorTarget::Local(entry.local().cloned().with_context(
            || format!("selected local device `{device_id}` is missing LAN MQTT config"),
        )?)),
    }
}

#[cfg(test)]
mod tests {
    use super::{select_monitor_target, MonitorTarget};
    use crate::{
        bambu::CloudDevice,
        devices::DeviceRegistry,
        local::{LocalDevice, LocalEndpoint},
    };

    fn cloud_device(id: &str) -> CloudDevice {
        CloudDevice {
            id: Some(id.to_owned()),
            online: Some(true),
            ..CloudDevice::default()
        }
    }

    fn local_device(id: &str) -> LocalDevice {
        LocalDevice {
            id: id.to_owned(),
            endpoint: LocalEndpoint::new("192.168.1.50", 8883, "12345678"),
        }
    }

    fn resolved() -> DeviceRegistry {
        let local = local_device("printer-b");
        DeviceRegistry::new(vec![cloud_device("printer-a")], vec![local])
    }

    #[test]
    fn monitor_selects_first_device_by_default() {
        let target = select_monitor_target(&resolved(), None).unwrap();

        assert!(matches!(target, MonitorTarget::Cloud(device_id) if device_id == "printer-a"));
    }

    #[test]
    fn monitor_can_select_local_device() {
        let target = select_monitor_target(&resolved(), Some("printer-b")).unwrap();

        assert!(matches!(target, MonitorTarget::Local(device) if device.id == "printer-b"));
    }
}
