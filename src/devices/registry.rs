use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::{
    bambu::{CloudDevice, PrinterStatus},
    local::LocalDevice,
    video::VideoEndpoint,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DeviceSource {
    Cloud,
    Local,
}

#[derive(Debug, Clone)]
pub(crate) struct KnownDevice {
    pub(crate) id: String,
    pub(crate) name: Option<String>,
    pub(crate) online: Option<bool>,
    pub(crate) access_code: Option<String>,
    pub(crate) status: PrinterStatus,
}

impl KnownDevice {
    pub(crate) fn from_cloud(device: CloudDevice) -> Option<Self> {
        Some(Self {
            id: non_empty_string(device.id)?,
            name: device.name,
            online: device.online,
            access_code: device.access_code,
            status: device.status,
        })
    }

    pub(crate) fn from_local(device: &LocalDevice) -> Self {
        Self {
            id: device.id.clone(),
            name: device.endpoint.name.clone(),
            online: Some(true),
            access_code: Some(device.endpoint.access_code.clone()),
            status: PrinterStatus::default(),
        }
    }

    pub(crate) fn has_access_code(&self) -> bool {
        self.access_code
            .as_deref()
            .is_some_and(|code| !code.trim().is_empty())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DeviceEntry {
    device: KnownDevice,
    capabilities: DeviceCapabilities,
}

#[derive(Debug, Clone, Default)]
struct DeviceCapabilities {
    cloud_mqtt: bool,
    local_mqtt: Option<LocalDevice>,
    explicit_video: Option<VideoEndpoint>,
}

#[derive(Debug, Clone)]
pub(crate) struct DeviceRegistry {
    entries: Vec<DeviceEntry>,
    entry_by_id: HashMap<String, usize>,
}

impl DeviceEntry {
    fn from_cloud(device: KnownDevice) -> Self {
        Self {
            device,
            capabilities: DeviceCapabilities {
                cloud_mqtt: true,
                ..DeviceCapabilities::default()
            },
        }
    }

    fn from_local(local: LocalDevice) -> Self {
        Self {
            device: KnownDevice::from_local(&local),
            capabilities: DeviceCapabilities {
                local_mqtt: Some(local),
                ..DeviceCapabilities::default()
            },
        }
    }

    pub(crate) fn device(&self) -> &KnownDevice {
        &self.device
    }

    pub(super) fn device_mut(&mut self) -> &mut KnownDevice {
        &mut self.device
    }

    pub(crate) fn id(&self) -> &str {
        self.device.id.as_str()
    }

    pub(crate) fn source(&self) -> DeviceSource {
        if self.local().is_some() {
            DeviceSource::Local
        } else {
            DeviceSource::Cloud
        }
    }

    pub(crate) fn has_cloud_mqtt(&self) -> bool {
        self.capabilities.cloud_mqtt
    }

    pub(crate) fn local(&self) -> Option<&LocalDevice> {
        self.capabilities.local_mqtt.as_ref()
    }

    pub(crate) fn explicit_video(&self) -> Option<&VideoEndpoint> {
        self.capabilities.explicit_video.as_ref()
    }

    pub(super) fn set_explicit_video(&mut self, video: VideoEndpoint) {
        self.capabilities.explicit_video = Some(video);
    }
}

impl DeviceRegistry {
    pub(crate) fn new(cloud_devices: Vec<CloudDevice>, local_devices: Vec<LocalDevice>) -> Self {
        let local_ids = local_devices
            .iter()
            .map(|device| device.id.as_str())
            .collect::<HashSet<_>>();
        let mut registry = Self {
            entries: Vec::new(),
            entry_by_id: HashMap::new(),
        };
        for device in cloud_devices.into_iter().filter(|device| {
            device
                .id
                .as_deref()
                .map(str::trim)
                .is_none_or(|device_id| !local_ids.contains(device_id))
        }) {
            if let Some(device) = KnownDevice::from_cloud(device) {
                registry.push(DeviceEntry::from_cloud(device));
            }
        }
        for local in local_devices {
            registry.push(DeviceEntry::from_local(local));
        }

        registry
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn entries(&self) -> &[DeviceEntry] {
        &self.entries
    }

    pub(crate) fn devices(&self) -> impl Iterator<Item = &KnownDevice> {
        self.entries.iter().map(|entry| &entry.device)
    }

    pub(crate) fn first(&self) -> Option<&DeviceEntry> {
        self.entries.first()
    }

    pub(crate) fn get(&self, device_id: &str) -> Option<&DeviceEntry> {
        self.entry_by_id
            .get(device_id)
            .and_then(|index| self.entries.get(*index))
    }

    pub(super) fn get_mut(&mut self, device_id: &str) -> Option<&mut DeviceEntry> {
        self.entry_by_id
            .get(device_id)
            .and_then(|index| self.entries.get_mut(*index))
    }

    pub(crate) fn local_devices(&self) -> Vec<LocalDevice> {
        self.entries
            .iter()
            .filter_map(|entry| entry.local().cloned())
            .collect()
    }

    pub(crate) fn cloud_mqtt_ids(&self) -> Vec<String> {
        self.entries
            .iter()
            .filter(|entry| entry.has_cloud_mqtt())
            .map(|entry| entry.device.id.clone())
            .collect()
    }

    fn push(&mut self, entry: DeviceEntry) {
        if self.entry_by_id.contains_key(entry.id()) {
            return;
        }
        self.entry_by_id
            .insert(entry.id().to_owned(), self.entries.len());
        self.entries.push(entry);
    }
}

fn non_empty_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use crate::{
        bambu::CloudDevice,
        local::{LocalDevice, LocalEndpoint},
    };

    use super::DeviceRegistry;

    fn local_device(id: &str, name: Option<&str>) -> LocalDevice {
        let mut endpoint = LocalEndpoint::new("192.168.1.50", 8883, "12345678");
        endpoint.name = name.map(str::to_owned);
        LocalDevice {
            id: id.to_owned(),
            endpoint,
        }
    }

    #[test]
    fn registry_uses_local_device_when_ids_overlap() {
        let local_devices = vec![local_device("printer-a", Some("Office"))];
        let registry = DeviceRegistry::new(
            vec![
                CloudDevice {
                    id: Some(" printer-a ".to_owned()),
                    name: Some("Cloud Office".to_owned()),
                    access_code: Some("87654321".to_owned()),
                    ..CloudDevice::default()
                },
                CloudDevice {
                    id: Some("printer-b".to_owned()),
                    name: Some("Garage".to_owned()),
                    ..CloudDevice::default()
                },
            ],
            local_devices,
        );
        let devices = registry.devices().collect::<Vec<_>>();

        assert_eq!(devices.len(), 2);
        assert_eq!(devices[0].id, "printer-b");
        assert_eq!(devices[1].id, "printer-a");
        assert_eq!(devices[1].access_code.as_deref(), Some("12345678"));
    }

    #[test]
    fn registry_ignores_cloud_devices_without_ids() {
        let registry = DeviceRegistry::new(
            vec![
                CloudDevice {
                    id: Some("printer-a".to_owned()),
                    ..CloudDevice::default()
                },
                CloudDevice::default(),
            ],
            Vec::new(),
        );
        let ids = registry
            .devices()
            .map(|device| device.id.clone())
            .collect::<Vec<_>>();

        assert_eq!(ids, vec!["printer-a".to_owned()]);
    }

    #[test]
    fn registry_cloud_mqtt_ids_only_include_cloud_devices() {
        let registry = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer-a".to_owned()),
                ..CloudDevice::default()
            }],
            vec![local_device("printer-b", Some("Office"))],
        );

        assert_eq!(registry.cloud_mqtt_ids(), vec!["printer-a".to_owned()]);
    }
}
