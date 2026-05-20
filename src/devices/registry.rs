//! Resolved, startup-stable device catalog.
//!
//! `DeviceRegistry` is the authority for devices known by the service after
//! startup discovery has finished. Local devices intentionally override cloud
//! entries with the same ID, because local MQTT owns the live data path in that
//! scenario. Credentials are kept behind accessors so web/API payloads cannot
//! accidentally serialize access codes.
//!
//! `DeviceRegistry` is immutable once it has been handed out. Mutation during
//! resolution goes through [`DeviceRegistryBuilder`], which returns a frozen
//! `DeviceRegistry` from [`DeviceRegistryBuilder::build`].

use std::collections::{HashMap, HashSet};

use crate::{
    backend::Backend,
    bambu::{printer_status_to_live, CloudDevice},
    live::PrinterReport,
    local::LocalDevice,
    secret::Secret,
    video::VideoEndpoint,
};
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeviceSource {
    Cloud,
    Local,
}

#[derive(Debug, Clone)]
pub(crate) struct KnownDevice {
    pub(crate) id: String,
    pub(crate) name: Option<String>,
    pub(crate) online: Option<bool>,
    pub(crate) status: PrinterReport,
}

impl KnownDevice {
    pub(crate) fn from_cloud(device: CloudDevice) -> Option<Self> {
        Some(Self {
            id: non_empty_string(device.id)?,
            name: device.name,
            online: device.online,
            status: printer_status_to_live(&device.status),
        })
    }

    pub(crate) fn from_local(device: &LocalDevice) -> Self {
        Self {
            id: device.id.clone(),
            name: device.endpoint.name.clone(),
            online: Some(true),
            status: PrinterReport::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DeviceEntry {
    device: KnownDevice,
    credentials: DeviceCredentials,
    capabilities: DeviceCapabilities,
    backend: Backend,
}

#[derive(Debug, Clone, Default)]
struct DeviceCredentials {
    access_code: Option<Secret<String>>,
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
    fn from_cloud(device: CloudDevice) -> Option<Self> {
        let credentials = DeviceCredentials {
            access_code: device.access_code.clone(),
        };
        Some(Self {
            device: KnownDevice::from_cloud(device)?,
            credentials,
            capabilities: DeviceCapabilities {
                cloud_mqtt: true,
                ..DeviceCapabilities::default()
            },
            backend: Backend::Bambu,
        })
    }

    fn from_local(local: LocalDevice) -> Self {
        Self {
            device: KnownDevice::from_local(&local),
            credentials: DeviceCredentials::default(),
            capabilities: DeviceCapabilities {
                local_mqtt: Some(local),
                ..DeviceCapabilities::default()
            },
            backend: Backend::Bambu,
        }
    }

    pub(crate) fn backend(&self) -> Backend {
        self.backend
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

    pub(crate) fn access_code(&self) -> Option<&str> {
        let local_access_code = self
            .local()
            .and_then(|local| non_empty_str(local.endpoint.access_code()));
        let entry_access_code = self
            .credentials
            .access_code
            .as_ref()
            .map(|code| code.expose().as_str())
            .and_then(non_empty_str);
        local_access_code.or(entry_access_code)
    }

    pub(crate) fn has_access_code(&self) -> bool {
        self.access_code().is_some()
    }

    pub(super) fn set_access_code(&mut self, access_code: Option<Secret<String>>) {
        self.credentials.access_code = access_code;
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

/// Mutable construction surface for [`DeviceRegistry`].
///
/// Used during startup resolution to hydrate access codes and explicit video
/// endpoints. Once [`build`](Self::build) is called, the resulting
/// `DeviceRegistry` has no mutable API and cannot be modified.
pub(crate) struct DeviceRegistryBuilder {
    inner: DeviceRegistry,
}

impl DeviceRegistry {
    /// Convenience constructor for tests and call sites that do not need to
    /// hydrate access codes or attach explicit video endpoints. Production code
    /// goes through [`DeviceRegistryBuilder`] so hydration can happen before
    /// the registry is frozen.
    #[cfg(test)]
    pub(crate) fn new(cloud_devices: Vec<CloudDevice>, local_devices: Vec<LocalDevice>) -> Self {
        DeviceRegistryBuilder::new(cloud_devices, local_devices).build()
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

    pub(crate) fn local_devices(&self) -> Vec<LocalDevice> {
        self.entries
            .iter()
            .filter_map(|entry| entry.local().cloned())
            .collect()
    }
}

impl DeviceRegistryBuilder {
    pub(crate) fn new(cloud_devices: Vec<CloudDevice>, local_devices: Vec<LocalDevice>) -> Self {
        let local_ids = local_devices
            .iter()
            .map(|device| device.id.as_str())
            .collect::<HashSet<_>>();
        let mut builder = Self {
            inner: DeviceRegistry {
                entries: Vec::new(),
                entry_by_id: HashMap::new(),
            },
        };
        for device in cloud_devices.into_iter().filter(|device| {
            device
                .id
                .as_deref()
                .map(str::trim)
                .is_none_or(|device_id| !local_ids.contains(device_id))
        }) {
            if let Some(entry) = DeviceEntry::from_cloud(device) {
                builder.push(entry);
            }
        }
        for local in local_devices {
            builder.push(DeviceEntry::from_local(local));
        }

        builder
    }

    pub(super) fn entry_mut(&mut self, device_id: &str) -> Option<&mut DeviceEntry> {
        self.inner
            .entry_by_id
            .get(device_id)
            .and_then(|index| self.inner.entries.get_mut(*index))
    }

    pub(crate) fn build(self) -> DeviceRegistry {
        self.inner
    }

    fn push(&mut self, entry: DeviceEntry) {
        let device_id = entry.id().to_owned();
        if self.inner.entry_by_id.contains_key(&device_id) {
            warn!(
                device_id = %device_id,
                "ignoring duplicate device entry in resolved catalog"
            );
            return;
        }
        self.inner
            .entry_by_id
            .insert(device_id, self.inner.entries.len());
        self.inner.entries.push(entry);
    }
}

fn non_empty_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn non_empty_str(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

#[cfg(test)]
mod tests {
    use crate::{
        bambu::CloudDevice,
        local::{LocalDevice, LocalEndpoint},
        secret::Secret,
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
                    access_code: Some(Secret::new("87654321".to_owned())),
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
        assert_eq!(
            registry.get("printer-a").unwrap().access_code(),
            Some("12345678")
        );
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
    fn registry_marks_cloud_mqtt_only_for_cloud_devices() {
        let registry = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer-a".to_owned()),
                ..CloudDevice::default()
            }],
            vec![local_device("printer-b", Some("Office"))],
        );

        let cloud_ids: Vec<_> = registry
            .entries()
            .iter()
            .filter(|entry| entry.has_cloud_mqtt())
            .map(|entry| entry.id().to_owned())
            .collect();
        assert_eq!(cloud_ids, vec!["printer-a".to_owned()]);
    }

    #[test]
    fn registry_access_code_prefers_local_credentials() {
        let registry = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer-a".to_owned()),
                access_code: Some(Secret::new("cloud-code".to_owned())),
                ..CloudDevice::default()
            }],
            vec![local_device("printer-a", Some("Office"))],
        );

        assert_eq!(
            registry.get("printer-a").unwrap().access_code(),
            Some("12345678")
        );
    }

    #[test]
    fn registry_source_follows_active_live_capability() {
        let registry = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer-a".to_owned()),
                ..CloudDevice::default()
            }],
            vec![local_device("printer-b", Some("Office"))],
        );

        assert!(registry.get("printer-a").unwrap().has_cloud_mqtt());
        assert!(registry.get("printer-a").unwrap().local().is_none());
        assert!(!registry.get("printer-b").unwrap().has_cloud_mqtt());
        assert!(registry.get("printer-b").unwrap().local().is_some());
    }
}
