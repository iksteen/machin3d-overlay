//! Resolved, startup-stable device catalog.
//!
//! `DeviceRegistry` is the authority for devices known by the service after
//! startup discovery has finished. Local Bambu devices intentionally
//! override cloud entries with the same ID, because local MQTT owns the
//! live data path in that scenario. Credentials are kept behind accessors
//! so web/API payloads cannot accidentally serialize access codes.
//!
//! Vendor-specific configuration lives in [`DeviceCapabilities`], a
//! per-vendor enum. The variant is the source of truth for which backend
//! handles a device — there is no separate discriminant field — and the
//! per-vendor payload is non-`Option` for everything that's actually
//! required at runtime.
//!
//! `DeviceRegistry` is immutable once it has been handed out. Mutation
//! during resolution goes through [`DeviceRegistryBuilder`], which returns
//! a frozen `DeviceRegistry` from [`DeviceRegistryBuilder::build`].

use std::collections::{HashMap, HashSet};

use crate::{
    bambu::{printer_status_to_live, CloudDevice},
    live::PrinterReport,
    local::LocalDevice,
    secret::Secret,
    snapmaker::{SnapMqttCreds, SnapmakerDevice, SnapmakerEndpoint},
    video::VideoEndpoint,
};
use tracing::warn;

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

    pub(crate) fn from_snapmaker(device: &SnapmakerDevice) -> Self {
        Self {
            id: device.serial.clone(),
            name: device.name.clone(),
            online: Some(true),
            status: PrinterReport::default(),
        }
    }
}

/// Per-vendor configuration owned by a [`DeviceEntry`]. The enum
/// variant is the authoritative "backend" tag; each variant holds only
/// fields that variant actually needs, with no cross-vendor `Option`
/// fields.
#[derive(Debug, Clone)]
pub(crate) enum DeviceCapabilities {
    Bambu(BambuCapabilities),
    Snapmaker(SnapmakerCapabilities),
}

#[derive(Debug, Clone)]
pub(crate) struct BambuCapabilities {
    /// True for cloud-discovered devices that the service should drive
    /// over the Bambu Cloud MQTT broker.
    pub(crate) cloud_mqtt: bool,
    /// Set for devices we drive over the LAN MQTT broker on the printer
    /// itself; carries the connection parameters plus access code.
    pub(crate) local_mqtt: Option<LocalDevice>,
    /// Set when the operator passed `--bbl-video-device`, or when startup
    /// probing auto-enabled a local video endpoint.
    pub(crate) explicit_video: Option<VideoEndpoint>,
    /// Access code from cloud `/bind` metadata, used when the LAN device
    /// does not carry one of its own. The effective access code is
    /// [`BambuCapabilities::access_code`].
    pub(crate) access_code: Option<Secret<String>>,
}

#[derive(Debug, Clone)]
pub(crate) struct SnapmakerCapabilities {
    /// Moonraker HTTP/WS endpoint. Required for every Snapmaker entry —
    /// startup resolution probes this to learn the SN that becomes the
    /// device ID.
    pub(crate) endpoint: SnapmakerEndpoint,
    /// Per-printer mTLS material from a paired `snap-pair` token. When
    /// `None`, the camera worker can still poll the JPEG but cannot wake
    /// the daemon on its own.
    pub(crate) mtls: Option<SnapMqttCreds>,
}

impl BambuCapabilities {
    /// Effective access code: prefers the LAN device's code over the
    /// cloud `/bind` code, both filtered for non-empty content.
    pub(crate) fn access_code(&self) -> Option<&str> {
        let local = self
            .local_mqtt
            .as_ref()
            .and_then(|local| non_empty_str(local.endpoint.access_code()));
        let entry = self
            .access_code
            .as_ref()
            .map(|code| code.expose().as_str())
            .and_then(non_empty_str);
        local.or(entry)
    }

    pub(crate) fn has_access_code(&self) -> bool {
        self.access_code().is_some()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DeviceEntry {
    device: KnownDevice,
    capabilities: DeviceCapabilities,
}

#[derive(Debug, Clone)]
pub(crate) struct DeviceRegistry {
    entries: Vec<DeviceEntry>,
    entry_by_id: HashMap<String, usize>,
}

impl DeviceEntry {
    fn from_cloud(device: CloudDevice) -> Option<Self> {
        let access_code = device.access_code.clone();
        Some(Self {
            device: KnownDevice::from_cloud(device)?,
            capabilities: DeviceCapabilities::Bambu(BambuCapabilities {
                cloud_mqtt: true,
                local_mqtt: None,
                explicit_video: None,
                access_code,
            }),
        })
    }

    fn from_local(local: LocalDevice) -> Self {
        Self {
            device: KnownDevice::from_local(&local),
            capabilities: DeviceCapabilities::Bambu(BambuCapabilities {
                cloud_mqtt: false,
                local_mqtt: Some(local),
                explicit_video: None,
                access_code: None,
            }),
        }
    }

    fn from_snapmaker(device: SnapmakerDevice) -> Self {
        Self {
            device: KnownDevice::from_snapmaker(&device),
            capabilities: DeviceCapabilities::Snapmaker(SnapmakerCapabilities {
                endpoint: device.endpoint,
                mtls: device.mtls,
            }),
        }
    }

    pub(crate) fn id(&self) -> &str {
        self.device.id.as_str()
    }

    pub(crate) fn device(&self) -> &KnownDevice {
        &self.device
    }

    pub(super) fn device_mut(&mut self) -> &mut KnownDevice {
        &mut self.device
    }

    pub(crate) fn capabilities(&self) -> &DeviceCapabilities {
        &self.capabilities
    }

    pub(crate) fn bambu(&self) -> Option<&BambuCapabilities> {
        match &self.capabilities {
            DeviceCapabilities::Bambu(bambu) => Some(bambu),
            DeviceCapabilities::Snapmaker(_) => None,
        }
    }

    pub(crate) fn snapmaker(&self) -> Option<&SnapmakerCapabilities> {
        match &self.capabilities {
            DeviceCapabilities::Snapmaker(snap) => Some(snap),
            DeviceCapabilities::Bambu(_) => None,
        }
    }

    pub(super) fn bambu_mut(&mut self) -> Option<&mut BambuCapabilities> {
        match &mut self.capabilities {
            DeviceCapabilities::Bambu(bambu) => Some(bambu),
            DeviceCapabilities::Snapmaker(_) => None,
        }
    }
}

/// Mutable construction surface for [`DeviceRegistry`].
///
/// Used during startup resolution to hydrate access codes and attach
/// explicit video endpoints. Once [`build`](Self::build) is called, the
/// resulting `DeviceRegistry` has no mutable API and cannot be modified.
pub(crate) struct DeviceRegistryBuilder {
    inner: DeviceRegistry,
}

impl DeviceRegistry {
    /// Convenience constructor for tests and call sites that do not need
    /// to hydrate access codes or attach explicit video endpoints.
    /// Production code goes through [`DeviceRegistryBuilder`] so
    /// hydration can happen before the registry is frozen.
    #[cfg(test)]
    pub(crate) fn new(cloud_devices: Vec<CloudDevice>, local_devices: Vec<LocalDevice>) -> Self {
        DeviceRegistryBuilder::new(cloud_devices, local_devices, Vec::new()).build()
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

    /// All Bambu entries paired with their typed capabilities. Use this
    /// instead of `entries()` + a `match` when you only care about Bambu.
    pub(crate) fn bambu_entries(&self) -> impl Iterator<Item = (&DeviceEntry, &BambuCapabilities)> {
        self.entries
            .iter()
            .filter_map(|entry| entry.bambu().map(|bambu| (entry, bambu)))
    }

    /// All Snapmaker entries paired with their typed capabilities.
    pub(crate) fn snapmaker_entries(
        &self,
    ) -> impl Iterator<Item = (&DeviceEntry, &SnapmakerCapabilities)> {
        self.entries
            .iter()
            .filter_map(|entry| entry.snapmaker().map(|snap| (entry, snap)))
    }

    pub(crate) fn local_devices(&self) -> Vec<LocalDevice> {
        self.bambu_entries()
            .filter_map(|(_, bambu)| bambu.local_mqtt.clone())
            .collect()
    }
}

impl DeviceRegistryBuilder {
    pub(crate) fn new(
        cloud_devices: Vec<CloudDevice>,
        local_devices: Vec<LocalDevice>,
        snapmaker_devices: Vec<SnapmakerDevice>,
    ) -> Self {
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
        for snapmaker in snapmaker_devices {
            builder.push(DeviceEntry::from_snapmaker(snapmaker));
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
            registry
                .get("printer-a")
                .and_then(|entry| entry.bambu())
                .and_then(|bambu| bambu.access_code()),
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
            .bambu_entries()
            .filter(|(_, bambu)| bambu.cloud_mqtt)
            .map(|(entry, _)| entry.id().to_owned())
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
            registry
                .get("printer-a")
                .and_then(|entry| entry.bambu())
                .and_then(|bambu| bambu.access_code()),
            Some("12345678")
        );
    }

    #[test]
    fn registry_capability_split_marks_local_and_cloud_paths() {
        let registry = DeviceRegistry::new(
            vec![CloudDevice {
                id: Some("printer-a".to_owned()),
                ..CloudDevice::default()
            }],
            vec![local_device("printer-b", Some("Office"))],
        );

        let printer_a = registry.get("printer-a").unwrap().bambu().unwrap();
        let printer_b = registry.get("printer-b").unwrap().bambu().unwrap();

        assert!(printer_a.cloud_mqtt);
        assert!(printer_a.local_mqtt.is_none());
        assert!(!printer_b.cloud_mqtt);
        assert!(printer_b.local_mqtt.is_some());
    }
}
