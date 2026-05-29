//! Resolved Moonraker device descriptor.

use crate::{endpoint::Endpoint, moonraker::u1::SnapMqttCreds};

pub(crate) type MoonrakerEndpoint = Endpoint;

#[derive(Debug, Clone)]
pub(crate) struct MoonrakerDevice {
    /// Stable device id (serial number) read from
    /// Moonraker's `machine/system_info`.
    pub(crate) serial: String,
    pub(crate) endpoint: MoonrakerEndpoint,
    /// Human-readable name (Moonraker's `product_info.device_name`,
    /// falling back to `machine_type`).
    pub(crate) name: Option<String>,
    /// Snapmaker U1 mTLS material from `snap-pair`, when the operator has
    /// paired this printer. `None` for a plain Moonraker printer (and for an
    /// unpaired U1); only the U1 camera-wake path consumes it (see
    /// [`crate::moonraker::u1`] and [`crate::video`]).
    pub(crate) mtls: Option<SnapMqttCreds>,
}
