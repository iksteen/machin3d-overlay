//! Resolved Snapmaker device descriptor.

use crate::local::Endpoint;

pub(crate) type SnapmakerEndpoint = Endpoint;

#[derive(Debug, Clone)]
pub(crate) struct SnapmakerDevice {
    /// Stable device id (serial number) read from
    /// Moonraker's `machine/system_info`.
    pub(crate) serial: String,
    pub(crate) endpoint: SnapmakerEndpoint,
    /// Human-readable name (Moonraker's `product_info.device_name`,
    /// falling back to `machine_type`).
    pub(crate) name: Option<String>,
}
