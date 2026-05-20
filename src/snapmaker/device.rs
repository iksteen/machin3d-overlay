//! Resolved Snapmaker device descriptor.

use crate::local::Endpoint;

pub(crate) type SnapmakerEndpoint = Endpoint;

#[derive(Debug, Clone)]
pub(crate) struct SnapmakerDevice {
    /// Stable device id; matches the printer's serial number from
    /// Moonraker's `machine/system_info`.
    pub(crate) serial: String,
    pub(crate) endpoint: SnapmakerEndpoint,
}
