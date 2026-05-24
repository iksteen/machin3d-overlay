//! Resolved Snapmaker device descriptor.

use crate::{endpoint::Endpoint, secret::Secret, snapmaker::SnapToken};

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
    /// mTLS material from `snap-pair` if the operator has paired this
    /// printer. Required to drive `camera.start_monitor` reliably (see
    /// [`crate::video::snapmaker`]).
    pub(crate) mtls: Option<SnapMqttCreds>,
}

/// Per-printer mutual-TLS material for the Snapmaker control-plane MQTT
/// channel. Derived from a paired [`SnapToken`] via [`From`].
#[derive(Debug, Clone)]
pub(crate) struct SnapMqttCreds {
    pub clientid: String,
    pub port: u16,
    pub ca: String,
    pub cert: String,
    pub key: Secret<String>,
}

impl From<SnapToken> for SnapMqttCreds {
    fn from(token: SnapToken) -> Self {
        Self {
            clientid: token.clientid,
            port: token.mqtt_port,
            ca: token.ca,
            cert: token.cert,
            key: token.key,
        }
    }
}
