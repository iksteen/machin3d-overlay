//! Snapmaker U1 specifics layered on top of the generic Moonraker backend.
//!
//! A plain Moonraker/Klipper printer needs none of this. The U1 differs in
//! exactly one way that matters here: its camera daemon only emits fresh
//! frames while "monitor mode" is active, and monitor mode is toggled over a
//! per-printer mTLS MQTT control plane. This module owns the LAN pairing
//! dance that issues the mTLS material ([`pair`]), its on-disk persistence
//! ([`SnapToken`], [`auth`]), and the rumqttc transport builder ([`mtls`]).
//! The camera-wake publisher that consumes [`SnapMqttCreds`] lives in
//! [`crate::video`].

mod auth;
pub(crate) mod mtls;
mod pair;

pub(crate) use auth::{default_snap_token_path, load_snap_tokens, upsert_snap_token, SnapToken};
pub(crate) use pair::{fresh_clientid, pair, PairConfig};

use crate::secret::Secret;

/// Per-printer mutual-TLS material for the Snapmaker U1 control-plane MQTT
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
