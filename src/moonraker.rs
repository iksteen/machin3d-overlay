//! Moonraker / Klipper backend.
//!
//! Each `--snap-device HOST[:PORT]` adds a
//! [`crate::devices::DeviceCapabilities::Moonraker`] entry to the resolved
//! device registry. A per-device worker connects to Moonraker's WebSocket
//! JSON-RPC at `ws://HOST:PORT/websocket`, subscribes to the printer objects
//! we consume, and publishes a vendor-neutral `PrinterReport` into the shared
//! `LiveStateStore`. This path is generic: it drives any conformant
//! Moonraker/Klipper printer, not only Snapmaker.
//!
//! The Snapmaker U1 is one such printer with a single wrinkle — its camera
//! daemon only streams while a bespoke mTLS-MQTT control plane has woken it.
//! That quirk is quarantined under [`u1`]; everything else here is vendor
//! neutral.

pub(crate) mod backend;
mod client;
mod config;
mod device;
mod probe;
mod report;
pub(crate) mod u1;

pub(crate) use config::MoonrakerDeviceConfig;
pub(crate) use device::{MoonrakerDevice, MoonrakerEndpoint};
pub(crate) use probe::probe_system_info;
pub(crate) use u1::{
    default_snap_token_path, fresh_clientid, load_snap_tokens, pair, upsert_snap_token, PairConfig,
    SnapMqttCreds, SnapToken,
};
