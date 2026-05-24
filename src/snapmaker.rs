//! Snapmaker / Klipper / Moonraker backend.
//!
//! Each `--snap-device HOST[:PORT]` adds an entry to the resolved device
//! registry under [`crate::devices::DeviceCapabilities::Snapmaker`]. A
//! per-device worker connects to Moonraker's WebSocket JSON-RPC at
//! `ws://HOST:PORT/websocket`, subscribes to the printer objects we care
//! about, and publishes a vendor-neutral `PrinterReport` into the shared
//! `LiveStateStore`.

pub(crate) mod auth;
pub(crate) mod backend;
mod config;
mod device;
mod moonraker;
pub(crate) mod mtls;
pub(crate) mod pair;
mod probe;
mod report;

pub(crate) use auth::{default_snap_token_path, load_snap_tokens, upsert_snap_token, SnapToken};
pub(crate) use config::SnapmakerDeviceConfig;
pub(crate) use device::{SnapMqttCreds, SnapmakerDevice, SnapmakerEndpoint};
pub(crate) use pair::{fresh_clientid, pair, PairConfig};
pub(crate) use probe::probe_system_info;
