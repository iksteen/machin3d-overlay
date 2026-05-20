//! Snapmaker / Klipper / Moonraker backend.
//!
//! Each `--snap-device SERIAL=HOST[:PORT]` adds an entry to the resolved
//! device registry with `Backend::Snapmaker`. A per-device worker connects
//! to Moonraker's WebSocket JSON-RPC at `ws://HOST:PORT/websocket`,
//! subscribes to the printer objects we care about, and publishes a
//! vendor-neutral `PrinterReport` into the shared `LiveStateStore`.

pub(crate) mod backend;
mod config;
mod device;
mod moonraker;
mod report;

pub(crate) use config::SnapmakerDeviceConfig;
pub(crate) use device::{SnapmakerDevice, SnapmakerEndpoint};
