//! Vendor-neutral live-state model.
//!
//! Every backend (Bambu MQTT and Moonraker) publishes the
//! same shape: a [`PrinterReport`] of the printer's current task plus a
//! [`DeviceConnection`] describing whether we're hearing from it. The device
//! summary layer and the web payload only ever see these types — they never
//! reach into vendor-specific decoded structures (such as Bambu's `AmsState`
//! or Klipper's `extruder` objects).

mod material;
mod report;
mod state;
mod store;

pub(crate) use material::Material;
pub(crate) use report::PrinterReport;
pub(crate) use state::{ConnectionStatus, DeviceConnection, DeviceLiveState, PrintActivity};
pub(crate) use store::{LiveStateStore, LiveStatusPayload};
