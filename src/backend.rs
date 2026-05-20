//! Vendor tag attached to each resolved device.
//!
//! The enum lists every supported printer vendor. `DeviceEntry::backend()`
//! returns the variant; per-vendor wiring (MQTT, video, thumbnail) lives in
//! the matching `<vendor>::backend` module.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Backend {
    Bambu,
    Snapmaker,
}
