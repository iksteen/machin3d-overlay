//! Vendor-neutral snapshot of the currently active print.
//!
//! Each backend decodes its own wire format into a `PrinterReport`. The summary
//! layer reads these fields directly — there is no further per-vendor branching
//! below this point.

use super::Material;

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct PrinterReport {
    pub(crate) task_id: Option<String>,
    pub(crate) task_name: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) filename: Option<String>,
    pub(crate) start_time: Option<String>,
    pub(crate) progress: Option<f64>,
    pub(crate) prediction_seconds: Option<f64>,
    pub(crate) remaining_minutes: Option<f64>,
    pub(crate) weight: Option<String>,
    pub(crate) layer_current: Option<i64>,
    pub(crate) layer_total: Option<i64>,
    pub(crate) toolhead_temperature: Option<f64>,
    pub(crate) bed_temperature: Option<f64>,
    pub(crate) fan_speed: Option<f64>,
    pub(crate) print_speed: Option<String>,
    pub(crate) materials: Vec<Material>,
    /// Label of the slot the printer reports as currently in use. Applied to
    /// `materials` at summary time, but only when the task is active — so an
    /// idle or finished printer doesn't highlight a slot.
    pub(crate) active_material: Option<String>,
}
