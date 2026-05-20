use std::collections::HashMap;

mod snapshot;

#[cfg(test)]
mod tests;

use crate::{
    devices::KnownDevice,
    live::{ConnectionStatus, DeviceConnection, DeviceLiveState, Material},
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum TaskSource {
    #[default]
    PrinterStatus,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DeviceSummary {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) online: bool,
    pub(crate) service_status: ConnectionStatus,
    pub(crate) service_connected: bool,
    pub(crate) service_error: Option<String>,
    pub(crate) task_name: Option<String>,
    pub(crate) title: Option<String>,
    pub(crate) filename: Option<String>,
    pub(crate) task_status: Option<String>,
    pub(crate) start_time: Option<String>,
    pub(crate) prediction: Option<f64>,
    pub(crate) progress: Option<f64>,
    pub(crate) thumbnail_task: Option<String>,
    pub(crate) weight: Option<String>,
    pub(crate) layer_current: Option<i64>,
    pub(crate) layer_total: Option<i64>,
    pub(crate) remaining_seconds: Option<f64>,
    pub(crate) toolhead_temperature: Option<f64>,
    pub(crate) bed_temperature: Option<f64>,
    pub(crate) fan_speed: Option<f64>,
    pub(crate) print_mode: Option<String>,
    pub(crate) materials: Vec<Material>,
    pub(crate) is_printing: bool,
    pub(crate) task_source: TaskSource,
    pub(crate) plate_index: Option<String>,
}

pub(crate) fn summarize_devices<'a>(
    devices: impl IntoIterator<Item = &'a KnownDevice>,
    states: &HashMap<String, DeviceLiveState>,
    connections: &HashMap<String, DeviceConnection>,
) -> Vec<DeviceSummary> {
    devices
        .into_iter()
        .map(|device| {
            snapshot::summarize_device(device, states.get(&device.id), connections.get(&device.id))
        })
        .collect()
}
