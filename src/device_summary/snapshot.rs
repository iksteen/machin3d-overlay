use crate::{
    devices::KnownDevice,
    live::{ConnectionStatus, DeviceConnection, DeviceLiveState, Material, PrintActivity, PrinterReport},
};

use super::{DeviceSummary, TaskSource};

pub(super) fn summarize_device(
    device: &KnownDevice,
    state: Option<&DeviceLiveState>,
    connection: Option<&DeviceConnection>,
) -> DeviceSummary {
    DeviceSummary::from_snapshot(DeviceSnapshot::new(device, state, connection))
}

struct DeviceSnapshot<'a> {
    device: &'a KnownDevice,
    live: Option<&'a DeviceLiveState>,
    connection: Option<&'a DeviceConnection>,
}

impl<'a> DeviceSnapshot<'a> {
    fn new(
        device: &'a KnownDevice,
        live: Option<&'a DeviceLiveState>,
        connection: Option<&'a DeviceConnection>,
    ) -> Self {
        Self {
            device,
            live,
            connection,
        }
    }

    fn catalog_report(&self) -> &PrinterReport {
        &self.device.status
    }

    fn connection(&self) -> Option<&DeviceConnection> {
        self.live.map(|state| &state.connection).or(self.connection)
    }

    fn service_connected(&self) -> bool {
        self.service_status() == ConnectionStatus::Connected
    }

    fn service_status(&self) -> ConnectionStatus {
        self.connection()
            .map(|connection| connection.status)
            .unwrap_or(ConnectionStatus::Disconnected)
    }

    fn catalog_fallback_report(&self) -> Option<&PrinterReport> {
        match self.live {
            Some(live) if !live.is_fresh() => None,
            None if self.connection().is_some() => None,
            _ => Some(self.catalog_report()),
        }
    }

    fn report(&self) -> Option<&PrinterReport> {
        self.live
            .filter(|state| state.is_fresh())
            .map(|state| &state.report)
    }

    fn active_report(&self) -> Option<&PrinterReport> {
        if let Some(live) = self.live.filter(|state| state.is_fresh()) {
            return live.is_active_task().then_some(&live.report);
        }
        let catalog_report = self.catalog_fallback_report()?;
        PrintActivity::from_status(catalog_report.status.as_deref())
            .is_active_task()
            .then_some(catalog_report)
    }

    fn active_string(&self, pick: impl Fn(&PrinterReport) -> Option<&String>) -> Option<String> {
        self.active_report().and_then(pick).cloned()
    }

    fn report_f64(&self, pick: impl Fn(&PrinterReport) -> Option<f64>) -> Option<f64> {
        self.report()
            .and_then(&pick)
            .or_else(|| self.catalog_fallback_report().and_then(pick))
    }

    fn active_f64(&self, pick: impl Fn(&PrinterReport) -> Option<f64>) -> Option<f64> {
        self.active_report().and_then(pick)
    }

    fn active_i64(&self, pick: impl Fn(&PrinterReport) -> Option<i64>) -> Option<i64> {
        self.active_report().and_then(pick)
    }

    fn materials(&self) -> Vec<Material> {
        let live_materials = self
            .report()
            .map(|report| report.materials.as_slice())
            .filter(|materials| !materials.is_empty());
        let mut materials = if let Some(live) = live_materials {
            live.to_vec()
        } else {
            self.catalog_fallback_report()
                .map(|report| report.materials.clone())
                .unwrap_or_default()
        };
        let active_slot = self
            .active_report()
            .and_then(|report| report.active_material.as_deref());
        if let Some(slot) = active_slot {
            for material in &mut materials {
                material.active = material.label == slot;
            }
        }
        materials
    }

    fn print_speed(&self) -> Option<String> {
        self.active_report()
            .and_then(|report| report.print_speed.clone())
    }

    fn task_id(&self) -> Option<String> {
        self.active_string(|report| report.task_id.as_ref())
    }

    fn device_id(&self) -> String {
        self.device.id.clone()
    }

    fn task_name(&self) -> Option<String> {
        self.active_string(|report| report.task_name.as_ref())
    }

    fn task_status(&self) -> Option<String> {
        self.active_string(|report| report.status.as_ref())
    }

    fn progress(&self) -> Option<f64> {
        self.active_f64(|report| report.progress)
    }

    fn prediction(&self) -> Option<f64> {
        self.active_f64(|report| report.prediction_seconds)
    }

    fn start_time(&self) -> Option<String> {
        self.active_string(|report| report.start_time.as_ref())
    }
}

impl DeviceSummary {
    fn from_snapshot(snapshot: DeviceSnapshot<'_>) -> Self {
        let device_id = snapshot.device_id();
        let task_id = snapshot.task_id();
        let task_name = snapshot.task_name();
        let task_status = snapshot.task_status();
        let progress = snapshot.progress();
        let prediction = snapshot.prediction();
        let start_time = snapshot.start_time();
        let filename = snapshot.active_string(|report| report.filename.as_ref());
        let is_printing = snapshot.active_report().is_some();
        let thumbnail_task = thumbnail_task(
            is_printing,
            task_id.as_deref(),
            filename.as_deref(),
            task_name.as_deref(),
            start_time.as_deref(),
        );

        DeviceSummary {
            id: device_id,
            name: snapshot
                .device
                .name
                .clone()
                .unwrap_or_else(|| snapshot.device.id.clone()),
            online: snapshot.device.online.unwrap_or(true),
            service_status: snapshot.service_status(),
            service_connected: snapshot.service_connected(),
            service_error: snapshot
                .connection()
                .and_then(|connection| connection.error.clone()),
            task_name: task_name.clone(),
            title: task_name,
            filename: filename.clone(),
            task_status,
            start_time,
            prediction,
            progress,
            thumbnail_task,
            weight: snapshot.active_string(|report| report.weight.as_ref()),
            layer_current: snapshot.active_i64(|report| report.layer_current),
            layer_total: snapshot.active_i64(|report| report.layer_total),
            remaining_seconds: snapshot
                .active_f64(|report| report.remaining_minutes)
                .map(|minutes| minutes * 60.0),
            toolhead_temperature: snapshot.report_f64(|report| report.toolhead_temperature),
            bed_temperature: snapshot.report_f64(|report| report.bed_temperature),
            fan_speed: snapshot.report_f64(|report| report.fan_speed),
            print_speed: snapshot.print_speed(),
            materials: snapshot.materials(),
            is_printing,
            task_source: TaskSource::PrinterStatus,
            plate_index: None,
        }
    }
}

fn thumbnail_task(
    has_print_status_task: bool,
    task_id: Option<&str>,
    filename: Option<&str>,
    task_name: Option<&str>,
    start_time: Option<&str>,
) -> Option<String> {
    if !has_print_status_task {
        return None;
    }
    task_id
        .or(filename)
        .or(task_name)
        .or(start_time)
        .map(str::trim)
        .filter(|task| !task.is_empty())
        .map(str::to_owned)
}
