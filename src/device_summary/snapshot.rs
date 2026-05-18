use crate::{
    bambu::{AmsState, PrinterStatus, Tray},
    devices::KnownDevice,
    mqtt::{MqttConnectionStatus, MqttDeviceConnection, MqttDeviceState, PrintActivity},
};

use super::{
    spool::{ams_spools, external_spool},
    DeviceSummary, TaskSource,
};

pub(super) fn summarize_device(
    device: &KnownDevice,
    state: Option<&MqttDeviceState>,
    connection: Option<&MqttDeviceConnection>,
) -> DeviceSummary {
    DeviceSummary::from_snapshot(DeviceSnapshot::new(device, state, connection))
}

struct DeviceSnapshot<'a> {
    device: &'a KnownDevice,
    live: Option<&'a MqttDeviceState>,
    connection: Option<&'a MqttDeviceConnection>,
}

impl<'a> DeviceSnapshot<'a> {
    fn new(
        device: &'a KnownDevice,
        live: Option<&'a MqttDeviceState>,
        connection: Option<&'a MqttDeviceConnection>,
    ) -> Self {
        Self {
            device,
            live,
            connection,
        }
    }

    fn catalog_status(&self) -> &PrinterStatus {
        &self.device.status
    }

    fn connection(&self) -> Option<&MqttDeviceConnection> {
        self.live.map(|state| &state.connection).or(self.connection)
    }

    fn service_connected(&self) -> bool {
        self.service_status() == MqttConnectionStatus::Connected
    }

    fn service_status(&self) -> MqttConnectionStatus {
        self.connection()
            .map(|connection| connection.status)
            .unwrap_or(MqttConnectionStatus::Disconnected)
    }

    fn catalog_fallback_status(&self) -> Option<&PrinterStatus> {
        match self.live {
            Some(live) if !live.is_fresh() => None,
            None if self.connection().is_some() => None,
            _ => Some(self.catalog_status()),
        }
    }

    fn report_status(&self) -> Option<&PrinterStatus> {
        self.live
            .filter(|state| state.is_fresh())
            .map(|state| &state.report)
    }

    fn active_status(&self) -> Option<&PrinterStatus> {
        if let Some(live) = self.live.filter(|state| state.is_fresh()) {
            return live.is_active_task().then_some(&live.report);
        }
        let catalog_status = self.catalog_fallback_status()?;
        PrintActivity::from_report(catalog_status)
            .is_active_task()
            .then_some(catalog_status)
    }

    fn active_string(&self, pick: impl Fn(&PrinterStatus) -> Option<&String>) -> Option<String> {
        self.active_status().and_then(pick).cloned()
    }

    fn print_f64(&self, pick: impl Fn(&PrinterStatus) -> Option<f64>) -> Option<f64> {
        self.report_status()
            .and_then(&pick)
            .or_else(|| self.catalog_fallback_status().and_then(pick))
    }

    fn active_f64(&self, pick: impl Fn(&PrinterStatus) -> Option<f64>) -> Option<f64> {
        self.active_status().and_then(pick)
    }

    fn active_i64(&self, pick: impl Fn(&PrinterStatus) -> Option<i64>) -> Option<i64> {
        self.active_status().and_then(pick)
    }

    fn ams(&self) -> Option<&AmsState> {
        self.report_status()
            .and_then(|status| status.ams.as_ref())
            .filter(|ams| ams.has_spool_data())
            .or_else(|| self.catalog_fallback_status()?.ams.as_ref())
    }

    fn external_tray(&self) -> Option<&Tray> {
        self.report_status()
            .and_then(|status| status.external_tray.as_ref())
            .filter(|tray| tray.has_spool_data())
            .or_else(|| self.catalog_fallback_status()?.external_tray.as_ref())
    }

    fn display_mode(&self) -> Option<String> {
        self.active_status().and_then(print_mode)
    }

    fn active_tray(&self) -> Option<i64> {
        self.active_status()
            .and_then(|status| status.ams.as_ref())
            .and_then(|ams| ams.tray_now)
    }

    fn task_id(&self) -> Option<String> {
        self.active_string(|print| print.task_id.as_ref())
    }

    fn device_id(&self) -> String {
        self.device.id.clone()
    }

    fn task_name(&self) -> Option<String> {
        self.active_string(|print| print.task_name.as_ref())
    }

    fn task_status(&self) -> Option<String> {
        self.active_string(|print| print.status.as_ref())
    }

    fn progress(&self) -> Option<f64> {
        self.active_f64(|print| print.progress)
    }

    fn prediction(&self) -> Option<f64> {
        self.active_f64(|print| print.prediction_seconds)
    }

    fn start_time(&self) -> Option<String> {
        self.active_string(|print| print.start_time.as_ref())
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
        let filename = snapshot.active_string(|print| print.filename.as_ref());
        let active_tray = snapshot.active_tray();
        let is_printing = snapshot.active_status().is_some();
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
                .unwrap_or_else(|| "Bambu printer".to_owned()),
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
            weight: snapshot.active_string(|print| print.weight.as_ref()),
            layer_current: snapshot.active_i64(|print| print.layer_current),
            layer_total: snapshot.active_i64(|print| print.layer_total),
            remaining_seconds: snapshot
                .active_f64(|print| print.remaining_minutes)
                .map(|minutes| minutes * 60.0),
            toolhead_temperature: snapshot.print_f64(|print| print.toolhead_temperature),
            bed_temperature: snapshot.print_f64(|print| print.bed_temperature),
            fan_speed: snapshot.print_f64(|print| print.fan_speed),
            print_mode: snapshot.display_mode(),
            ams_spools: ams_spools(snapshot.ams(), active_tray),
            external_spool: external_spool(snapshot.external_tray(), active_tray),
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

fn print_mode(print_status: &PrinterStatus) -> Option<String> {
    if let Some(speed_level) = print_status.speed_level {
        return Some(match speed_level {
            1 => "Silent".to_owned(),
            2 => "Standard".to_owned(),
            3 => "Sport".to_owned(),
            4 => "Ludicrous".to_owned(),
            other => format!("Level {other}"),
        });
    }
    None
}
