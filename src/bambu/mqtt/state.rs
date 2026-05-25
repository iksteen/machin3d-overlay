//! Bambu-internal envelope around the latest MQTT printer report.
//!
//! Thumbnail fetching needs the raw `PrinterStatus` (Bambu cloud task lookup
//! and local 3MF download read fields beyond the vendor-neutral
//! [`live::PrinterReport`]). Other consumers go through
//! [`live::DeviceLiveState`], which the MQTT runtime builds via
//! [`crate::bambu::printer_status_to_live`].

use chrono::{DateTime, Utc};

use crate::{
    bambu::PrinterStatus,
    live::{ConnectionStatus, DeviceConnection, PrintActivity},
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MqttDeviceState {
    pub(crate) report: PrinterStatus,
    pub(crate) activity: PrintActivity,
    pub(crate) last_report_at: Option<DateTime<Utc>>,
    pub(crate) connection: DeviceConnection,
}

impl MqttDeviceState {
    #[cfg(test)]
    pub(crate) fn from_report(report: PrinterStatus) -> Self {
        let now = Utc::now();
        Self::from_snapshot(
            report,
            Some(now),
            DeviceConnection {
                key: None,
                status: ConnectionStatus::Connected,
                error: None,
            },
        )
    }

    pub(crate) fn from_snapshot(
        report: PrinterStatus,
        last_report_at: Option<DateTime<Utc>>,
        connection: DeviceConnection,
    ) -> Self {
        let activity = PrintActivity::from_status(report.status.as_deref());
        Self {
            report,
            activity,
            last_report_at,
            connection,
        }
    }

    pub(crate) fn is_fresh(&self) -> bool {
        self.connection.status == ConnectionStatus::Connected
    }

    pub(crate) fn is_active_task(&self) -> bool {
        self.is_fresh() && self.activity.is_active_task()
    }
}
