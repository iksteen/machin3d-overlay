//! Normalized state derived from raw MQTT printer reports.

use chrono::{DateTime, Utc};

use crate::bambu::PrinterStatus;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MqttDeviceState {
    pub(crate) report: PrinterStatus,
    pub(crate) activity: PrintActivity,
    pub(crate) last_report_at: Option<DateTime<Utc>>,
    pub(crate) connection: MqttDeviceConnection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MqttDeviceConnection {
    pub(crate) key: Option<String>,
    pub(crate) status: MqttConnectionStatus,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum MqttConnectionStatus {
    #[default]
    Disconnected,
    Connecting,
    Connected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PrintActivity {
    Idle,
    Running,
    Paused,
    Finished,
    Failed,
    Missing,
    Unknown(String),
}

impl MqttDeviceState {
    #[cfg(test)]
    pub(crate) fn from_report(report: PrinterStatus) -> Self {
        let now = Utc::now();
        Self::from_snapshot(
            report,
            Some(now),
            MqttDeviceConnection {
                key: None,
                status: MqttConnectionStatus::Connected,
                error: None,
            },
        )
    }

    pub(crate) fn from_snapshot(
        report: PrinterStatus,
        last_report_at: Option<DateTime<Utc>>,
        connection: MqttDeviceConnection,
    ) -> Self {
        let activity = PrintActivity::from_report(&report);
        Self {
            report,
            activity,
            last_report_at,
            connection,
        }
    }

    pub(crate) fn is_fresh(&self) -> bool {
        self.connection.status == MqttConnectionStatus::Connected
    }

    pub(crate) fn is_active_task(&self) -> bool {
        self.is_fresh() && self.activity.is_active_task()
    }
}

impl MqttConnectionStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Connecting => "connecting",
            Self::Connected => "connected",
        }
    }
}

impl PrintActivity {
    pub(crate) fn from_report(report: &PrinterStatus) -> Self {
        Self::from_gcode_state(report.status.as_deref())
    }

    pub(crate) fn from_gcode_state(value: Option<&str>) -> Self {
        let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
            return Self::Missing;
        };
        match value.to_ascii_uppercase().as_str() {
            "IDLE" => Self::Idle,
            "RUNNING" => Self::Running,
            "PAUSED" => Self::Paused,
            "FINISH" => Self::Finished,
            "FAILED" => Self::Failed,
            _ => Self::Unknown(value.to_owned()),
        }
    }

    pub(crate) fn is_active_task(&self) -> bool {
        matches!(self, Self::Running | Self::Paused)
    }
}

#[cfg(test)]
mod tests {
    use crate::bambu::PrinterStatus;

    use super::{MqttDeviceState, PrintActivity};

    #[test]
    fn print_activity_classifies_known_gcode_states() {
        assert!(PrintActivity::from_gcode_state(Some("RUNNING")).is_active_task());
        assert!(PrintActivity::from_gcode_state(Some("PAUSED")).is_active_task());
        assert!(!PrintActivity::from_gcode_state(Some("IDLE")).is_active_task());
        assert!(!PrintActivity::from_gcode_state(Some("FINISH")).is_active_task());
        assert!(!PrintActivity::from_gcode_state(Some("FAILED")).is_active_task());
        assert!(!PrintActivity::from_gcode_state(None).is_active_task());
    }

    #[test]
    fn mqtt_device_state_preserves_raw_report() {
        let state = MqttDeviceState::from_report(PrinterStatus {
            status: Some("RUNNING".to_owned()),
            task_name: Some("Calibration cube".to_owned()),
            ..PrinterStatus::default()
        });

        assert_eq!(state.activity, PrintActivity::Running);
        assert!(state.is_fresh());
        assert!(state.is_active_task());
        assert_eq!(state.report.task_name.as_deref(), Some("Calibration cube"));
    }
}
