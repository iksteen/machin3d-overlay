//! Normalized state derived from raw MQTT printer reports.

use crate::bambu::PrinterStatus;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MqttDeviceState {
    pub(crate) report: PrinterStatus,
    pub(crate) activity: PrintActivity,
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
    pub(crate) fn from_report(report: PrinterStatus) -> Self {
        let activity = PrintActivity::from_report(&report);
        Self { report, activity }
    }

    pub(crate) fn is_active_task(&self) -> bool {
        self.activity.is_active_task()
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
        assert_eq!(state.report.task_name.as_deref(), Some("Calibration cube"));
    }
}
