//! Per-device live state envelope: the latest report plus the connection
//! health we observed when we received it.

use chrono::{DateTime, Utc};

use super::PrinterReport;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DeviceLiveState {
    pub(crate) report: PrinterReport,
    pub(crate) activity: PrintActivity,
    pub(crate) last_report_at: Option<DateTime<Utc>>,
    pub(crate) connection: DeviceConnection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeviceConnection {
    pub(crate) key: Option<String>,
    pub(crate) status: ConnectionStatus,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ConnectionStatus {
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

impl DeviceLiveState {
    #[cfg(test)]
    pub(crate) fn from_report(report: PrinterReport) -> Self {
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
        report: PrinterReport,
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

impl ConnectionStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Connecting => "connecting",
            Self::Connected => "connected",
        }
    }
}

impl PrintActivity {
    pub(crate) fn from_status(value: Option<&str>) -> Self {
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
    use super::{DeviceLiveState, PrintActivity, PrinterReport};

    #[test]
    fn print_activity_classifies_known_statuses() {
        assert!(PrintActivity::from_status(Some("RUNNING")).is_active_task());
        assert!(PrintActivity::from_status(Some("PAUSED")).is_active_task());
        assert!(!PrintActivity::from_status(Some("IDLE")).is_active_task());
        assert!(!PrintActivity::from_status(Some("FINISH")).is_active_task());
        assert!(!PrintActivity::from_status(Some("FAILED")).is_active_task());
        assert!(!PrintActivity::from_status(None).is_active_task());
    }

    #[test]
    fn device_live_state_preserves_report() {
        let state = DeviceLiveState::from_report(PrinterReport {
            status: Some("RUNNING".to_owned()),
            task_name: Some("Calibration cube".to_owned()),
            ..PrinterReport::default()
        });

        assert_eq!(state.activity, PrintActivity::Running);
        assert!(state.is_fresh());
        assert!(state.is_active_task());
        assert_eq!(state.report.task_name.as_deref(), Some("Calibration cube"));
    }
}
