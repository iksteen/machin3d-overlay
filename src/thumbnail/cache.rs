use crate::{bambu::PrinterStatus, mqtt::MqttDeviceState};

use super::trimmed;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TaskKey(String);

impl TaskKey {
    pub(super) fn from_state(state: &MqttDeviceState) -> Option<Self> {
        state
            .is_active_task()
            .then(|| Self::from_report(&state.report))
            .flatten()
    }

    #[cfg(test)]
    pub(super) fn for_test(value: &str) -> Self {
        Self(value.to_owned())
    }

    fn from_report(report: &PrinterStatus) -> Option<Self> {
        let task_id = trimmed(report.task_id.as_deref());
        let filename = trimmed(report.filename.as_deref());
        let task_name = trimmed(report.task_name.as_deref());
        if task_id.is_none() && filename.is_none() && task_name.is_none() {
            return None;
        }

        Some(Self(format!(
            "{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}",
            task_id.unwrap_or_default(),
            filename.unwrap_or_default(),
            task_name.unwrap_or_default(),
            trimmed(report.start_time.as_deref()).unwrap_or_default(),
            trimmed(report.print_type.as_deref()).unwrap_or_default()
        )))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        bambu::PrinterStatus,
        mqtt::{MqttConnectionStatus, MqttDeviceConnection, MqttDeviceState},
    };

    use super::TaskKey;

    #[test]
    fn task_key_tracks_the_active_print_identity() {
        let report = PrinterStatus {
            task_id: Some("task-1".to_owned()),
            filename: Some("cube.3mf".to_owned()),
            task_name: Some("Cube".to_owned()),
            start_time: Some("2026-01-01".to_owned()),
            ..PrinterStatus::default()
        };

        assert!(TaskKey::from_report(&report).is_some());
        assert_eq!(TaskKey::from_report(&PrinterStatus::default()), None);
    }

    #[test]
    fn task_key_ignores_inactive_live_state() {
        let state = MqttDeviceState::from_report(PrinterStatus {
            status: Some("FINISH".to_owned()),
            task_id: Some("task-1".to_owned()),
            filename: Some("cube.3mf".to_owned()),
            task_name: Some("Cube".to_owned()),
            ..PrinterStatus::default()
        });

        assert_eq!(TaskKey::from_state(&state), None);
    }

    #[test]
    fn task_key_ignores_stale_live_state() {
        let state = MqttDeviceState::from_snapshot(
            PrinterStatus {
                status: Some("RUNNING".to_owned()),
                task_id: Some("task-1".to_owned()),
                filename: Some("cube.3mf".to_owned()),
                task_name: Some("Cube".to_owned()),
                ..PrinterStatus::default()
            },
            None,
            MqttDeviceConnection {
                key: Some("printer-a".to_owned()),
                status: MqttConnectionStatus::Disconnected,
                error: Some("disconnected".to_owned()),
            },
        );

        assert_eq!(TaskKey::from_state(&state), None);
    }
}
