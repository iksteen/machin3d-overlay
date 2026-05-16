use std::io;

use crate::bambu::PrinterStatus;
use zip::result::ZipError;

pub(super) fn local_cloud_3mf_is_preparing(report: &PrinterStatus) -> bool {
    is_cloud_print(report)
        && report
            .file_prepare_percent
            .is_some_and(|percent| percent < 100.0)
}

pub(super) fn local_cloud_3mf_prepare_message(report: &PrinterStatus) -> String {
    match report.file_prepare_percent {
        Some(percent) => format!("printer is still preparing cloud 3MF ({percent:.0}%)"),
        None => "printer may still be preparing cloud 3MF".to_owned(),
    }
}

pub(super) fn local_cloud_3mf_may_still_be_preparing(
    report: &PrinterStatus,
    error: &anyhow::Error,
) -> bool {
    is_cloud_print(report) && is_incomplete_3mf_error(error)
}

fn is_cloud_print(report: &PrinterStatus) -> bool {
    report
        .print_type
        .as_deref()
        .map(str::trim)
        .is_some_and(|print_type| print_type.eq_ignore_ascii_case("cloud"))
}

fn is_incomplete_3mf_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<ZipError>()
            .is_some_and(|error| matches!(error, ZipError::InvalidArchive(_) | ZipError::Io(_)))
            || cause
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::UnexpectedEof)
    })
}

#[cfg(test)]
mod tests {
    use crate::bambu::PrinterStatus;
    use zip::result::ZipError;

    use super::{local_cloud_3mf_is_preparing, local_cloud_3mf_may_still_be_preparing};

    #[test]
    fn local_cloud_3mf_prepare_percent_defers_thumbnail_fetch() {
        let report = PrinterStatus {
            print_type: Some("cloud".to_owned()),
            file_prepare_percent: Some(99.0),
            ..PrinterStatus::default()
        };
        assert!(local_cloud_3mf_is_preparing(&report));

        let report = PrinterStatus {
            print_type: Some("cloud".to_owned()),
            file_prepare_percent: Some(100.0),
            ..PrinterStatus::default()
        };
        assert!(!local_cloud_3mf_is_preparing(&report));

        let report = PrinterStatus {
            print_type: Some("local".to_owned()),
            file_prepare_percent: Some(99.0),
            ..PrinterStatus::default()
        };
        assert!(!local_cloud_3mf_is_preparing(&report));
    }

    #[test]
    fn invalid_cloud_3mf_is_treated_as_still_preparing() {
        let error = anyhow::Error::new(ZipError::InvalidArchive(
            "could not find central directory".into(),
        ));
        let report = PrinterStatus {
            print_type: Some("cloud".to_owned()),
            file_prepare_percent: Some(100.0),
            ..PrinterStatus::default()
        };
        assert!(local_cloud_3mf_may_still_be_preparing(&report, &error));

        let report = PrinterStatus {
            print_type: Some("local".to_owned()),
            file_prepare_percent: Some(100.0),
            ..PrinterStatus::default()
        };
        assert!(!local_cloud_3mf_may_still_be_preparing(&report, &error));
    }
}
