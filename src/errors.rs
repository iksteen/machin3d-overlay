//! Helpers for rendering errors for human-readable log output.

/// Flatten an `anyhow::Error`'s cause chain into a single
/// `"top-level: cause-1: cause-2: ..."` string for `tracing` fields.
pub(crate) fn error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}
