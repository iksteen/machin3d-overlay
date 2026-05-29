//! Persisted credential files: state-directory resolution + atomic write.
//!
//! Both `bambu::auth` (Bambu Cloud token) and `moonraker::auth`
//! (Snapmaker mTLS pairing material) live as JSON under
//! `$XDG_STATE_HOME/machin3d-overlay/` and need the same durability +
//! permission story: write to a temp file under the same directory, sync
//! to disk, rename into place atomically; clean up the temp file on any
//! failure path; restrict permissions to the owning user on creation.

use std::{
    env,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

/// Path to a per-app credential file under the user's XDG state
/// directory: `$XDG_STATE_HOME/machin3d-overlay/<file_name>`, falling back
/// to `~/.local/state/machin3d-overlay/<file_name>` (or the current
/// directory if even `$HOME` is unset).
pub(crate) fn state_path(file_name: &str) -> PathBuf {
    if let Ok(xdg_state_home) = env::var("XDG_STATE_HOME") {
        return PathBuf::from(xdg_state_home)
            .join("machin3d-overlay")
            .join(file_name);
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local")
        .join("state")
        .join("machin3d-overlay")
        .join(file_name)
}

/// Write `encoded` (plus a trailing newline) to `path` atomically. The
/// file is created with mode `0o600` on unix. On any failure between
/// create and rename, the temp file is removed.
pub(crate) fn write_atomic(path: &Path, encoded: &[u8]) -> Result<()> {
    let temp_path = temporary_path(path);
    let cleanup = TempFileCleanup::new(temp_path.clone());
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);

    let mut file = options
        .open(&temp_path)
        .with_context(|| format!("could not create {}", temp_path.display()))?;
    file.write_all(encoded)
        .with_context(|| format!("could not write {}", temp_path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("could not write {}", temp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("could not sync {}", temp_path.display()))?;
    drop(file);

    fs::rename(&temp_path, path).with_context(|| {
        format!(
            "could not replace {} with {}",
            path.display(),
            temp_path.display()
        )
    })?;
    cleanup.disarm();
    Ok(())
}

fn temporary_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");
    path.with_file_name(format!(".{file_name}.{}.tmp", Uuid::new_v4()))
}

struct TempFileCleanup {
    path: PathBuf,
    armed: bool,
}

impl TempFileCleanup {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for TempFileCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}
