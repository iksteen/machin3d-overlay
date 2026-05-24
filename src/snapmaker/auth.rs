//! Persisted Snapmaker LAN pairing material.
//!
//! Each entry stores the per-printer mutual-TLS material the printer hands
//! us during `snap-pair`: the printer's CA, our client cert+key, the
//! stable client identifier the printer keys its auth DB on, and the SN we
//! must use as the topic prefix once we reconnect over mTLS.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::OnceLock,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::{
    secret::Secret,
    secret_file::{state_path, write_atomic},
};

static DEFAULT_TOKEN_PATH: OnceLock<PathBuf> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SnapToken {
    /// Host the user paired against. Used as the lookup key from
    /// `--snap-device HOST`. Stored verbatim — case-sensitive match.
    pub host: String,
    /// Printer serial number returned in the auth response. This is the
    /// topic prefix for all per-device MQTT requests (`<sn>/request`).
    pub sn: String,
    /// Stable client identifier we present to the printer's auth manager.
    /// Reusing it across runs avoids re-tapping approve.
    pub clientid: String,
    /// TLS port the printer told us to reconnect on (always 8883 observed).
    pub mqtt_port: u16,
    pub ca: String,
    pub cert: String,
    pub key: Secret<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapTokenFile {
    #[serde(default)]
    devices: Vec<SnapToken>,
}

pub(crate) fn load_snap_tokens(path: &Path) -> Result<Vec<SnapToken>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = fs::read_to_string(path)
        .with_context(|| format!("could not read snap-token file {}", path.display()))?;
    let parsed: SnapTokenFile = serde_json::from_str(&text).with_context(|| {
        format!(
            "snap-token file {} is not a valid token JSON document",
            path.display()
        )
    })?;
    Ok(parsed.devices)
}

pub(crate) fn upsert_snap_token(path: &Path, token: SnapToken) -> Result<()> {
    let mut tokens = load_snap_tokens(path)?;
    if let Some(existing) = tokens.iter_mut().find(|entry| entry.host == token.host) {
        *existing = token;
    } else {
        tokens.push(token);
    }
    write_snap_tokens(path, &tokens)
}

pub(crate) fn default_snap_token_path() -> &'static Path {
    DEFAULT_TOKEN_PATH.get_or_init(|| state_path("snap-tokens.json"))
}

fn write_snap_tokens(path: &Path, tokens: &[SnapToken]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("could not create {}", parent.display()))?;
    }
    let file_struct = SnapTokenFile {
        devices: tokens.to_vec(),
    };
    let encoded = serde_json::to_vec_pretty(&file_struct)?;
    write_atomic(path, &encoded)
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
    };
    use uuid::Uuid;

    use super::{load_snap_tokens, upsert_snap_token, SnapToken};
    use crate::secret::Secret;

    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new(label: &str) -> Self {
            let dir = env::temp_dir().join(format!("bambu-overlay-{label}-{}", Uuid::new_v4()));
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn token(host: &str, sn: &str) -> SnapToken {
        SnapToken {
            host: host.to_owned(),
            sn: sn.to_owned(),
            clientid: "overlay-test".to_owned(),
            mqtt_port: 8883,
            ca: "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n".to_owned(),
            cert: "-----BEGIN CERTIFICATE-----\nCERT\n-----END CERTIFICATE-----\n".to_owned(),
            key: Secret::new(
                "-----BEGIN RSA PRIVATE KEY-----\nKEY\n-----END RSA PRIVATE KEY-----\n".to_owned(),
            ),
        }
    }

    #[test]
    fn load_returns_empty_when_file_missing() {
        let dir = ScratchDir::new("snap-auth-missing");
        let path = dir.path().join("snap-tokens.json");
        let tokens = load_snap_tokens(&path).unwrap();
        assert!(tokens.is_empty());
    }

    #[test]
    fn upsert_appends_then_overwrites_by_host() {
        let dir = ScratchDir::new("snap-auth-upsert");
        let path = dir.path().join("snap-tokens.json");

        upsert_snap_token(&path, token("a", "SN-A")).unwrap();
        upsert_snap_token(&path, token("b", "SN-B")).unwrap();
        upsert_snap_token(&path, token("a", "SN-A-NEW")).unwrap();

        let tokens = load_snap_tokens(&path).unwrap();
        assert_eq!(tokens.len(), 2);
        let a = tokens.iter().find(|t| t.host == "a").unwrap();
        let b = tokens.iter().find(|t| t.host == "b").unwrap();
        assert_eq!(a.sn, "SN-A-NEW");
        assert_eq!(b.sn, "SN-B");
    }

    #[test]
    fn key_is_redacted_in_debug_output() {
        let tok = token("a", "SN-A");
        let debug = format!("{:?}", tok);
        assert!(!debug.contains("KEY"));
        assert!(debug.contains("redacted"));
    }
}
