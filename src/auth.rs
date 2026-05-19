use std::{
    env,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::OnceLock,
};

use anyhow::{bail, Context, Result};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use crate::{bambu::LoginResponse, secret::Secret};

static DEFAULT_TOKEN_PATH: OnceLock<PathBuf> = OnceLock::new();

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenData {
    pub access_token: Secret<String>,
    pub api_base: Option<String>,
    pub uid: String,
    pub expires_at: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SavedToken<'a> {
    access_token: &'a Secret<String>,
    refresh_token: Option<&'a Secret<String>>,
    uid: &'a str,
    created_at: String,
    api_base: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_in: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<String>,
}

pub fn save_token(
    login_response: &LoginResponse,
    token_file: Option<PathBuf>,
    api_base: &str,
    uid: &str,
) -> Result<PathBuf> {
    let access_token = login_response
        .access_token
        .as_ref()
        .filter(|token| !token.expose().is_empty())
        .context("cannot save token: login response did not include accessToken")?;
    let uid = (!uid.trim().is_empty())
        .then_some(uid.trim())
        .context("cannot save token: user preference did not include uid")?;

    let now = Utc::now();
    let token_data = SavedToken {
        access_token,
        refresh_token: login_response.refresh_token.as_ref(),
        uid,
        created_at: now.to_rfc3339(),
        api_base,
        expires_in: login_response.expires_in,
        expires_at: login_response
            .expires_in
            .map(|expires_in| (now + Duration::seconds(expires_in)).to_rfc3339()),
    };

    let path = token_path(token_file);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("could not create {}", parent.display()))?;
    }
    let encoded = serde_json::to_vec_pretty(&token_data)?;
    write_token_file(&path, &encoded)?;

    Ok(path)
}

pub fn load_token(token_file: Option<PathBuf>) -> Result<TokenData> {
    let path = token_path(token_file);
    if !path.exists() {
        bail!(
            "no cached Bambu token found at {}. Run `bambu-overlay login` first",
            path.display()
        );
    }
    let text = fs::read_to_string(&path)
        .with_context(|| format!("could not read token file {}", path.display()))?;
    let parsed: TokenData = serde_json::from_str(&text).with_context(|| {
        format!(
            "token file {} is not a valid token JSON object",
            path.display()
        )
    })?;
    Ok(parsed)
}

pub fn token_path(token_file: Option<PathBuf>) -> PathBuf {
    if let Some(token_file) = token_file {
        return token_file;
    }
    default_token_path().to_path_buf()
}

pub fn default_token_path() -> &'static Path {
    DEFAULT_TOKEN_PATH.get_or_init(resolve_default_token_path)
}

fn resolve_default_token_path() -> PathBuf {
    if let Ok(xdg_state_home) = env::var("XDG_STATE_HOME") {
        return PathBuf::from(xdg_state_home)
            .join("bambu-overlay")
            .join("token.json");
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local")
        .join("state")
        .join("bambu-overlay")
        .join("token.json")
}

fn write_token_file(path: &Path, encoded: &[u8]) -> Result<()> {
    let temp_path = temporary_token_path(path);
    let cleanup = TempFileCleanup::new(temp_path.clone());
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);

    let mut file = options
        .open(&temp_path)
        .with_context(|| format!("could not create token file {}", temp_path.display()))?;
    file.write_all(encoded)
        .with_context(|| format!("could not write token file {}", temp_path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("could not write token file {}", temp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("could not sync token file {}", temp_path.display()))?;
    drop(file);

    fs::rename(&temp_path, path).with_context(|| {
        format!(
            "could not replace token file {} with {}",
            path.display(),
            temp_path.display()
        )
    })?;
    cleanup.disarm();
    Ok(())
}

fn temporary_token_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("token");
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
