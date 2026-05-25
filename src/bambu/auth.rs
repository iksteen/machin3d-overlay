use std::{
    fs,
    path::{Path, PathBuf},
    sync::OnceLock,
};

use anyhow::{bail, Context, Result};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    bambu::LoginResponse,
    secret::Secret,
    secret_file::{state_path, write_atomic},
};

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
    write_atomic(&path, &encoded)?;

    Ok(path)
}

pub fn load_token(token_file: Option<PathBuf>) -> Result<TokenData> {
    let path = token_path(token_file);
    if !path.exists() {
        bail!(
            "no cached Bambu token found at {}. Run `machin3d-overlay bbl-login` first",
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
    DEFAULT_TOKEN_PATH.get_or_init(|| state_path("token.json"))
}
