use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
};
use minijinja::{context, Environment};
use serde_json::json;

use crate::assets;

use super::{default_device_id, device_not_found, known_device_id, AppState};

pub(super) async fn horizontal_overlay(
    State(state): State<AppState>,
) -> Result<Html<String>, Response> {
    render_overlay("horizontal", default_device_id(&state))
        .map(Html)
        .map_err(render_error)
}

pub(super) async fn horizontal_device_overlay(
    State(state): State<AppState>,
    Path(device_id): Path<String>,
) -> Result<Html<String>, Response> {
    let Some(device_id) = known_device_id(&state, &device_id) else {
        return Err(device_not_found(&device_id));
    };
    render_overlay("horizontal", Some(device_id))
        .map(Html)
        .map_err(render_error)
}

pub(super) async fn vertical_overlay(
    State(state): State<AppState>,
) -> Result<Html<String>, Response> {
    render_overlay("vertical", default_device_id(&state))
        .map(Html)
        .map_err(render_error)
}

pub(super) async fn vertical_device_overlay(
    State(state): State<AppState>,
    Path(device_id): Path<String>,
) -> Result<Html<String>, Response> {
    let Some(device_id) = known_device_id(&state, &device_id) else {
        return Err(device_not_found(&device_id));
    };
    render_overlay("vertical", Some(device_id))
        .map(Html)
        .map_err(render_error)
}

pub(super) async fn static_asset(Path(file): Path<String>) -> Response {
    match file.as_str() {
        "common.css" => asset_response("text/css; charset=utf-8", assets::COMMON_CSS),
        "horizontal.css" => asset_response("text/css; charset=utf-8", assets::HORIZONTAL_CSS),
        "vertical.css" => asset_response("text/css; charset=utf-8", assets::VERTICAL_CSS),
        "overlay.js" => asset_response("application/javascript; charset=utf-8", assets::OVERLAY_JS),
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

fn render_overlay(view_mode: &str, selected_device_id: Option<&str>) -> Result<String> {
    let mut env = Environment::new();
    env.add_template("overlay.html", assets::OVERLAY_HTML)?;
    let template = env.get_template("overlay.html")?;
    let view_mode = if view_mode == "vertical" {
        "vertical"
    } else {
        "horizontal"
    };
    let config_json = serde_json::to_string(&json!({
        "eventsUrl": "/api/current-print/events",
        "selectedDeviceId": selected_device_id,
    }))?;
    Ok(template.render(context! {
        view_mode => view_mode,
        config_json => config_json,
    })?)
}

fn asset_response(content_type: &'static str, body: &'static str) -> Response {
    ([(header::CONTENT_TYPE, content_type)], body).into_response()
}

fn render_error(error: anyhow::Error) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        error.to_string(),
    )
        .into_response()
}
