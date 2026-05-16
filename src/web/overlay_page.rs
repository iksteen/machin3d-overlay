use anyhow::Result;
use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
};
use minijinja::{context, Environment};
use serde_json::json;

use crate::assets;

pub(super) async fn horizontal_overlay() -> Result<Html<String>, Response> {
    render_overlay("horizontal").map(Html).map_err(render_error)
}

pub(super) async fn vertical_overlay() -> Result<Html<String>, Response> {
    render_overlay("vertical").map(Html).map_err(render_error)
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

fn render_overlay(view_mode: &str) -> Result<String> {
    let mut env = Environment::new();
    env.add_template("overlay.html", assets::OVERLAY_HTML)?;
    let template = env.get_template("overlay.html")?;
    let view_mode = if view_mode == "vertical" {
        "vertical"
    } else {
        "horizontal"
    };
    let config_json = serde_json::to_string(&json!({"eventsUrl": "/api/current-print/events"}))?;
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
