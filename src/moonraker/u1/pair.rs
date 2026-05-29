//! Drive the U1 LAN pairing dance on the cleartext MQTT broker (`:1884`).
//!
//! The printer accepts an unauthenticated TCP/MQTT CONNECT and serves a
//! constant `12345678/config/{request,response,notification}` channel that
//! is gated by an on-screen approval popup. On approval, it returns
//! per-client TLS material (`ca`, `cert`, `key`, `sn`, `port`) on the
//! notification channel. This module owns the protocol; the CLI calls
//! [`pair`] and persists the resulting [`SnapToken`].

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use rumqttc::{AsyncClient, ConnectReturnCode, Event, EventLoop, MqttOptions, Packet, QoS};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::secret::Secret;

use super::auth::SnapToken;

/// The bootstrap channel is gated by a constant string that every client
/// shares (the printer authorizes by an on-screen tap, not by knowledge of
/// this value). Topics under this prefix are the only ones the cleartext
/// :1884 broker accepts publishes on.
const CONFIG_REQUEST_TOPIC: &str = "12345678/config/request";
const CONFIG_RESPONSE_TOPIC: &str = "12345678/config/response";
const CONFIG_NOTIFICATION_TOPIC: &str = "12345678/config/notification";
const KEEPALIVE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub(crate) struct PairConfig {
    pub host: String,
    pub clear_port: u16,
    /// Total time we wait for the user to tap "approve" before giving up.
    pub approval_timeout: Duration,
    /// Stable client identifier the printer keys its auth DB on. Pass an
    /// existing value (e.g. one loaded from an earlier `snap-pair`) to
    /// re-pair without re-tapping; pass [`fresh_clientid`] for a brand-new
    /// pairing.
    pub clientid: String,
}

/// Generate a fresh client identifier of the shape the printer expects.
/// Persist it (via [`SnapToken::clientid`]) so subsequent `snap-pair`
/// runs against the same printer skip the on-screen approval.
pub(crate) fn fresh_clientid() -> String {
    format!("overlay-{}", Uuid::new_v4())
}

pub(crate) async fn pair(
    config: PairConfig,
    mut on_progress: impl FnMut(&str) + Send,
) -> Result<SnapToken> {
    let bootstrap_clientid = format!("overlay-try-{}", unix_millis());
    let mut options = MqttOptions::new(bootstrap_clientid, config.host.clone(), config.clear_port);
    options.set_keep_alive(KEEPALIVE);
    options.set_clean_session(false);

    let (client, eventloop) = AsyncClient::new(options, 32);
    let approval_timeout = config.approval_timeout;
    let host = config.host.clone();
    let clientid = config.clientid.clone();

    let token = tokio::time::timeout(
        approval_timeout,
        run_bootstrap(client.clone(), eventloop, host, clientid, &mut on_progress),
    )
    .await
    .with_context(|| {
        format!(
            "timed out after {}s waiting for the printer's approval popup; tap Approve on the printer or retry",
            approval_timeout.as_secs()
        )
    })??;

    let _ = client.disconnect().await;
    Ok(token)
}

async fn run_bootstrap(
    client: AsyncClient,
    mut eventloop: EventLoop,
    host: String,
    clientid: String,
    on_progress: &mut (impl FnMut(&str) + Send),
) -> Result<SnapToken> {
    subscribe_bootstrap_topics(&client, &host).await?;
    publish_confirm(&client, &clientid).await?;

    let app_id = format!("overlay-{}", unix_millis());
    let mut sent_auth_request = false;
    let mut popped_up = false;

    loop {
        let event = eventloop
            .poll()
            .await
            .context("MQTT event loop failed during pairing")?;
        match event {
            Event::Incoming(Packet::ConnAck(ack)) if ack.code != ConnectReturnCode::Success => {
                bail!("printer rejected cleartext MQTT CONNECT: {:?}", ack.code);
            }
            Event::Incoming(Packet::Publish(publish)) => match publish.topic.as_str() {
                CONFIG_RESPONSE_TOPIC => {
                    let body: ConfigResponse = serde_json::from_slice(&publish.payload)
                        .with_context(|| {
                            format!(
                                "could not parse config-response payload: {}",
                                String::from_utf8_lossy(&publish.payload)
                            )
                        })?;
                    match body.result.state.as_str() {
                        "reject" | "rejected" => {
                            bail!(
                                "printer rejected the pairing request: {}",
                                body.result.message.unwrap_or_default()
                            );
                        }
                        state => {
                            // Any other state (`unauthorized` for a brand-new
                            // clientid, `success` when the printer just acks
                            // the connection, `authorizing` while a popup is
                            // up, or anything else firmware-specific) means
                            // "drive request_lan_auth and wait for the
                            // notification". Sending request_lan_auth is
                            // idempotent — we only do it once per session.
                            if !sent_auth_request {
                                sent_auth_request = true;
                                on_progress(&format!(
                                    "Printer reported state `{state}`; requesting authorization (tap Approve on the printer if it prompts you)..."
                                ));
                                publish_request_auth(&client, &clientid, &app_id).await?;
                            } else if state == "authorizing" && !popped_up {
                                popped_up = true;
                                on_progress(
                                    "→ tap Approve on the printer screen to authorize this overlay",
                                );
                            }
                        }
                    }
                }
                CONFIG_NOTIFICATION_TOPIC => {
                    let body: ConfigNotification = serde_json::from_slice(&publish.payload)
                        .with_context(|| {
                            format!(
                                "could not parse config-notification payload: {}",
                                String::from_utf8_lossy(&publish.payload)
                            )
                        })?;
                    if body.method != "notify_lan_auth" {
                        continue;
                    }
                    let entry = body
                        .params
                        .into_iter()
                        .find(|entry| entry.clientid == clientid)
                        .context("notify_lan_auth did not include our clientid")?;
                    if entry.state != "approve" {
                        bail!("printer authorization ended with state `{}`", entry.state);
                    }
                    return Ok(token_from_notification(&host, &clientid, entry));
                }
                _ => {}
            },
            _ => {}
        }
    }
}

async fn subscribe_bootstrap_topics(client: &AsyncClient, host: &str) -> Result<()> {
    for topic in [
        format!("{host}/status"),
        CONFIG_RESPONSE_TOPIC.to_owned(),
        format!("{host}/notification"),
        CONFIG_NOTIFICATION_TOPIC.to_owned(),
    ] {
        client
            .subscribe(topic.clone(), QoS::AtLeastOnce)
            .await
            .with_context(|| format!("failed to subscribe to {topic}"))?;
    }
    Ok(())
}

async fn publish_confirm(client: &AsyncClient, clientid: &str) -> Result<()> {
    let body = json!({
        "jsonrpc": "2.0",
        "method": "server.client_manager.confirm_lan_status",
        "params": {"clientid": clientid},
        "id": unix_millis(),
    });
    client
        .publish(
            CONFIG_REQUEST_TOPIC,
            QoS::AtLeastOnce,
            false,
            serde_json::to_vec(&body)?,
        )
        .await
        .context("failed to publish confirm_lan_status request")?;
    Ok(())
}

async fn publish_request_auth(client: &AsyncClient, clientid: &str, app_id: &str) -> Result<()> {
    let body = json!({
        "jsonrpc": "2.0",
        "method": "server.client_manager.request_lan_auth",
        "params": {"clientid": clientid, "app_id": app_id},
        "id": unix_millis(),
    });
    client
        .publish(
            CONFIG_REQUEST_TOPIC,
            QoS::AtLeastOnce,
            false,
            serde_json::to_vec(&body)?,
        )
        .await
        .context("failed to publish request_lan_auth request")?;
    Ok(())
}

fn token_from_notification(host: &str, clientid: &str, entry: NotifyAuthEntry) -> SnapToken {
    SnapToken {
        host: host.to_owned(),
        sn: entry.sn,
        clientid: clientid.to_owned(),
        mqtt_port: entry.port,
        ca: entry.ca,
        cert: entry.cert,
        key: Secret::new(entry.key),
    }
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

#[derive(Debug, Deserialize)]
struct ConfigResponse {
    result: ConfigResult,
}

#[derive(Debug, Deserialize)]
struct ConfigResult {
    state: String,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConfigNotification {
    method: String,
    #[serde(default)]
    params: Vec<NotifyAuthEntry>,
}

#[derive(Debug, Deserialize)]
struct NotifyAuthEntry {
    state: String,
    clientid: String,
    sn: String,
    ca: String,
    cert: String,
    key: String,
    port: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_unauthorized_response() {
        let json = r#"{"jsonrpc":"2.0","result":{"state":"unauthorized","clientid":"x","appid":"","message":"client is unauthorized"}}"#;
        let body: ConfigResponse = serde_json::from_str(json).unwrap();
        assert_eq!(body.result.state, "unauthorized");
    }

    #[test]
    fn parses_authorizing_response() {
        let json = r#"{"jsonrpc":"2.0","result":{"state":"authorizing","clientid":"x","appid":"y","message":"waiting user authorization"}}"#;
        let body: ConfigResponse = serde_json::from_str(json).unwrap();
        assert_eq!(body.result.state, "authorizing");
    }

    #[test]
    fn parses_notify_lan_auth() {
        let json = r#"{"jsonrpc":"2.0","method":"notify_lan_auth","params":[{"state":"approve","clientid":"overlay-x","sn":"SN1","ca":"CA","cert":"CERT","key":"KEY","port":8883,"app_id":"app1"}]}"#;
        let body: ConfigNotification = serde_json::from_str(json).unwrap();
        assert_eq!(body.method, "notify_lan_auth");
        assert_eq!(body.params.len(), 1);
        let entry = &body.params[0];
        assert_eq!(entry.state, "approve");
        assert_eq!(entry.sn, "SN1");
        assert_eq!(entry.port, 8883);
    }

    #[test]
    fn notification_to_token_carries_all_material() {
        let entry = NotifyAuthEntry {
            state: "approve".to_owned(),
            clientid: "overlay-x".to_owned(),
            sn: "SN1".to_owned(),
            ca: "CA".to_owned(),
            cert: "CERT".to_owned(),
            key: "KEY".to_owned(),
            port: 8883,
        };
        let token = token_from_notification("192.168.0.120", "overlay-x", entry);
        assert_eq!(token.host, "192.168.0.120");
        assert_eq!(token.sn, "SN1");
        assert_eq!(token.clientid, "overlay-x");
        assert_eq!(token.mqtt_port, 8883);
        assert_eq!(token.ca, "CA");
        assert_eq!(token.key.expose(), "KEY");
    }
}
