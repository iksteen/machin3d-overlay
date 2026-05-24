use std::time::Duration;

use anyhow::{Context, Result};
use rumqttc::{MqttOptions, Transport};
use tracing::warn;
use uuid::Uuid;

use crate::{
    bambu::{device_tls, local::BambuLocalDevice},
    endpoint::MqttEndpoint,
    secret::Secret,
};

const KEEPALIVE: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub(crate) enum MqttTarget {
    Cloud {
        endpoint: MqttEndpoint,
        user_id: String,
        access_token: Secret<String>,
        device_ids: Vec<String>,
    },
    Local(BambuLocalDevice),
}

impl MqttTarget {
    pub(crate) fn cloud(
        endpoint: MqttEndpoint,
        user_id: String,
        access_token: Secret<String>,
        device_ids: Vec<String>,
    ) -> Self {
        Self::Cloud {
            endpoint,
            user_id,
            access_token,
            device_ids,
        }
    }

    pub(crate) fn local(device: BambuLocalDevice) -> Self {
        Self::Local(device)
    }

    pub(super) fn device_ids(&self) -> Vec<String> {
        match self {
            MqttTarget::Cloud { device_ids, .. } => device_ids.clone(),
            MqttTarget::Local(device) => vec![device.id.clone()],
        }
    }

    pub(super) fn connection_key(&self) -> String {
        match self {
            MqttTarget::Cloud { .. } => "cloud".to_owned(),
            MqttTarget::Local(device) => device.id.clone(),
        }
    }

    pub(super) fn options(&self) -> Result<MqttOptions> {
        match self {
            MqttTarget::Cloud {
                endpoint,
                user_id,
                access_token,
                ..
            } => cloud_mqtt_options(endpoint, user_id, access_token),
            MqttTarget::Local(device) => local_mqtt_options(device),
        }
    }

    pub(super) fn warn_disconnect(&self, error: &anyhow::Error, label: &'static str) {
        match self {
            MqttTarget::Cloud { .. } => warn!(%error, "{label}"),
            MqttTarget::Local(device) => {
                warn!(
                    device_id = %device.id,
                    host = %device.endpoint.host(),
                    error = %error,
                    "{label}"
                );
            }
        }
    }
}

fn cloud_mqtt_options(
    endpoint: &MqttEndpoint,
    user_id: &str,
    access_token: &Secret<String>,
) -> Result<MqttOptions> {
    let username = if user_id.starts_with("u_") {
        user_id.to_owned()
    } else {
        format!("u_{user_id}")
    };
    let mut options = MqttOptions::new(
        format!("bambu-overlay-{}", Uuid::new_v4()),
        endpoint.host.as_str(),
        endpoint.port,
    );
    options.set_keep_alive(KEEPALIVE);
    options.set_credentials(username, access_token.expose());
    options.set_transport(default_mqtt_transport()?);
    Ok(options)
}

fn local_mqtt_options(device: &BambuLocalDevice) -> Result<MqttOptions> {
    let mut options = MqttOptions::new(
        format!("bambu-overlay-{}", Uuid::new_v4()),
        device.endpoint.host(),
        device.endpoint.port(),
    );
    options.set_keep_alive(KEEPALIVE);
    options.set_credentials("bblp", device.endpoint.access_code());
    options.set_transport(local_mqtt_transport()?);
    Ok(options)
}

fn default_mqtt_transport() -> Result<Transport> {
    let connector =
        native_tls::TlsConnector::new().context("failed to build default MQTT TLS connector")?;
    Ok(Transport::tls_with_config(connector.into()))
}

fn local_mqtt_transport() -> Result<Transport> {
    Ok(Transport::tls_with_config(
        device_tls::native_connector()?.into(),
    ))
}
