use anyhow::{Context, Result};

use crate::{
    bambu::{mqtt::MqttTarget, BambuClient, BambuCloudDevice},
    endpoint::MqttEndpoint,
    secret::Secret,
};

#[derive(Clone)]
pub struct CloudSession {
    pub client: BambuClient,
    pub access_token: Secret<String>,
    pub user_id: String,
}

pub(crate) struct CloudMqttStartup {
    pub(crate) endpoint: MqttEndpoint,
    pub(crate) user_id: String,
    pub(crate) access_token: Secret<String>,
    pub(crate) device_ids: Vec<String>,
}

impl CloudMqttStartup {
    pub(crate) fn into_target(self) -> MqttTarget {
        MqttTarget::cloud(
            self.endpoint,
            self.user_id,
            self.access_token,
            self.device_ids,
        )
    }
}

pub(crate) async fn bound_cloud_devices(
    cloud: Option<&CloudSession>,
) -> Result<Vec<BambuCloudDevice>> {
    let cloud = cloud.context("Bambu Cloud /bind metadata requires a Bambu Cloud token")?;
    let mut bound = cloud
        .client
        .bound_devices(cloud.access_token.expose())
        .await?;
    for device in &mut bound.devices {
        device.status = Default::default();
    }
    Ok(bound.devices)
}

pub(crate) fn explicit_cloud_devices(configs: &[String]) -> Vec<BambuCloudDevice> {
    configs
        .iter()
        .map(|device_id| explicit_cloud_device(device_id))
        .collect()
}

fn explicit_cloud_device(device_id: &str) -> BambuCloudDevice {
    BambuCloudDevice {
        id: Some(device_id.to_owned()),
        online: Some(true),
        ..BambuCloudDevice::default()
    }
}

pub(crate) fn cloud_mqtt_startup(
    cloud: Option<&CloudSession>,
    endpoint: &MqttEndpoint,
    device_ids: &[String],
) -> Result<Option<CloudMqttStartup>> {
    if device_ids.is_empty() {
        return Ok(None);
    }

    let cloud = cloud.with_context(|| {
        "cloud MQTT devices require a Bambu Cloud token; run `bambu-overlay login` or configure the device as --bbl-local-device"
    })?;
    Ok(Some(CloudMqttStartup {
        endpoint: endpoint.clone(),
        user_id: cloud.user_id.clone(),
        access_token: cloud.access_token.clone(),
        device_ids: device_ids.to_vec(),
    }))
}

#[cfg(test)]
mod tests {
    use super::{bound_cloud_devices, cloud_mqtt_startup, explicit_cloud_devices, Secret};
    use crate::{bambu::BambuClient, endpoint::MqttEndpoint};
    use std::time::Duration;

    fn mqtt_endpoint(value: &str) -> MqttEndpoint {
        value.parse().expect("MQTT endpoint should parse")
    }

    fn cloud_session(user_id: &str) -> super::CloudSession {
        super::CloudSession {
            client: BambuClient::new("https://example.invalid", Duration::from_secs(1)).unwrap(),
            access_token: Secret::new("access-token".to_owned()),
            user_id: user_id.to_owned(),
        }
    }

    #[test]
    fn cloud_mqtt_startup_skips_when_no_cloud_devices_exist() {
        let startup = cloud_mqtt_startup(None, &mqtt_endpoint("mqtt.example.test"), &[]).unwrap();

        assert!(startup.is_none());
    }

    #[test]
    fn cloud_mqtt_startup_requires_cloud_session_for_cloud_devices() {
        let error = match cloud_mqtt_startup(
            None,
            &mqtt_endpoint("mqtt.example.test"),
            &["printer-a".to_owned()],
        ) {
            Ok(_) => panic!("cloud MQTT startup should require a cloud session"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("Bambu Cloud token"));
    }

    #[test]
    fn cloud_mqtt_startup_uses_stored_user_id() {
        let startup = cloud_mqtt_startup(
            Some(&cloud_session("1234567890")),
            &mqtt_endpoint("mqtt.example.test"),
            &["printer-a".to_owned()],
        )
        .unwrap()
        .unwrap();

        assert_eq!(startup.user_id, "1234567890");
    }

    #[test]
    fn explicit_cloud_devices_do_not_require_cloud_session_for_catalog() {
        let devices = explicit_cloud_devices(&["printer-a".to_owned()]);

        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].id.as_deref(), Some("printer-a"));
        assert_eq!(devices[0].access_code, None);
    }

    #[test]
    fn explicit_cloud_devices_returns_empty_catalog_for_empty_config() {
        let devices = explicit_cloud_devices(&[]);

        assert!(devices.is_empty());
    }

    #[tokio::test]
    async fn cloud_metadata_requires_cloud_session() {
        let error = bound_cloud_devices(None).await.unwrap_err();

        assert!(error.to_string().contains("/bind metadata"));
        assert!(error.to_string().contains("Bambu Cloud token"));
    }
}
