use anyhow::Result;

use crate::{
    bambu::{MQTT_HOST, MQTT_PORT},
    cloud::{cloud_mqtt_startup, start_cloud_mqtt, CloudSession},
    devices::{resolve_devices, resolve_video_endpoints},
    local::{Endpoint, LocalEndpointArg, MqttEndpoint},
    mqtt::{start_local_supervisors, MqttRuntime},
    thumbnail::ThumbnailRuntime,
    video::VideoEndpoint,
    web::{app_state, serve_http},
};

pub(crate) const DEFAULT_HOST: &str = "127.0.0.1";
pub(crate) const DEFAULT_PORT: u16 = 8765;

#[derive(Clone)]
pub(crate) struct ServerConfig {
    pub bind: Endpoint,
    pub cloud_mqtt: MqttEndpoint,
    pub local_devices: Vec<LocalEndpointArg>,
    pub cloud_devices: Vec<String>,
    pub video_endpoints: Vec<VideoEndpoint>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: Endpoint::new(DEFAULT_HOST, DEFAULT_PORT),
            cloud_mqtt: MqttEndpoint::new(MQTT_HOST, MQTT_PORT),
            local_devices: Vec::new(),
            cloud_devices: Vec::new(),
            video_endpoints: Vec::new(),
        }
    }
}

pub(crate) async fn serve(cloud: Option<CloudSession>, config: ServerConfig) -> Result<()> {
    let mqtt = MqttRuntime::new();
    let registry = resolve_devices(
        cloud.as_ref(),
        &config.cloud_devices,
        &config.local_devices,
        &config.video_endpoints,
    )
    .await?;
    let cloud_mqtt_ids = registry.cloud_mqtt_ids();
    let cloud_mqtt = cloud_mqtt_startup(cloud.as_ref(), &config.cloud_mqtt, &cloud_mqtt_ids)?;
    let video = resolve_video_endpoints(&registry).await?;
    let thumbnail = ThumbnailRuntime::new(mqtt.clone(), cloud.clone(), registry.clone());
    thumbnail.start();
    let local_devices = registry.local_devices();
    let state = app_state(mqtt.clone(), registry, video, thumbnail)?;

    start_cloud_mqtt(mqtt.clone(), cloud_mqtt);
    start_local_supervisors(mqtt, local_devices);
    serve_http(config.bind, state).await
}
