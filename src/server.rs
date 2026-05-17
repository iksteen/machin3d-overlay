use std::time::Duration;

use anyhow::Result;
use tracing::warn;

use crate::{
    bambu::{MQTT_HOST, MQTT_PORT},
    cloud::{cloud_mqtt_startup, CloudSession},
    devices::{resolve_devices, resolve_video_endpoints},
    local::{Endpoint, LocalEndpointConfig, MqttEndpoint},
    mqtt::{supervise_target, MqttRuntime, MqttTarget},
    service::{wait_for_process_shutdown_signal, ServiceTasks, Shutdown},
    thumbnail::ThumbnailRuntime,
    video::{VideoEndpoint, VideoRuntime},
    web::{app_state, serve_http},
};

pub(crate) const DEFAULT_HOST: &str = "127.0.0.1";
pub(crate) const DEFAULT_PORT: u16 = 8765;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct ServerConfig {
    pub bind: Endpoint,
    pub cloud_mqtt: MqttEndpoint,
    pub local_devices: Vec<LocalEndpointConfig>,
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
    let shutdown = Shutdown::new();
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
    let video_endpoints = resolve_video_endpoints(&registry).await?;
    let video = VideoRuntime::new(
        registry.clone(),
        video_endpoints.endpoints,
        video_endpoints.endpoint_map,
    )?;
    let video_watcher = video.clone();
    let thumbnail = ThumbnailRuntime::new(mqtt.clone(), cloud.clone(), registry.clone());
    let thumbnail_watcher = thumbnail.clone();
    let local_devices = registry.local_devices();
    let state = app_state(mqtt.clone(), registry, video, thumbnail, shutdown.clone());

    let mut tasks = ServiceTasks::new();
    let video_watcher_shutdown = shutdown.subscribe();
    tasks.spawn("video worker watcher", async move {
        video_watcher.watch_workers(video_watcher_shutdown).await;
    });
    let thumbnail_shutdown = shutdown.subscribe();
    tasks.spawn("thumbnail watcher", async move {
        thumbnail_watcher
            .watch_task_changes(thumbnail_shutdown)
            .await;
    });

    if let Some(cloud_mqtt) = cloud_mqtt {
        let mqtt_shutdown = shutdown.subscribe();
        tasks.spawn(
            "cloud MQTT supervisor",
            supervise_target(mqtt.clone(), cloud_mqtt.into_target(), mqtt_shutdown),
        );
    }

    for device in local_devices {
        let task_name = format!("local MQTT supervisor ({})", device.id);
        let mqtt_shutdown = shutdown.subscribe();
        tasks.spawn(
            task_name,
            supervise_target(mqtt.clone(), MqttTarget::local(device), mqtt_shutdown),
        );
    }

    let server = serve_http(config.bind, state, shutdown.clone());
    tokio::pin!(server);

    let (mut result, server_finished) = match tokio::select! {
        result = &mut server => ServeStop::Server(result),
        result = tasks.wait_for_failure() => ServeStop::Background(result),
        _ = wait_for_process_shutdown_signal() => ServeStop::Signal,
    } {
        ServeStop::Server(result) => (result, true),
        ServeStop::Background(result) => (result, false),
        ServeStop::Signal => (Ok(()), false),
    };

    shutdown.trigger();

    if !server_finished {
        result = combine_shutdown_result(result, (&mut server).await, "HTTP server");
    }
    result = combine_shutdown_result(
        result,
        tasks.shutdown(SHUTDOWN_GRACE).await,
        "background tasks",
    );
    result
}

enum ServeStop {
    Server(Result<()>),
    Background(Result<()>),
    Signal,
}

fn combine_shutdown_result(
    primary: Result<()>,
    shutdown_result: Result<()>,
    label: &str,
) -> Result<()> {
    match (primary, shutdown_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Ok(())) => Err(error),
        (Err(primary), Err(shutdown_error)) => {
            warn!(
                error = %shutdown_error,
                "{label} shutdown failed after an earlier service error"
            );
            Err(primary)
        }
    }
}
