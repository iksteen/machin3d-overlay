use std::time::Duration;

use anyhow::Result;
use tracing::warn;

use crate::{
    bambu::{MQTT_HOST, MQTT_PORT},
    cloud::{cloud_mqtt_startup, CloudSession},
    devices::{resolve_devices, resolve_video_endpoints},
    local::{Endpoint, LocalDevice, LocalEndpointConfig, MqttEndpoint},
    mqtt::{supervise_target, MqttRuntime, MqttTarget},
    service::{wait_for_process_shutdown_signal, ServiceTasks, Shutdown},
    thumbnail::ThumbnailService,
    video::{VideoEndpoint, VideoStreams},
    web::{serve_http, AppState},
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
    let ServiceGraph {
        bind,
        state,
        mut tasks,
    } = ServiceGraph::build(cloud, config, shutdown.clone()).await?;

    let server = serve_http(bind, state, shutdown.clone());
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

struct ServiceGraph {
    bind: Endpoint,
    state: AppState,
    tasks: ServiceTasks,
}

struct BackgroundServices {
    mqtt: MqttRuntime,
    video: VideoStreams,
    thumbnail: ThumbnailService,
    cloud_mqtt: Option<MqttTarget>,
    local_devices: Vec<LocalDevice>,
}

impl ServiceGraph {
    async fn build(
        cloud: Option<CloudSession>,
        config: ServerConfig,
        shutdown: Shutdown,
    ) -> Result<Self> {
        let mqtt = MqttRuntime::new();
        let registry = resolve_devices(
            cloud.as_ref(),
            &config.cloud_devices,
            &config.local_devices,
            &config.video_endpoints,
        )
        .await?;
        let cloud_mqtt_ids = registry.cloud_mqtt_ids();
        let cloud_mqtt = cloud_mqtt_startup(cloud.as_ref(), &config.cloud_mqtt, &cloud_mqtt_ids)?
            .map(|startup| startup.into_target());
        let video_endpoints = resolve_video_endpoints(&registry).await?;
        let video = VideoStreams::new(registry.clone(), video_endpoints.endpoints_by_device)?;
        let thumbnail = ThumbnailService::new(mqtt.clone(), cloud.clone(), registry.clone());
        let local_devices = registry.local_devices();
        let state = AppState::new(
            mqtt.clone(),
            registry,
            video.clone(),
            thumbnail.clone(),
            shutdown.clone(),
        );
        let background = BackgroundServices {
            mqtt,
            video,
            thumbnail,
            cloud_mqtt,
            local_devices,
        };

        let mut tasks = ServiceTasks::new();
        background.spawn(&mut tasks, &shutdown);

        Ok(Self {
            bind: config.bind,
            state,
            tasks,
        })
    }
}

impl BackgroundServices {
    fn spawn(self, tasks: &mut ServiceTasks, shutdown: &Shutdown) {
        let video = self.video;
        tasks.spawn_with_shutdown(
            shutdown,
            "video worker watcher",
            move |shutdown| async move {
                video.watch_workers(shutdown).await;
            },
        );

        let thumbnail = self.thumbnail;
        tasks.spawn_with_shutdown(shutdown, "thumbnail watcher", move |shutdown| async move {
            thumbnail.watch_task_changes(shutdown).await;
        });

        if let Some(cloud_mqtt) = self.cloud_mqtt {
            let mqtt = self.mqtt.clone();
            tasks.spawn_with_shutdown(shutdown, "cloud MQTT supervisor", move |shutdown| {
                supervise_target(mqtt, cloud_mqtt, shutdown)
            });
        }

        for device in self.local_devices {
            let task_name = format!("local MQTT supervisor ({})", device.id);
            let mqtt = self.mqtt.clone();
            tasks.spawn_with_shutdown(shutdown, task_name, move |shutdown| {
                supervise_target(mqtt, MqttTarget::local(device), shutdown)
            });
        }
    }
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
