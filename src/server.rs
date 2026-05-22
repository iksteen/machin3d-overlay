use std::time::Duration;

use anyhow::Result;
use tracing::warn;

use crate::{
    bambu,
    bambu::{MQTT_HOST, MQTT_PORT},
    cloud::CloudSession,
    devices::{resolve_devices, resolve_video_endpoints},
    live::LiveStateStore,
    local::{Endpoint, LocalEndpointConfig, MqttEndpoint},
    mqtt::MqttRuntime,
    service::{wait_for_process_shutdown_signal, ServiceTasks, Shutdown},
    snapmaker::{self, SnapmakerDeviceConfig},
    thumbnail::ThumbnailService,
    video::{self, VideoEndpoint, VideoStreams, VideoWorkerEvents},
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
    pub snapmaker_devices: Vec<SnapmakerDeviceConfig>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: Endpoint::new(DEFAULT_HOST, DEFAULT_PORT),
            cloud_mqtt: MqttEndpoint::new(MQTT_HOST, MQTT_PORT),
            local_devices: Vec::new(),
            cloud_devices: Vec::new(),
            video_endpoints: Vec::new(),
            snapmaker_devices: Vec::new(),
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

impl ServiceGraph {
    async fn build(
        cloud: Option<CloudSession>,
        config: ServerConfig,
        shutdown: Shutdown,
    ) -> Result<Self> {
        let live = LiveStateStore::new();
        let mqtt = MqttRuntime::new(live.clone());
        let registry = resolve_devices(
            cloud.as_ref(),
            &config.cloud_devices,
            &config.local_devices,
            &config.video_endpoints,
            &config.snapmaker_devices,
        )
        .await?;
        let video_endpoints = resolve_video_endpoints(&registry).await?;
        let video_sources =
            video::collect_sources(&registry, &video_endpoints.endpoints_by_device)?;
        let (video, video_worker_events) = VideoStreams::new(video_sources, shutdown.clone())?;
        let thumbnail =
            ThumbnailService::new(mqtt.clone(), live.clone(), cloud.clone(), registry.clone());
        let state = AppState::new(
            live.clone(),
            registry.clone(),
            video.clone(),
            thumbnail.clone(),
            shutdown.clone(),
        );

        let mut tasks = ServiceTasks::new();
        bambu::backend::spawn(
            mqtt,
            cloud.as_ref(),
            &config.cloud_mqtt,
            &registry,
            &mut tasks,
            &shutdown,
        )?;
        snapmaker::backend::spawn(live, &registry, &mut tasks, &shutdown);
        spawn_shared_workers(video, video_worker_events, thumbnail, &mut tasks, &shutdown);

        Ok(Self {
            bind: config.bind,
            state,
            tasks,
        })
    }
}

fn spawn_shared_workers(
    video: VideoStreams,
    video_worker_events: VideoWorkerEvents,
    thumbnail: ThumbnailService,
    tasks: &mut ServiceTasks,
    shutdown: &Shutdown,
) {
    tasks.spawn_with_shutdown(
        shutdown,
        "video worker watcher",
        move |shutdown| async move {
            video.watch_workers(video_worker_events, shutdown).await;
        },
    );
    tasks.spawn_with_shutdown(shutdown, "thumbnail watcher", move |shutdown| async move {
        thumbnail.watch_task_changes(shutdown).await;
    });
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
