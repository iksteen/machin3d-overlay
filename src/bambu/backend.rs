//! Bambu-specific background wiring: cloud broker supervisor (shared across
//! all cloud devices) plus one local broker supervisor per local device.
//!
//! Filters the registry on [`Backend::Bambu`] so a future Snapmaker entry is
//! never handed to a Bambu MQTT supervisor.

use anyhow::Result;

use crate::{
    backend::Backend,
    cloud::{cloud_mqtt_startup, CloudSession},
    devices::DeviceRegistry,
    local::{LocalDevice, MqttEndpoint},
    mqtt::{supervise_target, MqttRuntime, MqttTarget},
    service::{ServiceTasks, Shutdown},
};

pub(crate) fn spawn(
    runtime: MqttRuntime,
    cloud: Option<&CloudSession>,
    cloud_mqtt_endpoint: &MqttEndpoint,
    registry: &DeviceRegistry,
    tasks: &mut ServiceTasks,
    shutdown: &Shutdown,
) -> Result<()> {
    let cloud_mqtt_ids = bambu_cloud_mqtt_ids(registry);
    if let Some(target) = cloud_mqtt_startup(cloud, cloud_mqtt_endpoint, &cloud_mqtt_ids)?
        .map(|startup| startup.into_target())
    {
        let runtime = runtime.clone();
        tasks.spawn_with_shutdown(shutdown, "cloud MQTT supervisor", move |shutdown| {
            supervise_target(runtime, target, shutdown)
        });
    }

    for device in bambu_local_devices(registry) {
        let task_name = format!("local MQTT supervisor ({})", device.id);
        let runtime = runtime.clone();
        tasks.spawn_with_shutdown(shutdown, task_name, move |shutdown| {
            supervise_target(runtime, MqttTarget::local(device), shutdown)
        });
    }
    Ok(())
}

fn bambu_cloud_mqtt_ids(registry: &DeviceRegistry) -> Vec<String> {
    registry
        .entries()
        .iter()
        .filter(|entry| entry.backend() == Backend::Bambu && entry.has_cloud_mqtt())
        .map(|entry| entry.id().to_owned())
        .collect()
}

fn bambu_local_devices(registry: &DeviceRegistry) -> Vec<LocalDevice> {
    registry
        .entries()
        .iter()
        .filter(|entry| entry.backend() == Backend::Bambu)
        .filter_map(|entry| entry.local().cloned())
        .collect()
}
