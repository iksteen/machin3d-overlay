//! Bambu-specific background wiring: cloud broker supervisor (shared
//! across all cloud devices) plus one local broker supervisor per local
//! device.
//!
//! Iterates the registry via [`DeviceRegistry::bambu_entries`] so
//! Snapmaker entries are never handed to a Bambu MQTT supervisor.

use anyhow::Result;

use crate::{
    bambu::{
        cloud::{cloud_mqtt_startup, CloudSession},
        local::BambuLocalDevice,
    },
    devices::DeviceRegistry,
    endpoint::MqttEndpoint,
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
        .bambu_entries()
        .filter(|(_, bambu)| bambu.cloud_mqtt)
        .map(|(entry, _)| entry.id().to_owned())
        .collect()
}

fn bambu_local_devices(registry: &DeviceRegistry) -> Vec<BambuLocalDevice> {
    registry
        .bambu_entries()
        .filter_map(|(_, bambu)| bambu.local_mqtt.clone())
        .collect()
}
