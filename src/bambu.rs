pub(crate) mod auth;
pub(crate) mod backend;
mod client;
pub(crate) mod cloud;
mod de;
pub(crate) mod device_tls;
mod error;
pub(crate) mod local;
mod models;
mod report;

pub use client::BambuClient;
pub use models::{
    AmsState, AmsUnit, BambuCloudDevice, DeviceListResponse, LoginResponse, PrinterStatus, Task,
    TasksResponse, Tray, UserPreference,
};
pub(crate) use report::to_live as printer_status_to_live;

pub const API_BASE: &str = "https://api.bambulab.com";
pub const MQTT_HOST: &str = "us.mqtt.bambulab.com";
pub const MQTT_PORT: u16 = 8883;

const USER_AGENT: &str = "bambu-overlay/0.1";
