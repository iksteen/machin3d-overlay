//! Parsing for `--snap-device SERIAL=HOST[:PORT]`.

use std::str::FromStr;

use crate::local::endpoint::parse_endpoint;

use super::{SnapmakerDevice, SnapmakerEndpoint};

const DEFAULT_MOONRAKER_PORT: u16 = 80;

#[derive(Debug, Clone)]
pub(crate) struct SnapmakerDeviceConfig {
    pub(crate) device: SnapmakerDevice,
}

impl FromStr for SnapmakerDeviceConfig {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        let Some((serial, host_port)) = value.split_once('=') else {
            return Err(format!(
                "invalid --snap-device `{value}`: expected SERIAL=HOST[:PORT]"
            ));
        };
        let serial = serial.trim();
        if serial.is_empty() {
            return Err(format!(
                "invalid --snap-device `{value}`: serial number is empty"
            ));
        }
        let host_port = host_port.trim();
        if host_port.is_empty() {
            return Err(format!(
                "invalid --snap-device `{value}`: host is empty"
            ));
        }
        let endpoint: SnapmakerEndpoint = parse_endpoint(
            host_port,
            value,
            "Snapmaker endpoint",
            DEFAULT_MOONRAKER_PORT,
        )?;
        Ok(Self {
            device: SnapmakerDevice {
                serial: serial.to_owned(),
                endpoint,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SnapmakerDeviceConfig;

    #[test]
    fn parses_serial_host_pair() {
        let parsed: SnapmakerDeviceConfig = "8110026040110371KB88=192.168.0.120"
            .parse()
            .expect("simple form should parse");
        assert_eq!(parsed.device.serial, "8110026040110371KB88");
        assert_eq!(parsed.device.endpoint.host, "192.168.0.120");
        assert_eq!(parsed.device.endpoint.port, 80);
    }

    #[test]
    fn parses_custom_port() {
        let parsed: SnapmakerDeviceConfig = "U1=printer.local:7125".parse().unwrap();
        assert_eq!(parsed.device.endpoint.port, 7125);
    }

    #[test]
    fn rejects_missing_serial() {
        let error = "=192.168.0.120".parse::<SnapmakerDeviceConfig>().unwrap_err();
        assert!(error.contains("serial number is empty"));
    }

    #[test]
    fn rejects_missing_host() {
        let error = "SERIAL=".parse::<SnapmakerDeviceConfig>().unwrap_err();
        assert!(error.contains("host is empty"));
    }

    #[test]
    fn rejects_missing_separator() {
        let error = "SERIAL".parse::<SnapmakerDeviceConfig>().unwrap_err();
        assert!(error.contains("SERIAL=HOST[:PORT]"));
    }
}
