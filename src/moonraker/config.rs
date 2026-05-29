//! Parsing for `--snap-device HOST[:PORT]`.
//!
//! The CLI only carries the network address; the printer's serial number and
//! friendly name are discovered at startup by probing
//! `http://HOST:PORT/machine/system_info`.

use std::str::FromStr;

use crate::endpoint::Endpoint;

use super::MoonrakerEndpoint;

const DEFAULT_MOONRAKER_PORT: u16 = 80;

#[derive(Debug, Clone)]
pub(crate) struct MoonrakerDeviceConfig {
    pub(crate) endpoint: MoonrakerEndpoint,
}

impl FromStr for MoonrakerDeviceConfig {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err("invalid Moonraker endpoint: host is empty".to_owned());
        }
        let endpoint = Endpoint::parse(value, DEFAULT_MOONRAKER_PORT)
            .map_err(|error| format!("invalid Moonraker endpoint `{value}`: {error}"))?;
        Ok(Self { endpoint })
    }
}

#[cfg(test)]
mod tests {
    use super::MoonrakerDeviceConfig;

    #[test]
    fn parses_default_port() {
        let parsed: MoonrakerDeviceConfig = "192.168.0.120".parse().unwrap();
        assert_eq!(parsed.endpoint.host, "192.168.0.120");
        assert_eq!(parsed.endpoint.port, 80);
    }

    #[test]
    fn parses_custom_port() {
        let parsed: MoonrakerDeviceConfig = "printer.local:7125".parse().unwrap();
        assert_eq!(parsed.endpoint.host, "printer.local");
        assert_eq!(parsed.endpoint.port, 7125);
    }

    #[test]
    fn rejects_empty() {
        let error = "".parse::<MoonrakerDeviceConfig>().unwrap_err();
        assert!(error.contains("host is empty"));
    }
}
