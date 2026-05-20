//! Parsing for `--snap-device HOST[:PORT]`.
//!
//! The CLI only carries the network address; the printer's serial number and
//! friendly name are discovered at startup by probing
//! `http://HOST:PORT/machine/system_info`.

use std::str::FromStr;

use crate::local::endpoint::parse_endpoint;

use super::SnapmakerEndpoint;

const DEFAULT_MOONRAKER_PORT: u16 = 80;

#[derive(Debug, Clone)]
pub(crate) struct SnapmakerDeviceConfig {
    pub(crate) endpoint: SnapmakerEndpoint,
}

impl FromStr for SnapmakerDeviceConfig {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err("invalid --snap-device: host is empty".to_owned());
        }
        let endpoint = parse_endpoint(
            value,
            value,
            "Snapmaker endpoint",
            DEFAULT_MOONRAKER_PORT,
        )?;
        Ok(Self { endpoint })
    }
}

#[cfg(test)]
mod tests {
    use super::SnapmakerDeviceConfig;

    #[test]
    fn parses_default_port() {
        let parsed: SnapmakerDeviceConfig = "192.168.0.120".parse().unwrap();
        assert_eq!(parsed.endpoint.host, "192.168.0.120");
        assert_eq!(parsed.endpoint.port, 80);
    }

    #[test]
    fn parses_custom_port() {
        let parsed: SnapmakerDeviceConfig = "printer.local:7125".parse().unwrap();
        assert_eq!(parsed.endpoint.host, "printer.local");
        assert_eq!(parsed.endpoint.port, 7125);
    }

    #[test]
    fn rejects_empty() {
        let error = "".parse::<SnapmakerDeviceConfig>().unwrap_err();
        assert!(error.contains("host is empty"));
    }
}
