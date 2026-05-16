use std::{fmt, str::FromStr};

use crate::bambu::MQTT_PORT;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

pub type MqttEndpoint = Endpoint;

impl Endpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    pub fn parse_with_default(
        value: &str,
        label: &str,
        default_port: u16,
    ) -> std::result::Result<Self, String> {
        let value = value.trim();
        parse_endpoint(value, value, label, default_port)
    }
}

impl FromStr for Endpoint {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Endpoint::parse_with_default(value, "MQTT endpoint", MQTT_PORT)
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(formatter, "[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "{}:{}", self.host, self.port)
        }
    }
}

pub(crate) fn parse_endpoint(
    endpoint: &str,
    value: &str,
    label: &str,
    default_port: u16,
) -> std::result::Result<Endpoint, String> {
    if endpoint.is_empty() {
        return Err(format!("invalid {label} `{value}`: host is empty"));
    }
    let (host, port) = split_host_port(endpoint, value, label)?;
    let host = host.trim();
    if host.is_empty() {
        return Err(format!("invalid {label} `{value}`: host is empty"));
    }
    let port = port
        .map(|port| parse_port(port, value, label))
        .transpose()?
        .unwrap_or(default_port);
    Ok(Endpoint::new(host, port))
}

fn split_host_port<'a>(
    endpoint: &'a str,
    value: &str,
    label: &str,
) -> std::result::Result<(&'a str, Option<&'a str>), String> {
    if let Some(rest) = endpoint.strip_prefix('[') {
        let Some((host, suffix)) = rest.split_once(']') else {
            return Err(format!("invalid {label} `{value}`"));
        };

        let port = match suffix.strip_prefix(':') {
            Some(port) => Some(port),
            None if suffix.is_empty() => None,
            _ => return Err(format!("invalid {label} `{value}`")),
        };
        return Ok((host, port));
    }

    if endpoint.matches(':').count() == 1 {
        let (host, port) = endpoint
            .split_once(':')
            .expect("single colon should split endpoint");
        return Ok((host, Some(port)));
    }

    Ok((endpoint, None))
}

fn parse_port(port: &str, value: &str, label: &str) -> std::result::Result<u16, String> {
    let port = port.trim();
    if port.is_empty() {
        return Err(format!("invalid {label} `{value}`: port is empty"));
    }
    port.parse::<u16>()
        .map_err(|_| format!("invalid {label} `{value}`: expected a valid port"))
}

#[cfg(test)]
mod tests {
    use super::{Endpoint, MqttEndpoint};

    #[test]
    fn mqtt_endpoint_parser_defaults_to_port_8883() {
        let endpoint: MqttEndpoint = "us.mqtt.bambulab.com".parse().unwrap();

        assert_eq!(endpoint.host, "us.mqtt.bambulab.com");
        assert_eq!(endpoint.port, 8883);
        assert_eq!(endpoint.to_string(), "us.mqtt.bambulab.com:8883");
    }

    #[test]
    fn mqtt_endpoint_parser_accepts_custom_port_and_ipv6() {
        let endpoint: MqttEndpoint = "mqtt.example.test:18883".parse().unwrap();

        assert_eq!(endpoint.host, "mqtt.example.test");
        assert_eq!(endpoint.port, 18883);

        let endpoint: MqttEndpoint = "[fe80::1]:18883".parse().unwrap();

        assert_eq!(endpoint.host, "fe80::1");
        assert_eq!(endpoint.port, 18883);
        assert_eq!(endpoint.to_string(), "[fe80::1]:18883");
    }

    #[test]
    fn endpoint_parser_accepts_a_custom_default_port() {
        let endpoint = Endpoint::parse_with_default("127.0.0.1", "bind address", 8765).unwrap();

        assert_eq!(endpoint.host, "127.0.0.1");
        assert_eq!(endpoint.port, 8765);
        assert_eq!(endpoint.to_string(), "127.0.0.1:8765");
    }
}
