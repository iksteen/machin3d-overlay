//! Generic `host:port` value type. Shared by every vendor; not
//! Bambu-specific despite its historical home under `local::`.

use std::{error::Error, fmt, str::FromStr};

use crate::bambu::MQTT_PORT;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

pub type MqttEndpoint = Endpoint;

/// Reasons [`Endpoint::parse`] can fail. Carries no user-facing label
/// or original input — callers wrap with their own context (e.g.
/// "invalid `--bbl-local-device` value `{value}`: {error}").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointParseError {
    EmptyHost,
    EmptyPort,
    InvalidPort,
    MalformedBracket,
}

impl fmt::Display for EndpointParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyHost => formatter.write_str("host is empty"),
            Self::EmptyPort => formatter.write_str("port is empty"),
            Self::InvalidPort => formatter.write_str("expected a valid port"),
            Self::MalformedBracket => formatter.write_str("malformed bracketed host"),
        }
    }
}

impl Error for EndpointParseError {}

impl Endpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// Parse `host`, `host:port`, or `[ipv6]:port`. When the input has
    /// no port, `default_port` is used. Errors are structural; callers
    /// wrap with the appropriate `--flag` / value context.
    pub fn parse(value: &str, default_port: u16) -> Result<Self, EndpointParseError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(EndpointParseError::EmptyHost);
        }
        let (host, port) = split_host_port(value)?;
        let host = host.trim();
        if host.is_empty() {
            return Err(EndpointParseError::EmptyHost);
        }
        let port = port.map(parse_port).transpose()?.unwrap_or(default_port);
        Ok(Endpoint::new(host, port))
    }
}

impl FromStr for Endpoint {
    type Err = EndpointParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Endpoint::parse(value, MQTT_PORT)
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

fn split_host_port(value: &str) -> Result<(&str, Option<&str>), EndpointParseError> {
    if let Some(rest) = value.strip_prefix('[') {
        let Some((host, suffix)) = rest.split_once(']') else {
            return Err(EndpointParseError::MalformedBracket);
        };

        let port = match suffix.strip_prefix(':') {
            Some(port) => Some(port),
            None if suffix.is_empty() => None,
            _ => return Err(EndpointParseError::MalformedBracket),
        };
        return Ok((host, port));
    }

    if value.matches(':').count() == 1 {
        let (host, port) = value
            .split_once(':')
            .expect("single colon should split endpoint");
        return Ok((host, Some(port)));
    }

    Ok((value, None))
}

fn parse_port(port: &str) -> Result<u16, EndpointParseError> {
    let port = port.trim();
    if port.is_empty() {
        return Err(EndpointParseError::EmptyPort);
    }
    port.parse::<u16>().map_err(|_| EndpointParseError::InvalidPort)
}

#[cfg(test)]
mod tests {
    use super::{Endpoint, EndpointParseError, MqttEndpoint};

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
        let endpoint = Endpoint::parse("127.0.0.1", 8765).unwrap();

        assert_eq!(endpoint.host, "127.0.0.1");
        assert_eq!(endpoint.port, 8765);
        assert_eq!(endpoint.to_string(), "127.0.0.1:8765");
    }

    #[test]
    fn endpoint_parser_reports_structural_errors() {
        assert_eq!(Endpoint::parse("", 8883), Err(EndpointParseError::EmptyHost));
        assert_eq!(
            Endpoint::parse("host:", 8883),
            Err(EndpointParseError::EmptyPort)
        );
        assert_eq!(
            Endpoint::parse("host:not-a-port", 8883),
            Err(EndpointParseError::InvalidPort)
        );
        assert_eq!(
            Endpoint::parse("[fe80::1", 8883),
            Err(EndpointParseError::MalformedBracket)
        );
    }
}
