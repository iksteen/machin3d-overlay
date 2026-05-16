use std::{fmt, str::FromStr};

use crate::bambu::MQTT_PORT;

use super::{endpoint::parse_endpoint, Endpoint, LocalEndpoint};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalEndpointConfig {
    pub endpoint: Endpoint,
    pub access_code: Option<String>,
    pub name: Option<String>,
}

impl LocalEndpointConfig {
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }

    pub fn into_endpoint(self, access_code: String) -> LocalEndpoint {
        LocalEndpoint {
            endpoint: self.endpoint,
            access_code,
            name: self.name,
        }
    }
}

impl fmt::Display for LocalEndpointConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.endpoint.fmt(formatter)
    }
}

impl FromStr for LocalEndpointConfig {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        parse_local_device_arg(value)
    }
}

fn parse_local_device_arg(value: &str) -> std::result::Result<LocalEndpointConfig, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("local device must not be empty".to_owned());
    }
    if value.contains('=') {
        return Err(format!(
            "invalid local device `{value}`: DEVICE_ID= prefix is not supported; use HOST[:PORT][,ACCESS_CODE[,NAME]]"
        ));
    }

    parse_local_endpoint_arg(value, "local device", MQTT_PORT)
}

fn parse_local_endpoint_arg(
    value: &str,
    label: &str,
    default_port: u16,
) -> std::result::Result<LocalEndpointConfig, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("{label} must not be empty"));
    }

    let fields = value.splitn(3, ',').collect::<Vec<_>>();
    let parsed = parse_endpoint(fields[0].trim(), value, label, default_port)?;
    let access_code = parse_access_code_arg(fields.get(1).copied(), label, value)?;
    let name = optional_field(&fields, 2);

    Ok(LocalEndpointConfig {
        endpoint: parsed,
        access_code,
        name,
    })
}

pub(crate) fn parse_access_code_arg(
    access_code: Option<&str>,
    label: &str,
    value: &str,
) -> std::result::Result<Option<String>, String> {
    let access_code = access_code
        .map(str::trim)
        .filter(|access_code| !access_code.is_empty())
        .map(str::to_owned);
    if access_code
        .as_deref()
        .is_some_and(|access_code| !access_code.is_ascii())
    {
        return Err(format!(
            "invalid {label} `{value}`: access code must be ASCII"
        ));
    }
    Ok(access_code)
}

fn optional_field(fields: &[&str], index: usize) -> Option<String> {
    fields
        .get(index)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::LocalEndpointConfig;

    fn local_device_arg(value: &str) -> LocalEndpointConfig {
        value.parse().expect("local device arg should parse")
    }

    #[test]
    fn local_device_parser_accepts_default_mqtt_port() {
        let device = local_device_arg("192.168.1.50,12345678,Office X1");

        assert_eq!(device.endpoint.host, "192.168.1.50");
        assert_eq!(device.endpoint.port, 8883);
        assert_eq!(device.access_code.as_deref(), Some("12345678"));
        assert_eq!(device.name.as_deref(), Some("Office X1"));
    }

    #[test]
    fn local_device_parser_accepts_custom_port() {
        let device = local_device_arg("printer.local:18883,12345678");

        assert_eq!(device.endpoint.host, "printer.local");
        assert_eq!(device.endpoint.port, 18883);
        assert_eq!(device.name, None);
    }

    #[test]
    fn local_device_parser_accepts_missing_access_code() {
        let device = local_device_arg("printer.local");

        assert_eq!(device.endpoint.host, "printer.local");
        assert_eq!(device.access_code, None);
        assert_eq!(device.name, None);

        let device = local_device_arg("printer.local,,Office X1");

        assert_eq!(device.access_code, None);
        assert_eq!(device.name.as_deref(), Some("Office X1"));
    }

    #[test]
    fn local_device_parser_accepts_bracketed_ipv6() {
        let device = local_device_arg("[fe80::1]:18883,12345678");

        assert_eq!(device.endpoint.host, "fe80::1");
        assert_eq!(device.endpoint.port, 18883);
    }

    #[test]
    fn local_device_parser_rejects_device_id_prefix() {
        let error = "printer-a=printer.local:18883,12345678"
            .parse::<LocalEndpointConfig>()
            .unwrap_err();

        assert!(error.contains("DEVICE_ID= prefix is not supported"));
    }

    #[test]
    fn local_device_config_accepts_host_only_form() {
        let device = local_device_arg("printer.local:18883,12345678,Office X1");

        assert_eq!(device.endpoint.host, "printer.local");
        assert_eq!(device.endpoint.port, 18883);
        assert_eq!(device.access_code.as_deref(), Some("12345678"));
        assert_eq!(device.name.as_deref(), Some("Office X1"));
    }
}
