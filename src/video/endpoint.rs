use std::{fmt, str::FromStr};

use crate::{bambu::local::parse_access_code_arg, endpoint::Endpoint, secret::Secret};

pub const DEFAULT_VIDEO_PORT: u16 = 6000;

#[derive(Clone, Debug, Eq)]
pub struct VideoEndpoint {
    endpoint: Endpoint,
    access_code: Option<Secret<String>>,
}

impl VideoEndpoint {
    pub fn new(endpoint: Endpoint, access_code: Option<Secret<String>>) -> Self {
        Self {
            endpoint,
            access_code,
        }
    }

    pub(super) fn address(&self) -> String {
        self.endpoint.to_string()
    }

    pub(super) fn host(&self) -> &str {
        self.endpoint.host.as_str()
    }

    pub(super) fn port(&self) -> u16 {
        self.endpoint.port
    }

    pub(crate) fn access_code(&self) -> Option<&str> {
        self.access_code.as_ref().map(|code| code.expose().as_str())
    }
}

impl PartialEq for VideoEndpoint {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint == other.endpoint
    }
}

impl fmt::Display for VideoEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.address())
    }
}

impl FromStr for VideoEndpoint {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        parse_video_endpoint(value)
    }
}

fn parse_video_endpoint(value: &str) -> std::result::Result<VideoEndpoint, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("video endpoint must not be empty".to_owned());
    }

    let fields = value.splitn(3, ',').collect::<Vec<_>>();
    if fields.len() > 2 {
        return Err(format!(
            "invalid video endpoint `{value}`: expected HOST[:PORT][,ACCESS_CODE]"
        ));
    }

    let endpoint = Endpoint::parse(fields[0].trim(), DEFAULT_VIDEO_PORT)
        .map_err(|error| format!("invalid video endpoint `{value}`: {error}"))?;
    let access_code = parse_access_code_arg(fields.get(1).copied(), "video endpoint", value)?;
    Ok(VideoEndpoint::new(endpoint, access_code))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::VideoEndpoint;

    fn endpoint(value: &str) -> VideoEndpoint {
        VideoEndpoint::from_str(value).expect("endpoint should parse")
    }

    #[test]
    fn video_endpoint_parser_defaults_to_port_6000() {
        let endpoint = endpoint("192.168.1.50");

        assert_eq!(endpoint.endpoint.host, "192.168.1.50");
        assert_eq!(endpoint.endpoint.port, 6000);
        assert_eq!(endpoint.to_string(), "192.168.1.50:6000");
    }

    #[test]
    fn video_endpoint_parser_accepts_custom_port() {
        let endpoint = endpoint("printer.local:6001");

        assert_eq!(endpoint.endpoint.host, "printer.local");
        assert_eq!(endpoint.endpoint.port, 6001);
        assert_eq!(endpoint.to_string(), "printer.local:6001");
    }

    #[test]
    fn video_endpoint_parser_accepts_access_code_without_displaying_it() {
        let endpoint = endpoint("printer.local:6001,12345678");

        assert_eq!(endpoint.endpoint.host, "printer.local");
        assert_eq!(endpoint.endpoint.port, 6001);
        assert_eq!(endpoint.access_code(), Some("12345678"));
        assert_eq!(endpoint.to_string(), "printer.local:6001");
    }

    #[test]
    fn video_endpoint_parser_rejects_name_metadata() {
        let error = VideoEndpoint::from_str("printer.local:6001,12345678,Office").unwrap_err();

        assert!(error.contains("HOST[:PORT][,ACCESS_CODE]"));
    }

    #[test]
    fn video_endpoint_parser_accepts_bracketed_ipv6_with_port() {
        let endpoint = endpoint("[fe80::1]:6002");

        assert_eq!(endpoint.endpoint.host, "fe80::1");
        assert_eq!(endpoint.endpoint.port, 6002);
        assert_eq!(endpoint.to_string(), "[fe80::1]:6002");
    }

    #[test]
    fn video_endpoint_parser_keeps_unbracketed_ipv6_on_default_port() {
        let endpoint = endpoint("fe80::1");

        assert_eq!(endpoint.endpoint.host, "fe80::1");
        assert_eq!(endpoint.endpoint.port, 6000);
        assert_eq!(endpoint.to_string(), "[fe80::1]:6000");
    }
}
