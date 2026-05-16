use std::fmt;

use super::Endpoint;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalEndpoint {
    pub endpoint: Endpoint,
    pub access_code: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalDevice {
    pub id: String,
    pub endpoint: LocalEndpoint,
}

impl LocalEndpoint {
    #[cfg(test)]
    pub fn new(host: impl Into<String>, port: u16, access_code: impl Into<String>) -> Self {
        Self {
            endpoint: Endpoint::new(host, port),
            access_code: access_code.into(),
            name: None,
        }
    }

    pub fn host(&self) -> &str {
        self.endpoint.host.as_str()
    }

    pub fn port(&self) -> u16 {
        self.endpoint.port
    }

    pub fn access_code(&self) -> &str {
        self.access_code.as_str()
    }
}

impl fmt::Display for LocalDevice {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}={}", self.id, self.endpoint)
    }
}

impl fmt::Display for LocalEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.endpoint.fmt(formatter)
    }
}
