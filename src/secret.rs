//! Newtype wrapper that hides a value from Debug and Display.
//!
//! `Secret<T>` is used for access codes and access tokens. The intent is that
//! a slip-up like `tracing::warn!(?device, "...")` cannot leak credentials,
//! because the `Debug` impl on `Secret<T>` always writes `Secret(<redacted>)`
//! regardless of the inner type. Use `expose()` at the boundary where the
//! plaintext value is actually needed (TLS auth, MQTT credentials, FTPS login,
//! token file serialization).

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, PartialEq, Eq)]
pub struct Secret<T>(T);

impl<T> Secret<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }

    pub fn expose(&self) -> &T {
        &self.0
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Secret(<redacted>)")
    }
}

impl<T> fmt::Display for Secret<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl<T> From<T> for Secret<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<'de, T> Deserialize<'de> for Secret<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self)
    }
}

impl<T> Serialize for Secret<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::Secret;

    #[test]
    fn debug_does_not_print_inner_value() {
        let secret = Secret::new("supersecret-access-code".to_owned());

        let debug = format!("{:?}", secret);
        let display = format!("{}", secret);

        assert!(!debug.contains("supersecret"));
        assert!(!display.contains("supersecret"));
        assert!(debug.contains("redacted"));
        assert!(display.contains("redacted"));
    }

    #[test]
    fn debug_hides_inner_value_even_in_containers() {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct Holder {
            id: &'static str,
            code: Secret<String>,
        }

        let holder = Holder {
            id: "printer-a",
            code: Secret::new("12345678".to_owned()),
        };

        let debug = format!("{:?}", holder);

        assert!(debug.contains("printer-a"));
        assert!(!debug.contains("12345678"));
        assert!(debug.contains("redacted"));
    }

    #[test]
    fn serde_roundtrips_transparently() {
        let secret: Secret<String> = serde_json::from_str("\"supersecret\"").unwrap();
        assert_eq!(secret.expose(), "supersecret");

        let json = serde_json::to_string(&secret).unwrap();
        assert_eq!(json, "\"supersecret\"");
    }
}
