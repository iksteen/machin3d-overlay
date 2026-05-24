//! Build a rumqttc [`Transport`] from per-printer mTLS material.
//!
//! Mirrors the role of `device_tls` for Bambu, but the trust anchor is
//! the printer-issued CA from a paired [`crate::snapmaker::SnapToken`]
//! (not a hard-coded vendor CA). Hostname verification is disabled
//! because the printer's TLS cert is keyed to its SN, not the LAN host
//! or IP we connect to.

use anyhow::{Context, Result};
use native_tls::{Certificate, Identity, TlsConnector as NativeTlsConnector};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use rumqttc::Transport;

use super::SnapMqttCreds;

pub(crate) fn transport_for(creds: &SnapMqttCreds) -> Result<Transport> {
    let ca = Certificate::from_pem(creds.ca.as_bytes())
        .context("could not parse Snapmaker mTLS CA certificate")?;
    let key_pkcs8 = normalize_key_to_pkcs8_pem(creds.key.expose())
        .context("could not normalize Snapmaker mTLS private key to PKCS#8")?;
    let identity = Identity::from_pkcs8(creds.cert.as_bytes(), key_pkcs8.as_bytes())
        .context("could not build Snapmaker mTLS client identity")?;
    let mut builder = NativeTlsConnector::builder();
    builder.disable_built_in_roots(true);
    builder.add_root_certificate(ca);
    builder.identity(identity);
    builder.use_sni(true);
    // The printer's server cert is keyed to its SN, not the LAN host we
    // dial. Native-TLS would otherwise fail the SNI/hostname check.
    builder.danger_accept_invalid_hostnames(true);
    let connector = builder
        .build()
        .context("could not build Snapmaker mTLS connector")?;
    Ok(Transport::tls_with_config(connector.into()))
}

/// native-tls's `Identity::from_pkcs8` strictly requires the PKCS#8 PEM
/// label (`-----BEGIN PRIVATE KEY-----`). Snapmaker ships PKCS#1 RSA keys
/// (`-----BEGIN RSA PRIVATE KEY-----`); the `rsa` crate handles the
/// conversion in two trait calls.
fn normalize_key_to_pkcs8_pem(pem: &str) -> Result<String> {
    let trimmed = pem.trim();
    if trimmed.contains("-----BEGIN PRIVATE KEY-----") {
        return Ok(trimmed.to_owned());
    }
    if trimmed.contains("-----BEGIN RSA PRIVATE KEY-----") {
        let key = rsa::RsaPrivateKey::from_pkcs1_pem(trimmed)
            .map_err(|error| anyhow::anyhow!("could not parse PKCS#1 RSA private key: {error}"))?;
        return key
            .to_pkcs8_pem(LineEnding::LF)
            .map(|s| s.to_string())
            .map_err(|error| anyhow::anyhow!("could not encode key as PKCS#8 PEM: {error}"));
    }
    anyhow::bail!(
        "unsupported private key format; expected PEM with `PRIVATE KEY` or `RSA PRIVATE KEY` label"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkcs8_pem_passes_through_unchanged() {
        let input = "-----BEGIN PRIVATE KEY-----\nABCD\n-----END PRIVATE KEY-----\n";
        let normalized = normalize_key_to_pkcs8_pem(input).unwrap();
        assert_eq!(normalized, input.trim());
    }

    #[test]
    fn rejects_unknown_pem_label() {
        let input = "-----BEGIN EC PRIVATE KEY-----\nABCD\n-----END EC PRIVATE KEY-----\n";
        let error = normalize_key_to_pkcs8_pem(input).unwrap_err();
        assert!(error.to_string().contains("unsupported private key format"));
    }

    #[test]
    fn pkcs1_rsa_pem_is_rewrapped_into_pkcs8_pem() {
        // 1024-bit RSA test key (`openssl genrsa -traditional 1024`).
        let pkcs1 = r#"-----BEGIN RSA PRIVATE KEY-----
MIICXQIBAAKBgQCzmFoHvoOyU0OBjRu57QDEN1J9Ln+PF+uKAD54VaiTEozLcoFn
k2s6+zNob+KrN/ecfQiyIQzLzI9dhEe62tsYq9wmmxsFkLxOTM+R7h9nhq10QOmn
uzyKnJ70aYoesXoj7bH14JjSWtXwVoAVZvd1FLkBGHzeP+5tK4w3d30ROwIDAQAB
AoGBAKZFf9y5mm4HvnD7tla9QL9oxJsW6IwPRkc+kJeSHn8DZoyY14uQJW+2z9J5
+64vI7Si4eEgzhsEqRqYdFxfcQVpjHP9Cb9Twm8ZQ7jiNwVZUNKIJOT0wsACAg+0
Uh4qfzSTAjPcUV4k0eOcuzimA/q0+9cpwumDiFmwu2u+E2Z5AkEA5GLjNuObc0KW
QvIAn99QmWRwQhBnf/+uJOkPJB27879SaldFDzBotXrjM6K0yr5UXhLgCn7sxLWk
6B/iVnz4PQJBAMlPQkNLj0WBCAgZMVYDmt01l08c58XpVJfvvfan52ciISQj99zo
j33JsZ1pghJcwpZlIsyCeLAeVdwBgSzeTtcCQQCFs/qu5JrZ5E6RjJme/p5x3qH1
myLshWOOyj4J97pT3VrDVKniVYXHUNT4IrXSx5AertAodNvp4SlUl23rEihFAkAM
tre9nkkHH7YNJOIrx4CBVgAfW/j7U9gm3FpH+KSxq8MiEC94QSvGyvUvttkjJb6Y
VvzSo67RmKjdgy7QUZ3zAkBlaJ7H4zT23ow2bQfzIi0eTPZXhwHLi1mia2d+jsJH
metxN2tLlxN1XW/RyvAjP0YdRUyCPPt/8HAoJQpS9KCf
-----END RSA PRIVATE KEY-----
"#;
        let pkcs8_pem = normalize_key_to_pkcs8_pem(pkcs1).expect("should wrap PKCS#1 into PKCS#8");
        assert!(pkcs8_pem.contains("-----BEGIN PRIVATE KEY-----"));
        assert!(pkcs8_pem.contains("-----END PRIVATE KEY-----"));
    }
}
