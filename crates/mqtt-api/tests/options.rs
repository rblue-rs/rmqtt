use mqtt_api::{Error, Options};
use std::time::Duration;

fn pem_fence(kind: &str) -> String {
    format!("-----BEGIN {kind}-----")
}

fn dummy_unparsed_key() -> Vec<u8> {
    format!(
        "{start}\nMIIB\n-----END {kind}-----\n",
        start = pem_fence("PRIVATE KEY"),
        kind = "PRIVATE KEY"
    )
    .into_bytes()
}

fn dummy_redaction_key() -> Vec<u8> {
    format!(
        "{start}\nSECRET-CLIENT-KEY\n-----END {kind}-----\n",
        start = pem_fence("PRIVATE KEY"),
        kind = "PRIVATE KEY"
    )
    .into_bytes()
}

fn dummy_unparsed_cert() -> Vec<u8> {
    format!(
        "{start}\nMIIB\n-----END {kind}-----\n",
        start = pem_fence("CERTIFICATE"),
        kind = "CERTIFICATE"
    )
    .into_bytes()
}

fn dummy_redaction_client_cert() -> Vec<u8> {
    format!(
        "{start}\nSECRET-CLIENT-CERT\n-----END {kind}-----\n",
        start = pem_fence("CERTIFICATE"),
        kind = "CERTIFICATE"
    )
    .into_bytes()
}

fn dummy_redaction_cert() -> Vec<u8> {
    format!(
        "{start}\nSECRET-CA-MATERIAL\n-----END {kind}-----\n",
        start = pem_fence("CERTIFICATE"),
        kind = "CERTIFICATE"
    )
    .into_bytes()
}

#[test]
fn default_keepalive_zero_and_supported_v311() {
    let o = Options::new("localhost", "test", 1883);
    assert_eq!(o.keep_alive, Duration::ZERO);
    assert_eq!(o.validate(), Ok(()));
}
#[test]
fn v5_zero_is_typed_unsupported_not_panic() {
    let mut o = Options::new("localhost", "test", 1883);
    o.protocol = 5;
    assert_eq!(o.validate(), Err(Error::Unsupported));
}
#[test]
fn reject_invalid_inputs_before_engine() {
    for (server, id, port) in [("", "x", 1883), ("h", "", 1883), ("h", "x", 0)] {
        assert_eq!(
            Options::new(server, id, port).validate(),
            Err(Error::InvalidArgument)
        );
    }
    let mut o = Options::new("h", "x", 1883);
    o.keep_alive = Duration::from_millis(500);
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.keep_alive = Duration::from_secs(65536);
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
}

#[test]
fn tls_error_codes_are_new_and_stable() {
    assert_eq!(Error::Tls as i32, 14);
    assert_eq!(Error::CertificateUntrusted as i32, 15);
    assert_eq!(Error::HostnameMismatch as i32, 16);
    assert_eq!(Error::CertificateExpired as i32, 17);
    for code in [
        Error::Tls,
        Error::CertificateUntrusted,
        Error::HostnameMismatch,
        Error::CertificateExpired,
    ] {
        let text = format!("{code} {code:?}");
        assert!(!text.contains("BEGIN "));
        assert!(!text.contains("PRIVATE KEY"));
        assert!(!text.contains("-----"));
    }
}

#[test]
fn default_options_are_plaintext_without_custom_ca() {
    let o = Options::new("localhost", "test", 1883);
    assert!(!o.use_tls);
    assert_eq!(o.ca_pem, None);
    assert_eq!(o.validate(), Ok(()));
}

#[test]
fn tls_without_custom_ca_is_valid_and_selects_system_roots() {
    let mut o = Options::new("localhost", "test", 8883);
    o.use_tls = true;
    assert_eq!(o.ca_pem, None);
    assert_eq!(o.validate(), Ok(()));
}

#[test]
fn tls_with_custom_ca_is_valid() {
    let mut o = Options::new("127.0.0.1", "test", 8883);
    o.use_tls = true;
    o.ca_pem = Some(dummy_unparsed_cert());
    assert_eq!(o.validate(), Ok(()));
}

#[test]
fn tls_empty_or_oversized_or_nul_ca_is_rejected_without_falling_back() {
    let mut o = Options::new("127.0.0.1", "test", 8883);
    o.use_tls = true;
    o.ca_pem = Some(vec![]);
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.ca_pem = Some(vec![0x41, 0, 0x42]);
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.ca_pem = Some(vec![b'A'; mqtt_api::MAX_CA_PEM + 1]);
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
}

#[test]
fn custom_ca_without_tls_is_rejected_not_ignored() {
    let mut o = Options::new("localhost", "test", 1883);
    o.use_tls = false;
    o.ca_pem = Some(dummy_unparsed_cert());
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
}

#[test]
fn options_debug_redacts_ca_pem() {
    let mut o = Options::new("127.0.0.1", "secret-client", 8883);
    o.use_tls = true;
    let pem = dummy_redaction_cert();
    o.ca_pem = Some(pem.to_vec());
    let text = format!("{o:?}");
    assert!(text.contains("use_tls: true"));
    assert!(text.contains("<redacted>") || text.contains("Some"));
    assert!(!text.contains("SECRET-CA-MATERIAL"));
    assert!(!text.contains("BEGIN CERTIFICATE"));
    assert!(!text.contains(String::from_utf8_lossy(&pem).as_ref()));
}

#[test]
fn auth_and_will_error_codes_are_new_and_stable() {
    assert_eq!(Error::PlaintextCredentials as i32, 18);
    assert_eq!(Error::BadCredentials as i32, 19);
    for code in [Error::PlaintextCredentials, Error::BadCredentials] {
        let text = format!("{code} {code:?}");
        assert!(!text.contains("password"));
        assert!(!text.contains("PASSWORD"));
        assert!(!text.contains("secret"));
    }
}

#[test]
fn default_options_have_no_credentials_or_will() {
    let o = Options::new("localhost", "test", 1883);
    assert_eq!(o.username, None);
    assert!(o.password.is_none());
    assert!(!o.allow_plaintext_credentials);
    assert!(o.will.is_none());
    assert_eq!(o.validate(), Ok(()));
}

#[test]
fn username_without_password_is_valid_on_plaintext() {
    let mut o = Options::new("localhost", "test", 1883);
    o.username = Some("user".into());
    assert_eq!(o.validate(), Ok(()));
}

#[test]
fn password_without_username_is_invalid() {
    let mut o = Options::new("localhost", "test", 1883);
    o.password = Some("unit-fixture-secret".into());
    o.allow_plaintext_credentials = true;
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
}

#[test]
fn plaintext_password_is_rejected_unless_explicitly_allowed() {
    let mut o = Options::new("localhost", "test", 1883);
    o.username = Some("user".into());
    o.password = Some("unit-fixture-secret".into());
    assert_eq!(o.validate(), Err(Error::PlaintextCredentials));
    o.allow_plaintext_credentials = true;
    assert_eq!(o.validate(), Ok(()));
}

#[test]
fn tls_password_is_allowed_without_plaintext_switch() {
    let mut o = Options::new("localhost", "test", 8883);
    o.use_tls = true;
    o.username = Some("user".into());
    o.password = Some("unit-fixture-secret".into());
    assert!(!o.allow_plaintext_credentials);
    assert_eq!(o.validate(), Ok(()));
}

#[test]
fn empty_username_or_nul_credentials_are_rejected() {
    let mut o = Options::new("localhost", "test", 1883);
    o.username = Some(String::new());
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.username = Some("ok".into());
    o.password = Some("x".into());
    o.allow_plaintext_credentials = true;
    o.username = Some("bad\0user".into());
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.username = Some("ok".into());
    o.password = Some("bad\0pass".into());
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
}

#[test]
fn will_qos0_and_qos1_are_valid_qos2_is_unsupported() {
    let mut o = Options::new("localhost", "test", 1883);
    o.will = Some(mqtt_api::Will {
        topic: "will/topic".into(),
        payload: b"bye".to_vec(),
        qos: 0,
        retain: false,
    });
    assert_eq!(o.validate(), Ok(()));
    o.will.as_mut().unwrap().qos = 1;
    o.will.as_mut().unwrap().retain = true;
    assert_eq!(o.validate(), Ok(()));
    o.will.as_mut().unwrap().qos = 2;
    assert_eq!(o.validate(), Err(Error::Unsupported));
}

#[test]
fn empty_or_wildcard_will_topic_is_rejected() {
    let mut o = Options::new("localhost", "test", 1883);
    o.will = Some(mqtt_api::Will {
        topic: String::new(),
        payload: vec![],
        qos: 0,
        retain: false,
    });
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.will.as_mut().unwrap().topic = "a/#".into();
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.will.as_mut().unwrap().topic = "a/+".into();
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
}

#[test]
fn options_debug_redacts_password() {
    let mut o = Options::new("127.0.0.1", "secret-client", 1883);
    o.username = Some("visible-user".into());
    let secret = "unit-fixture-secret";
    o.password = Some(secret.into());
    o.allow_plaintext_credentials = true;
    o.will = Some(mqtt_api::Will {
        topic: "w".into(),
        payload: secret.as_bytes().to_vec(),
        qos: 0,
        retain: false,
    });
    let text = format!("{o:?}");
    assert!(text.contains("visible-user"));
    assert!(text.contains("<redacted>"));
    assert!(!text.contains(secret));
    assert!(!text.contains(&format!("Some(\"{secret}\")")));
}

#[test]
fn mtls_error_codes_are_new_and_stable() {
    assert_eq!(Error::InvalidClientCertificate as i32, 20);
    assert_eq!(Error::CertificateKeyMismatch as i32, 21);
    for code in [
        Error::InvalidClientCertificate,
        Error::CertificateKeyMismatch,
    ] {
        let text = format!("{code} {code:?}");
        assert!(!text.contains("BEGIN "));
        assert!(!text.contains("PRIVATE KEY"));
        assert!(!text.contains("-----"));
        assert!(!text.contains("CERTIFICATE"));
    }
}

#[test]
fn default_options_have_no_client_identity() {
    let o = Options::new("localhost", "test", 1883);
    assert!(o.client_cert_pem.is_none());
    assert!(o.client_key_pem.is_none());
    assert_eq!(o.validate(), Ok(()));
}

#[test]
fn tls_without_client_identity_is_valid() {
    let mut o = Options::new("localhost", "test", 8883);
    o.use_tls = true;
    assert_eq!(o.validate(), Ok(()));
}

#[test]
fn client_identity_without_tls_is_rejected_not_ignored() {
    let mut o = Options::new("localhost", "test", 1883);
    o.client_cert_pem = Some(dummy_unparsed_cert());
    o.client_key_pem = Some(dummy_unparsed_key());
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
}

#[test]
fn incomplete_client_identity_is_a_typed_mismatch() {
    let mut o = Options::new("localhost", "test", 8883);
    o.use_tls = true;
    o.client_cert_pem = Some(dummy_unparsed_cert());
    assert_eq!(o.validate(), Err(Error::CertificateKeyMismatch));
    o.client_cert_pem = None;
    o.client_key_pem = Some(dummy_unparsed_key());
    assert_eq!(o.validate(), Err(Error::CertificateKeyMismatch));
}

#[test]
fn empty_or_oversized_or_nul_client_identity_is_rejected() {
    let mut o = Options::new("127.0.0.1", "test", 8883);
    o.use_tls = true;
    let cert = dummy_unparsed_cert();
    let key = dummy_unparsed_key();
    o.client_cert_pem = Some(vec![]);
    o.client_key_pem = Some(key.clone());
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.client_cert_pem = Some(cert.clone());
    o.client_key_pem = Some(vec![]);
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.client_cert_pem = Some(vec![0x41, 0, 0x42]);
    o.client_key_pem = Some(key.clone());
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.client_cert_pem = Some(cert);
    o.client_key_pem = Some(vec![b'A'; mqtt_api::MAX_CLIENT_KEY_PEM + 1]);
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
}

#[test]
fn paired_client_identity_on_tls_is_valid_before_pem_parse() {
    let mut o = Options::new("127.0.0.1", "test", 8883);
    o.use_tls = true;
    o.client_cert_pem = Some(dummy_unparsed_cert());
    o.client_key_pem = Some(dummy_unparsed_key());
    assert_eq!(o.validate(), Ok(()));
}

#[test]
fn options_debug_redacts_client_identity() {
    let mut o = Options::new("127.0.0.1", "secret-client", 8883);
    o.use_tls = true;
    let cert = dummy_redaction_client_cert();
    let key = dummy_redaction_key();
    o.client_cert_pem = Some(cert.to_vec());
    o.client_key_pem = Some(key.to_vec());
    let text = format!("{o:?}");
    assert!(text.contains("use_tls: true"));
    assert!(text.contains("<redacted>"));
    assert!(!text.contains("SECRET-CLIENT-CERT"));
    assert!(!text.contains("SECRET-CLIENT-KEY"));
    assert!(!text.contains("BEGIN CERTIFICATE"));
    assert!(!text.contains("BEGIN PRIVATE KEY"));
    assert!(!text.contains(String::from_utf8_lossy(&cert).as_ref()));
    assert!(!text.contains(String::from_utf8_lossy(&key).as_ref()));
}

#[test]
fn clear_secrets_drops_client_key_material() {
    let mut o = Options::new("127.0.0.1", "secret-client", 8883);
    o.use_tls = true;
    o.client_cert_pem = Some(dummy_redaction_client_cert());
    o.client_key_pem = Some(dummy_redaction_key());
    o.password = Some("unit-identity-secret".into());
    o.clear_secrets();
    assert!(o.client_cert_pem.is_none());
    assert!(o.client_key_pem.is_none());
    assert!(o.password.is_none());
    let text = format!("{o:?}");
    assert!(!text.contains("SECRET-CLIENT-KEY"));
    assert!(!text.contains("unit-identity-secret"));
}

#[test]
fn default_clean_session_is_true_and_reconnect_is_off() {
    let o = Options::new("localhost", "test", 1883);
    assert!(o.clean_session);
    assert!(!o.reconnect.enabled);
    assert_eq!(o.reconnect.max_attempts, 8);
    assert_eq!(o.reconnect.base_delay, Duration::from_secs(1));
    assert_eq!(o.reconnect.max_delay, Duration::from_secs(30));
    assert_eq!(o.validate(), Ok(()));
    let text = format!("{o:?}");
    assert!(text.contains("clean_session"));
    assert!(text.contains("reconnect"));
    assert!(!text.contains("SECRET"));
}

#[test]
fn enabled_reconnect_policy_requires_a_finite_ceiling() {
    let mut o = Options::new("localhost", "test", 1883);
    o.reconnect.enabled = true;
    o.reconnect.max_attempts = 0;
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.reconnect.max_attempts = 3;
    o.reconnect.base_delay = Duration::from_micros(500);
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.reconnect.base_delay = Duration::from_millis(20);
    o.reconnect.max_delay = Duration::from_millis(10);
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.reconnect.max_delay = Duration::from_millis(20);
    assert_eq!(o.validate(), Ok(()));
}

#[test]
fn disabled_reconnect_policy_does_not_require_delay_fields() {
    let mut o = Options::new("localhost", "test", 1883);
    o.reconnect.enabled = false;
    o.reconnect.max_attempts = 0;
    o.reconnect.base_delay = Duration::ZERO;
    o.reconnect.max_delay = Duration::ZERO;
    assert_eq!(o.validate(), Ok(()));
}

#[test]
fn unbounded_raw_host_metadata_is_rejected() {
    let o = mqtt_api::Options::new("h".repeat(65536), "bounded", 1883);
    assert_eq!(o.validate(), Err(mqtt_api::Error::InvalidArgument));
}

#[test]
fn will_binary_u16_boundary_rejected_before_engine() {
    for protocol in [4, 5] {
        let mut o = Options::new("localhost", "will-bound", 1883);
        o.protocol = protocol;
        if protocol == 5 {
            o.keep_alive = Duration::from_secs(5);
        }
        o.will = Some(mqtt_api::Will {
            topic: "dummy".into(),
            payload: vec![0; 65535],
            qos: 0,
            retain: false,
        });
        assert_eq!(o.validate(), Ok(()));
        o.will.as_mut().unwrap().payload.push(0);
        assert_eq!(o.validate(), Err(Error::InvalidArgument));
    }
}
