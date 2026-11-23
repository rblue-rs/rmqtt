//! Retained input normalization; no new allocations proportional to spare capacity.
use mqtt_api::Options;
pub(crate) fn normalize_options(o: &mut Options) {
    let mut n = o.clone();
    n.server = std::mem::take(&mut n.server).into_boxed_str().into_string();
    n.client_id = std::mem::take(&mut n.client_id)
        .into_boxed_str()
        .into_string();
    n.websocket_path = std::mem::take(&mut n.websocket_path)
        .into_boxed_str()
        .into_string();
    n.username = n.username.take().map(|s| s.into_boxed_str().into_string());
    n.password = n.password.take().map(|s| s.into_boxed_str().into_string());
    n.ca_pem = n.ca_pem.take().map(|v| v.into_boxed_slice().into_vec());
    n.client_cert_pem = n
        .client_cert_pem
        .take()
        .map(|v| v.into_boxed_slice().into_vec());
    n.client_key_pem = n
        .client_key_pem
        .take()
        .map(|v| v.into_boxed_slice().into_vec());
    if let Some(w) = n.will.as_mut() {
        w.topic = std::mem::take(&mut w.topic).into_boxed_str().into_string();
        w.payload = std::mem::take(&mut w.payload).into_boxed_slice().into_vec();
    }
    o.clear_secrets();
    *o = n;
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retained_options_do_not_inherit_unbounded_spare_capacity() {
        let mut o = Options::new("127.0.0.1", "bounded", 1883);
        let mut s = String::with_capacity(4 * 1024 * 1024);
        s.push_str("pass");
        o.password = Some(s);
        o.username = Some("u".into());
        o.allow_plaintext_credentials = true;
        let mut ca = Vec::with_capacity(4 * 1024 * 1024);
        ca.extend([1, 2, 3]);
        o.ca_pem = Some(ca);
        normalize_options(&mut o);
        assert_eq!(o.password.as_ref().unwrap().capacity(), 4);
        assert_eq!(o.ca_pem.as_ref().unwrap().capacity(), 3);
        assert_eq!(o.password.as_deref(), Some("pass"));
    }
}
