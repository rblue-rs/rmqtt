//! Experimental polling C ABI. Caller-owned buffers; no cross-thread callbacks.
mod ownership;

use mqtt_api::{Error, Options, ReconnectPolicy};
use mqtt_core::Client;
use std::{
    collections::HashMap,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{LazyLock, Mutex},
    time::Duration,
};

/// Event metadata. Buffers are copied into caller memory by mqtt_client_poll.
#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct MqttEvent {
    pub kind: u32,
    pub code: i32,
    pub operation_id: u64,
    pub packet_id: u16,
    pub qos: u8,
    pub session_present: u8,
    pub topic_len: usize,
    pub payload_len: usize,
}
/// Additive reconnect policy for `mqtt_client_connect_ex`. `enabled` must be 0 or 1.
/// A null policy pointer means disabled (the default).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MqttReconnectPolicy {
    pub enabled: u8,
    pub max_attempts: u32,
    pub base_delay_ms: u32,
    pub max_delay_ms: u32,
}
struct Entry {
    client: Client,
    options: Options,
    closed: bool,
}
static CLIENTS: LazyLock<Mutex<(u64, HashMap<u64, Entry>)>> =
    LazyLock::new(|| Mutex::new((0, HashMap::new())));
fn boundary(f: impl FnOnce() -> Result<(), Error>) -> i32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => 0,
        Ok(Err(e)) => e as i32,
        Err(_) => Error::Internal as i32,
    }
}
fn entry(handle: u64, f: impl FnOnce(&mut Entry) -> Result<(), Error>) -> Result<(), Error> {
    let mut store = CLIENTS.lock().map_err(|_| Error::Internal)?;
    let e = store.1.get_mut(&handle).ok_or(Error::Closed)?;
    if e.closed {
        return Err(Error::Closed);
    }
    f(e)
}
unsafe fn bytes<'a>(p: *const u8, len: usize, max: usize) -> Result<&'a [u8], Error> {
    if len > max || len > isize::MAX as usize || (p.is_null() && len != 0) {
        return Err(Error::InvalidArgument);
    }
    if len == 0 {
        Ok(&[])
    } else {
        Ok(unsafe { std::slice::from_raw_parts(p, len) })
    }
}
// Borrow/validate before the client registry lock, allocate only inside entry.
// This prevents unbounded concurrent FFI copies waiting for the same owner lock.
unsafe fn text_ref<'a>(p: *const u8, len: usize) -> Result<&'a str, Error> {
    std::str::from_utf8(unsafe { bytes(p, len, 65535)? }).map_err(|_| Error::InvalidArgument)
}
unsafe fn text(p: *const u8, len: usize) -> Result<String, Error> {
    unsafe { text_ref(p, len) }.map(str::to_owned)
}
#[no_mangle]
pub extern "C" fn mqtt_runtime_init() -> i32 {
    boundary(mqtt_runtime::init)
}
/// Pair a plain-C init reference; do not consume Dart owner-managed references.
/// Allows up to two seconds for native-only owner-exit cleanup notification races,
/// then applies the live-client/worker guard. Not a message drain or Dart handshake.
#[no_mangle]
pub extern "C" fn mqtt_runtime_shutdown() -> i32 {
    // A VM exit message can race its final reply to another isolate. Waiting
    // here is native-only, bounded, and never waits for a Dart callback.
    ownership::wait_for_exit_cleanup();
    boundary(mqtt_runtime::shutdown)
}
/// # Safety
/// Input pointers must reference readable lengths; out must be aligned/writable.
#[no_mangle]
pub unsafe extern "C" fn mqtt_client_new(
    host: *const u8,
    host_len: usize,
    id: *const u8,
    id_len: usize,
    port: u16,
    out: *mut u64,
) -> i32 {
    boundary(|| {
        if out.is_null() {
            return Err(Error::InvalidArgument);
        }
        let options = Options::new(
            unsafe { text(host, host_len)? },
            unsafe { text(id, id_len)? },
            port,
        );
        options.validate()?;
        let client = Client::new()?;
        let mut s = CLIENTS.lock().map_err(|_| Error::Internal)?;
        if s.1.len() >= 64 {
            return Err(Error::QueueFull);
        }
        s.0 = s.0.checked_add(1).ok_or(Error::Internal)?;
        let handle = s.0;
        s.1.insert(
            handle,
            Entry {
                client,
                options,
                closed: false,
            },
        );
        unsafe {
            out.write(handle);
        }
        Ok(())
    })
}
/// Configure WebSocket for subsequent connect calls on this handle only.
/// Does not mutate an existing connection. use_tls on connect selects WSS.
/// enabled=0 resets to TCP/TLS and requires null+0 path; enabled=1 with
/// null+0 selects /mqtt. Otherwise path is copied and validated (max2048 bytes).
/// # Safety
/// A non-null path must reference path_len readable bytes.
#[no_mangle]
pub unsafe extern "C" fn mqtt_client_set_websocket(
    handle: u64,
    enabled: u8,
    path: *const u8,
    path_len: usize,
) -> i32 {
    boundary(|| {
        if enabled > 1 || path_len > 2048 || (enabled == 0 && (!path.is_null() || path_len != 0)) {
            return Err(Error::InvalidArgument);
        }
        let value = if path.is_null() && path_len == 0 {
            "/mqtt"
        } else {
            unsafe { text_ref(path, path_len)? }
        };
        entry(handle, |e| {
            let mut opts = e.options.clone();
            opts.use_websocket = enabled == 1;
            opts.websocket_path = value.to_owned();
            if opts.use_websocket {
                opts.websocket_uri()?;
            }
            e.options = opts;
            Ok(())
        })
    })
}

unsafe fn optional_text<'a>(p: *const u8, len: usize) -> Result<Option<&'a str>, Error> {
    if p.is_null() && len == 0 {
        Ok(None)
    } else {
        Ok(Some(unsafe { text_ref(p, len)? }))
    }
}

struct ConnectParams {
    handle: u64,
    op: u64,
    protocol: u8,
    keep_alive_seconds: u16,
    use_tls: u8,
    ca_pem: *const u8,
    ca_pem_len: usize,
    username: *const u8,
    username_len: usize,
    password: *const u8,
    password_len: usize,
    allow_plaintext_credentials: u8,
    will_topic: *const u8,
    will_topic_len: usize,
    will_payload: *const u8,
    will_payload_len: usize,
    will_qos: u8,
    will_retain: u8,
    client_cert_pem: *const u8,
    client_cert_pem_len: usize,
    client_key_pem: *const u8,
    client_key_pem_len: usize,
    clean_session: u8,
    reconnect: ReconnectPolicy,
}
unsafe fn apply_connect(p: ConnectParams) -> Result<(), Error> {
    let ConnectParams {
        handle,
        op,
        protocol,
        keep_alive_seconds,
        use_tls,
        ca_pem,
        ca_pem_len,
        username,
        username_len,
        password,
        password_len,
        allow_plaintext_credentials,
        will_topic,
        will_topic_len,
        will_payload,
        will_payload_len,
        will_qos,
        will_retain,
        client_cert_pem,
        client_cert_pem_len,
        client_key_pem,
        client_key_pem_len,
        clean_session,
        reconnect,
    } = p;
    if use_tls > 1 || allow_plaintext_credentials > 1 || will_retain > 1 || clean_session > 1 {
        return Err(Error::InvalidArgument);
    }
    let ca = if ca_pem.is_null() && ca_pem_len == 0 {
        None
    } else {
        Some(unsafe { bytes(ca_pem, ca_pem_len, mqtt_api::MAX_CA_PEM)? })
    };
    let client_cert = if client_cert_pem.is_null() && client_cert_pem_len == 0 {
        None
    } else {
        Some(unsafe {
            bytes(
                client_cert_pem,
                client_cert_pem_len,
                mqtt_api::MAX_CLIENT_CERT_PEM,
            )?
        })
    };
    let client_key = if client_key_pem.is_null() && client_key_pem_len == 0 {
        None
    } else {
        Some(unsafe {
            bytes(
                client_key_pem,
                client_key_pem_len,
                mqtt_api::MAX_CLIENT_KEY_PEM,
            )?
        })
    };
    let username = unsafe { optional_text(username, username_len)? };
    let password = unsafe { optional_text(password, password_len)? };
    let will = if will_topic.is_null() && will_topic_len == 0 {
        if will_payload_len != 0 {
            return Err(Error::InvalidArgument);
        }
        None
    } else {
        Some((unsafe { text_ref(will_topic, will_topic_len)? }, unsafe {
            bytes(will_payload, will_payload_len, mqtt_api::MAX_WILL_PAYLOAD)?
        }))
    };
    entry(handle, |e| {
        let mut opts = e.options.clone();
        opts.protocol = protocol;
        opts.keep_alive = Duration::from_secs(keep_alive_seconds as u64);
        opts.use_tls = use_tls == 1;
        opts.ca_pem = ca.map(<[u8]>::to_vec);
        opts.client_cert_pem = client_cert.map(<[u8]>::to_vec);
        opts.client_key_pem = client_key.map(<[u8]>::to_vec);
        opts.username = username.map(str::to_owned);
        opts.password = password.map(str::to_owned);
        opts.allow_plaintext_credentials = allow_plaintext_credentials == 1;
        opts.will = will.map(|(t, p)| mqtt_api::Will {
            topic: t.to_owned(),
            payload: p.to_vec(),
            qos: will_qos,
            retain: will_retain == 1,
        });
        opts.clean_session = clean_session == 1;
        opts.reconnect = reconnect.clone();
        e.client.connect(op, opts)
    })
}

/// # Safety
/// `ca_pem` must reference `ca_pem_len` readable bytes when non-null.
/// A null pointer is allowed only when `ca_pem_len` is zero, which means no
/// custom CA: if `use_tls` is 1, the host system roots are used. `use_tls`
/// must be 0 or 1. Providing a CA without TLS is rejected.
/// Username and password pointers are copied immediately; null + zero length
/// means the field is absent. `allow_plaintext_credentials` and `will_retain`
/// must be 0 or 1. A will is absent when `will_topic` is null and
/// `will_topic_len` is zero.
/// `client_cert_pem` and `client_key_pem` are copied immediately; null + zero
/// length means the field is absent. Both must be provided together and only
/// when `use_tls` is 1. They are not written to disk.
#[no_mangle]
pub unsafe extern "C" fn mqtt_client_connect(
    handle: u64,
    op: u64,
    protocol: u8,
    keep_alive_seconds: u16,
    use_tls: u8,
    ca_pem: *const u8,
    ca_pem_len: usize,
    username: *const u8,
    username_len: usize,
    password: *const u8,
    password_len: usize,
    allow_plaintext_credentials: u8,
    will_topic: *const u8,
    will_topic_len: usize,
    will_payload: *const u8,
    will_payload_len: usize,
    will_qos: u8,
    will_retain: u8,
    client_cert_pem: *const u8,
    client_cert_pem_len: usize,
    client_key_pem: *const u8,
    client_key_pem_len: usize,
) -> i32 {
    boundary(|| unsafe {
        apply_connect(ConnectParams {
            handle,
            op,
            protocol,
            keep_alive_seconds,
            use_tls,
            ca_pem,
            ca_pem_len,
            username,
            username_len,
            password,
            password_len,
            allow_plaintext_credentials,
            will_topic,
            will_topic_len,
            will_payload,
            will_payload_len,
            will_qos,
            will_retain,
            client_cert_pem,
            client_cert_pem_len,
            client_key_pem,
            client_key_pem_len,
            clean_session: 1,
            reconnect: ReconnectPolicy::default(),
        })
    })
}

/// Additive connect: MQTT 3.1.1 `clean_session` (0 or 1) and an optional reconnect policy.
/// A null `reconnect` pointer uses the default disabled policy. Existing
/// `mqtt_client_connect` remains clean_session=1 with reconnect off.
///
/// # Safety
/// Same pointer rules as `mqtt_client_connect`. `reconnect` may be null; a
/// non-null pointer must be aligned and readable for `MqttReconnectPolicy`.
#[no_mangle]
pub unsafe extern "C" fn mqtt_client_connect_ex(
    handle: u64,
    op: u64,
    protocol: u8,
    keep_alive_seconds: u16,
    use_tls: u8,
    ca_pem: *const u8,
    ca_pem_len: usize,
    username: *const u8,
    username_len: usize,
    password: *const u8,
    password_len: usize,
    allow_plaintext_credentials: u8,
    will_topic: *const u8,
    will_topic_len: usize,
    will_payload: *const u8,
    will_payload_len: usize,
    will_qos: u8,
    will_retain: u8,
    client_cert_pem: *const u8,
    client_cert_pem_len: usize,
    client_key_pem: *const u8,
    client_key_pem_len: usize,
    clean_session: u8,
    reconnect: *const MqttReconnectPolicy,
) -> i32 {
    boundary(|| unsafe {
        let policy = if reconnect.is_null() {
            ReconnectPolicy::default()
        } else {
            let raw = *reconnect;
            if raw.enabled > 1 {
                return Err(Error::InvalidArgument);
            }
            ReconnectPolicy {
                enabled: raw.enabled == 1,
                max_attempts: raw.max_attempts,
                base_delay: Duration::from_millis(raw.base_delay_ms as u64),
                max_delay: Duration::from_millis(raw.max_delay_ms as u64),
            }
        };
        apply_connect(ConnectParams {
            handle,
            op,
            protocol,
            keep_alive_seconds,
            use_tls,
            ca_pem,
            ca_pem_len,
            username,
            username_len,
            password,
            password_len,
            allow_plaintext_credentials,
            will_topic,
            will_topic_len,
            will_payload,
            will_payload_len,
            will_qos,
            will_retain,
            client_cert_pem,
            client_cert_pem_len,
            client_key_pem,
            client_key_pem_len,
            clean_session,
            reconnect: policy,
        })
    })
}
/// # Safety
/// topic must reference topic_len readable bytes.
#[no_mangle]
pub unsafe extern "C" fn mqtt_client_subscribe(
    handle: u64,
    op: u64,
    topic: *const u8,
    topic_len: usize,
    qos: u8,
) -> i32 {
    boundary(|| {
        let topic = unsafe { text_ref(topic, topic_len)? };
        entry(handle, |e| e.client.subscribe(op, topic.to_owned(), qos))
    })
}
/// # Safety
/// topic and payload must reference their readable lengths; null allowed iff length=0.
#[no_mangle]
pub unsafe extern "C" fn mqtt_client_publish(
    handle: u64,
    op: u64,
    topic: *const u8,
    topic_len: usize,
    payload: *const u8,
    payload_len: usize,
    qos: u8,
    retain: u8,
) -> i32 {
    boundary(|| {
        if retain > 1 {
            return Err(Error::InvalidArgument);
        }
        let topic = unsafe { text_ref(topic, topic_len)? };
        let payload = unsafe { bytes(payload, payload_len, mqtt_core::MAX_PAYLOAD)? };
        entry(handle, |e| {
            e.client
                .publish(op, topic.to_owned(), payload.to_vec(), qos, retain != 0)
        })
    })
}
/// # Safety
/// topic must reference topic_len readable bytes.
#[no_mangle]
pub unsafe extern "C" fn mqtt_client_unsubscribe(
    handle: u64,
    op: u64,
    topic: *const u8,
    topic_len: usize,
) -> i32 {
    boundary(|| {
        let topic = unsafe { text_ref(topic, topic_len)? };
        entry(handle, |e| e.client.unsubscribe(op, topic.to_owned()))
    })
}
#[no_mangle]
pub extern "C" fn mqtt_client_disconnect(handle: u64, op: u64) -> i32 {
    boundary(|| entry(handle, |e| e.client.disconnect(op)))
}
/// Begin optional graceful close without blocking. timeout_ms must be 1..300000.
/// First accepted deadline wins; repeats join the same irreversible attempt.
/// New operations return Closed after acceptance; connecting/disconnecting -> Busy.
/// Poll retained control completions, not inbound messages. QoS1 waits for PUBACK;
/// success is not subscriber delivery. Caller must still close/free the handle.
#[no_mangle]
pub extern "C" fn mqtt_client_drain_begin(handle: u64, timeout_ms: u32) -> i32 {
    boundary(|| {
        let mut store = CLIENTS.lock().map_err(|_| Error::Internal)?;
        let e = store.1.get_mut(&handle).ok_or(Error::Closed)?;
        e.client
            .begin_drain(Duration::from_millis(timeout_ms as u64))
    })
}
/// Nonblocking out-of-band drain result, available even after force close until
/// free. 0 not-started, 1 pending, 2 drained, 3 timed-out, 4 failed, 5 forced.
/// Drained includes engine Outgoing Disconnect, not proof of broker receipt.
/// # Safety
/// out_status must point to aligned writable u32. Errors leave it unchanged.
#[no_mangle]
pub unsafe extern "C" fn mqtt_client_drain_status(handle: u64, out_status: *mut u32) -> i32 {
    boundary(|| {
        if out_status.is_null() {
            return Err(Error::InvalidArgument);
        }
        let store = CLIENTS.lock().map_err(|_| Error::Internal)?;
        let e = store.1.get(&handle).ok_or(Error::Closed)?;
        let status = e.client.drain_status()? as u32;
        unsafe {
            out_status.write(status);
        }
        Ok(())
    })
}
/// Poll without blocking. BUFFER_TOO_SMALL (9) writes metadata but retains event.
/// EMPTY (10) does not change caller memory. Native never retains caller pointers.
/// # Safety
/// out must be writable/aligned. Non-null buffers must be writable for capacities;
/// all output regions must be non-overlapping. Null buffers require capacity zero.
#[no_mangle]
pub unsafe extern "C" fn mqtt_client_poll(
    handle: u64,
    out: *mut MqttEvent,
    topic: *mut u8,
    topic_capacity: usize,
    payload: *mut u8,
    payload_capacity: usize,
) -> i32 {
    boundary(|| {
        if out.is_null()
            || (topic.is_null() && topic_capacity != 0)
            || (payload.is_null() && payload_capacity != 0)
        {
            return Err(Error::InvalidArgument);
        }
        entry(handle, |e| {
            let event = e.client.peek()?;
            unsafe {
                out.write(MqttEvent {
                    kind: event.kind as u32,
                    code: event.code,
                    operation_id: event.op,
                    packet_id: event.packet_id,
                    qos: event.qos,
                    session_present: event.session_present as u8,
                    topic_len: event.topic.len(),
                    payload_len: event.payload.len(),
                });
            }
            if topic_capacity < event.topic.len() || payload_capacity < event.payload.len() {
                return Err(Error::BufferTooSmall);
            }
            unsafe {
                if !event.topic.is_empty() {
                    std::ptr::copy_nonoverlapping(event.topic.as_ptr(), topic, event.topic.len());
                }
                if !event.payload.is_empty() {
                    std::ptr::copy_nonoverlapping(
                        event.payload.as_ptr(),
                        payload,
                        event.payload.len(),
                    );
                }
            }
            e.client.pop()?;
            Ok(())
        })
    })
}
/// Prototype force-close, idempotent; aborts pending work. No graceful drain promise.
/// After a successful close, the handle stays Closed until free; poll and other
/// operations except repeated close/free return Closed and cannot deliver events.
#[no_mangle]
pub extern "C" fn mqtt_client_close(handle: u64) -> i32 {
    boundary(|| {
        let mut s = CLIENTS.lock().map_err(|_| Error::Internal)?;
        let e = s.1.get_mut(&handle).ok_or(Error::Closed)?;
        e.closed = true;
        e.options.clear_secrets();
        e.client.close()
    })
}
/// Idempotent numeric-handle release. Must close first. Never dereferences a handle.
#[no_mangle]
pub extern "C" fn mqtt_client_free(handle: u64) -> i32 {
    boundary(|| {
        let mut s = CLIENTS.lock().map_err(|_| Error::Internal)?;
        if s.1.get(&handle).is_some_and(|e| !e.closed) {
            return Err(Error::Busy);
        }
        s.1.remove(&handle);
        Ok(())
    })
}

// Not product ABI: opt-in test observer only, no header/generated binding additions.
#[cfg(feature = "ws-test-observer")]
#[no_mangle]
pub extern "C" fn mqtt_test_ws_probe(index: u32) -> u64 {
    mqtt_core::ws_test_observer::read(index)
}
#[cfg(feature = "ws-test-observer")]
#[no_mangle]
pub extern "C" fn mqtt_test_ws_probe_reset() {
    mqtt_core::ws_test_observer::reset();
}

#[cfg(test)]
mod tests {
    #[test]
    fn catches_unwind() {
        assert_eq!(super::boundary(|| panic!("test-only panic")), 11);
    }
}
