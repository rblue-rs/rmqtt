//! Experimental polling C ABI. Caller-owned buffers; no cross-thread callbacks.
use mqtt_api::{Error, Options};
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
    f(store.1.get_mut(&handle).ok_or(Error::Closed)?)
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
unsafe fn text(p: *const u8, len: usize) -> Result<String, Error> {
    let b = unsafe { bytes(p, len, 65535)? };
    std::str::from_utf8(b)
        .map(str::to_owned)
        .map_err(|_| Error::InvalidArgument)
}
#[no_mangle]
pub extern "C" fn mqtt_runtime_init() -> i32 {
    boundary(mqtt_runtime::init)
}
#[no_mangle]
pub extern "C" fn mqtt_runtime_shutdown() -> i32 {
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
#[no_mangle]
pub extern "C" fn mqtt_client_connect(
    handle: u64,
    op: u64,
    protocol: u8,
    keep_alive_seconds: u16,
) -> i32 {
    boundary(|| {
        entry(handle, |e| {
            let mut opts = e.options.clone();
            opts.protocol = protocol;
            opts.keep_alive = Duration::from_secs(keep_alive_seconds as u64);
            e.client.connect(op, opts)
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
        let topic = unsafe { text(topic, topic_len)? };
        entry(handle, |e| e.client.subscribe(op, topic, qos))
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
        let topic = unsafe { text(topic, topic_len)? };
        let payload = unsafe { bytes(payload, payload_len, mqtt_core::MAX_PAYLOAD)? }.to_vec();
        entry(handle, |e| {
            e.client.publish(op, topic, payload, qos, retain != 0)
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
        let topic = unsafe { text(topic, topic_len)? };
        entry(handle, |e| e.client.unsubscribe(op, topic))
    })
}
#[no_mangle]
pub extern "C" fn mqtt_client_disconnect(handle: u64, op: u64) -> i32 {
    boundary(|| entry(handle, |e| e.client.disconnect(op)))
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
#[no_mangle]
pub extern "C" fn mqtt_client_close(handle: u64) -> i32 {
    boundary(|| {
        entry(handle, |e| {
            e.client.close()?;
            e.closed = true;
            Ok(())
        })
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

#[cfg(test)]
mod tests {
    #[test]
    fn catches_unwind() {
        assert_eq!(super::boundary(|| panic!("test-only panic")), 11);
    }
}
