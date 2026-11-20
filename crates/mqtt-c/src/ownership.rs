//! Once-only native resource ownership. No pointers are delivered to Dart listeners.
use super::{boundary, entry, mqtt_client_close, mqtt_client_free, MqttEvent};
use mqtt_api::Error;
use std::{
    collections::{HashMap, HashSet},
    ffi::c_void,
    sync::{
        atomic::{AtomicI64, Ordering},
        Condvar, LazyLock, Mutex,
    },
    time::{Duration, Instant},
};

#[allow(non_camel_case_types)]
pub type Dart_Handle = *mut _Dart_Handle;
/// Opaque VM handle. Only valid in a current Dart isolate/FFI scope.
#[allow(non_camel_case_types)]
pub struct _Dart_Handle {
    _private: (),
}
/// Native allocator deallocator, never a Dart trampoline.
pub type MqttNativeFree = Option<unsafe extern "C" fn(*mut c_void)>;
unsafe extern "C" {
    fn rmqtt_dart_initialize(api: *mut c_void, cleanup: extern "C" fn(u64)) -> i64;
    fn rmqtt_dart_send_port(port: i64) -> *mut c_void;
}
static API_INIT: Mutex<()> = Mutex::new(());
static EXIT_PORT: AtomicI64 = AtomicI64::new(0);
struct Resources {
    owner: usize,
    handle: u64,
    event: usize,
    topic: usize,
    payload: usize,
    free: unsafe extern "C" fn(*mut c_void),
}
#[derive(Default)]
struct Registry {
    next_owner: usize,
    next_client: usize,
    owners: HashSet<usize>,
    clients: HashMap<usize, Resources>,
    cleaning: usize,
    owner_slots: usize,
    resource_slots: usize,
}
static REGISTRY: LazyLock<(Mutex<Registry>, Condvar)> = LazyLock::new(Default::default);
fn next(value: &mut usize) -> Result<usize, Error> {
    *value = value
        .checked_add(1)
        .filter(|v| *v <= isize::MAX as usize)
        .ok_or(Error::Internal)?;
    Ok(*value)
}
extern "C" fn exit_owner(id: u64) {
    let _ = boundary(|| release_owner(id as usize, true));
}
/// Initialize official Dart API DL and one process-wide native exit receiver.
/// # Safety
/// api must be NativeApi.initializeApiDLData from the current Dart VM.
#[no_mangle]
pub unsafe extern "C" fn mqtt_dart_initialize(api: *mut c_void) -> i32 {
    boundary(|| {
        if api.is_null() {
            return Err(Error::InvalidArgument);
        }
        let _guard = API_INIT.lock().map_err(|_| Error::Internal)?;
        if EXIT_PORT.load(Ordering::Acquire) == 0 {
            let port = unsafe { rmqtt_dart_initialize(api, exit_owner) };
            if port == 0 {
                return Err(Error::Unsupported);
            }
            EXIT_PORT.store(port, Ordering::Release);
        }
        Ok(())
    })
}
/// Return the native-only exit receiver's SendPort as a Dart handle.
/// # Safety
/// Requires successful mqtt_dart_initialize and a current Dart isolate/FFI scope.
#[no_mangle]
pub unsafe extern "C" fn mqtt_dart_exit_send_port() -> Dart_Handle {
    let port = EXIT_PORT.load(Ordering::Acquire);
    unsafe { rmqtt_dart_send_port(port).cast() }
}
/// Acquire one runtime init reference for an isolate owner (maximum 64 owners).
/// # Safety
/// out must be aligned/writable. Dart users establish an exit listener before
/// claiming owner-exit support; plain C users must explicitly release owners.
#[no_mangle]
pub unsafe extern "C" fn mqtt_dart_owner_new(out: *mut usize) -> i32 {
    boundary(|| {
        if out.is_null() {
            return Err(Error::InvalidArgument);
        }
        let mut r = REGISTRY.0.lock().map_err(|_| Error::Internal)?;
        if r.owner_slots >= 64 {
            return Err(Error::QueueFull);
        }
        let id = next(&mut r.next_owner)?;
        mqtt_runtime::init()?;
        r.owners.insert(id);
        r.owner_slots += 1;
        unsafe {
            out.write(id);
        }
        Ok(())
    })
}
/// Pair an owner's runtime reference. Busy while it has registered clients.
#[no_mangle]
pub extern "C" fn mqtt_dart_owner_shutdown(owner: usize) -> i32 {
    boundary(|| release_owner(owner, false))
}
/// NativeFinalizer fallback for an inaccessible owner; encoded key, not a pointer.
/// No Dart calls or blocking runtime destruction; repeated cleanup is harmless.
#[no_mangle]
pub extern "C" fn mqtt_dart_owner_finalize(token: *mut c_void) {
    exit_owner(token as usize as u64);
}
fn release_owner(owner: usize, force: bool) -> Result<(), Error> {
    if !force {
        let mut r = REGISTRY.0.lock().map_err(|_| Error::Internal)?;
        if !r.owners.contains(&owner) {
            return Ok(());
        }
        if r.clients.values().any(|c| c.owner == owner) {
            return Err(Error::Busy);
        }
        // Explicit shutdown retains the ordinary runtime/live-lease/worker guard.
        // Only inaccessible-owner callbacks use nonblocking background release.
        mqtt_runtime::shutdown()?;
        r.owners.remove(&owner);
        r.owner_slots -= 1;
        REGISTRY.1.notify_all();
        return Ok(());
    }
    let clients = {
        let mut r = REGISTRY.0.lock().map_err(|_| Error::Internal)?;
        if !r.owners.contains(&owner) {
            return Ok(());
        }
        if !force && r.clients.values().any(|c| c.owner == owner) {
            return Err(Error::Busy);
        }
        r.owners.remove(&owner);
        let ids: Vec<_> = r
            .clients
            .iter()
            .filter(|(_, c)| c.owner == owner)
            .map(|(id, _)| *id)
            .collect();
        let clients: Vec<_> = ids
            .into_iter()
            .filter_map(|id| r.clients.remove(&id))
            .collect();
        r.cleaning += 1;
        clients
    };
    let mut result = Ok(());
    let mut released = 0;
    for client in clients {
        let disposed = dispose(client);
        if disposed.is_ok() {
            released += 1;
        }
        result = disposed.and(result);
    }
    // Never drop a Tokio runtime with blocking shutdown on a VM finalizer thread.
    let owner_released = mqtt_runtime::release_background();
    let owner_done = owner_released.is_ok();
    result = owner_released.and(result);
    finish_cleanup(released, owner_done);
    result
}
/// Transfer ownership of all three full-sized caller buffers and one native handle.
/// # Safety
/// Buffers must be distinct allocations from the supplied native free function,
/// sized/aligned for MqttEvent, 65535 topic bytes and 65536 payload bytes. Free must
/// not be a Dart trampoline. On success caller must use client_release/finalize,
/// never independently free buffers. out must be writable/aligned. No native event
/// pointer is returned; these remain caller-owned poll copy destinations.
#[no_mangle]
pub unsafe extern "C" fn mqtt_dart_client_register(
    owner: usize,
    handle: u64,
    event: *mut MqttEvent,
    topic: *mut u8,
    payload: *mut u8,
    free: MqttNativeFree,
    out: *mut usize,
) -> i32 {
    boundary(|| {
        if event.is_null()
            || topic.is_null()
            || payload.is_null()
            || free.is_none()
            || out.is_null()
        {
            return Err(Error::InvalidArgument);
        }
        let mut r = REGISTRY.0.lock().map_err(|_| Error::Internal)?;
        if !r.owners.contains(&owner) {
            return Err(Error::Closed);
        }
        if r.resource_slots >= 64 {
            return Err(Error::QueueFull);
        }
        if r.clients.values().any(|c| c.handle == handle) {
            return Err(Error::Busy);
        }
        entry(handle, |_| Ok(()))?;
        let token = next(&mut r.next_client)?;
        r.resource_slots += 1;
        r.clients.insert(
            token,
            Resources {
                owner,
                handle,
                event: event as usize,
                topic: topic as usize,
                payload: payload as usize,
                free: free.unwrap(),
            },
        );
        unsafe {
            out.write(token);
        }
        Ok(())
    })
}
/// Once-only close -> free -> matching buffer deallocator; stale tokens are harmless.
#[no_mangle]
pub extern "C" fn mqtt_dart_client_release(token: usize) -> i32 {
    boundary(|| {
        let client = {
            let mut r = REGISTRY.0.lock().map_err(|_| Error::Internal)?;
            match r.clients.remove(&token) {
                None => return Ok(()),
                Some(c) => {
                    r.cleaning += 1;
                    c
                }
            }
        };
        let result = dispose(client);
        finish_cleanup(usize::from(result.is_ok()), false);
        result
    })
}
/// NativeFinalizer callback: token is an encoded registry key, never dereferenced.
/// Safe on arbitrary native threads; does not call Dart or block on a Tokio worker.
#[no_mangle]
pub extern "C" fn mqtt_dart_client_finalize(token: *mut c_void) {
    let _ = mqtt_dart_client_release(token as usize);
}
fn dispose(c: Resources) -> Result<(), Error> {
    let close = mqtt_client_close(c.handle);
    if close != 0 && close != Error::Closed as i32 {
        return Err(Error::Internal);
    }
    if mqtt_client_free(c.handle) != 0 {
        return Err(Error::Internal);
    }
    unsafe {
        (c.free)(c.event as *mut c_void);
        (c.free)(c.topic as *mut c_void);
        (c.free)(c.payload as *mut c_void);
    }
    Ok(())
}
fn finish_cleanup(resources: usize, owner: bool) {
    if let Ok(mut r) = REGISTRY.0.lock() {
        r.cleaning -= 1;
        r.resource_slots -= resources;
        r.owner_slots -= usize::from(owner);
        REGISTRY.1.notify_all();
    }
    let _ = mqtt_runtime::collect_idle_background();
}
pub(super) fn wait_for_exit_cleanup() {
    if tokio_worker() {
        return;
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    if let Ok(mut r) = REGISTRY.0.lock() {
        while !r.owners.is_empty() || r.cleaning != 0 {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match REGISTRY.1.wait_timeout(r, deadline - now) {
                Ok((guard, _)) => r = guard,
                Err(_) => break,
            }
        }
    }
}
fn tokio_worker() -> bool {
    mqtt_runtime::on_runtime_thread()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    static FREES: AtomicUsize = AtomicUsize::new(0);
    static BLOCK: AtomicBool = AtomicBool::new(false);
    static ENTERED: AtomicBool = AtomicBool::new(false);
    static ALLOW: AtomicBool = AtomicBool::new(false);
    unsafe extern "C" {
        fn calloc(n: usize, size: usize) -> *mut c_void;
        fn free(p: *mut c_void);
    }
    unsafe extern "C" fn counted_free(p: *mut c_void) {
        if BLOCK.swap(false, Ordering::SeqCst) {
            ENTERED.store(true, Ordering::SeqCst);
            while !ALLOW.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
        }
        FREES.fetch_add(1, Ordering::SeqCst);
        unsafe {
            free(p);
        }
    }
    fn owner() -> usize {
        let mut id = 0;
        assert_eq!(unsafe { mqtt_dart_owner_new(&mut id) }, 0);
        id
    }
    fn raw_client() -> (u64, usize, usize, usize) {
        let mut h = 0;
        assert_eq!(
            unsafe {
                super::super::mqtt_client_new(
                    b"localhost".as_ptr(),
                    9,
                    b"owned".as_ptr(),
                    5,
                    1883,
                    &mut h,
                )
            },
            0
        );
        unsafe {
            (
                h,
                calloc(1, std::mem::size_of::<MqttEvent>()) as usize,
                calloc(65535, 1) as usize,
                calloc(65536, 1) as usize,
            )
        }
    }
    fn register(owner: usize, c: (u64, usize, usize, usize)) -> (i32, usize) {
        let mut token = 0;
        let code = unsafe {
            mqtt_dart_client_register(
                owner,
                c.0,
                c.1 as *mut MqttEvent,
                c.2 as *mut u8,
                c.3 as *mut u8,
                Some(counted_free),
                &mut token,
            )
        };
        (code, token)
    }
    #[test]
    fn registered_and_retiring_buffers_stay_bounded_and_cleanup_is_once_only() {
        assert_eq!(unsafe { mqtt_dart_initialize(std::ptr::null_mut()) }, 1);
        let guarded_owner = owner();
        let raw = raw_client();
        let guarded_shutdown = mqtt_dart_owner_shutdown(guarded_owner);
        assert_eq!(mqtt_client_close(raw.0), 0);
        assert_eq!(mqtt_client_free(raw.0), 0);
        unsafe {
            free(raw.1 as *mut c_void);
            free(raw.2 as *mut c_void);
            free(raw.3 as *mut c_void);
        }
        assert_eq!(mqtt_dart_owner_shutdown(guarded_owner), 0);
        assert_eq!(
            guarded_shutdown, 4,
            "explicit shutdown must still refuse an unregistered live native lease"
        );
        let id = owner();
        let mut tokens = Vec::new();
        for _ in 0..64 {
            let c = raw_client();
            let (code, token) = register(id, c);
            assert_eq!(code, 0);
            tokens.push(token);
        }
        assert_eq!(mqtt_dart_owner_shutdown(id), 4);
        BLOCK.store(true, Ordering::SeqCst);
        let first = tokens.remove(0);
        let cleaner = std::thread::spawn(move || mqtt_dart_client_release(first));
        let deadline = Instant::now() + Duration::from_secs(2);
        while !ENTERED.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let entered = ENTERED.load(Ordering::SeqCst);
        let raw = raw_client();
        let (code, token) = register(id, raw);
        ALLOW.store(true, Ordering::SeqCst);
        assert_eq!(cleaner.join().unwrap(), 0);
        if code == 0 {
            assert_eq!(mqtt_dart_client_release(token), 0);
        } else {
            assert_eq!(mqtt_client_close(raw.0), 0);
            assert_eq!(mqtt_client_free(raw.0), 0);
            unsafe {
                counted_free(raw.1 as *mut c_void);
                counted_free(raw.2 as *mut c_void);
                counted_free(raw.3 as *mut c_void);
            }
        }
        for token in tokens {
            assert_eq!(mqtt_dart_client_release(token), 0);
        }
        assert_eq!(mqtt_dart_owner_shutdown(id), 0);
        assert!(entered, "cleanup barrier was not reached");
        assert_eq!(
            code, 5,
            "retiring allocations must still consume the 64-buffer-set quota"
        );
        assert_eq!(FREES.load(Ordering::SeqCst), 65 * 3);
        for _ in 0..32 {
            let id = owner();
            let c = raw_client();
            let (code, token) = register(id, c);
            assert_eq!(code, 0);
            let before = FREES.load(Ordering::SeqCst);
            let handle = c.0;
            let poller = std::thread::spawn(move || {
                let mut out = MqttEvent::default();
                for _ in 0..10000 {
                    let code = unsafe {
                        super::super::mqtt_client_poll(
                            handle,
                            &mut out,
                            std::ptr::null_mut(),
                            0,
                            std::ptr::null_mut(),
                            0,
                        )
                    };
                    if code == 3 {
                        return;
                    }
                    assert_eq!(code, 10);
                }
            });
            let mut threads = Vec::new();
            threads.push(std::thread::spawn(move || {
                mqtt_dart_owner_finalize(id as *mut c_void);
            }));
            for i in 0..8 {
                threads.push(std::thread::spawn(move || {
                    if i % 2 == 0 {
                        assert_eq!(mqtt_dart_client_release(token), 0);
                    } else {
                        mqtt_dart_client_finalize(token as *mut c_void);
                    }
                }));
            }
            for t in threads {
                t.join().unwrap();
            }
            poller.join().unwrap();
            assert_eq!(mqtt_dart_owner_shutdown(id), 0);
            assert_eq!(
                FREES.load(Ordering::SeqCst) - before,
                3,
                "free each buffer exactly once"
            );
        }
        let owners: Vec<_> = (0..64).map(|_| owner()).collect();
        let mut extra = 0;
        assert_eq!(unsafe { mqtt_dart_owner_new(&mut extra) }, 5);
        for id in owners {
            assert_eq!(mqtt_dart_owner_shutdown(id), 0);
        }
        let r = REGISTRY.0.lock().unwrap();
        assert!(r.clients.is_empty() && r.owners.is_empty());
        assert_eq!(r.cleaning, 0);
        assert_eq!(r.resource_slots, 0);
        assert_eq!(r.owner_slots, 0);
    }
}
