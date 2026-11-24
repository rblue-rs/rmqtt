//! Public ABI regression based on independent reviewer retirement probe.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        mpsc, Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};
struct Track;
static LIVE: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Track {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            LIVE.fetch_add(l.size(), SeqCst);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), SeqCst);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, n) };
        if !q.is_null() {
            LIVE.fetch_sub(l.size(), SeqCst);
            LIVE.fetch_add(n, SeqCst);
        }
        q
    }
}
#[global_allocator]
static ALLOC: Track = Track;
unsafe fn new_client(id: &[u8]) -> u64 {
    let mut h = 0;
    let host = b"127.0.0.1";
    assert_eq!(
        unsafe {
            mqtt_c::mqtt_client_new(
                host.as_ptr(),
                host.len(),
                id.as_ptr(),
                id.len(),
                1883,
                &mut h,
            )
        },
        0
    );
    assert_eq!(
        mqtt_c::mqtt_client_set_websocket(h, 1, std::ptr::null(), 0),
        0
    );
    h
}
unsafe fn connect(h: u64, op: u64) -> i32 {
    let n = std::ptr::null();
    unsafe {
        mqtt_c::mqtt_client_connect(
            h, op, 4, 0, 0, n, 0, n, 0, n, 0, 0, n, 0, n, 0, 0, 0, n, 0, n, 0,
        )
    }
}
#[test]
fn public_abi_retiring_quota_pre_first_poll_and_recovery() {
    mqtt_runtime::init().unwrap();
    let rt = mqtt_runtime::acquire().unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (tx, rx) = mpsc::channel();
    for _ in 0..2 {
        let g = gate.clone();
        let tx = tx.clone();
        rt.spawn(async move {
            tx.send(()).unwrap();
            let mut open = g.0.lock().unwrap();
            while !*open {
                open = g.1.wait(open).unwrap();
            }
        });
    }
    for _ in 0..2 {
        rx.recv_timeout(Duration::from_secs(3)).unwrap();
    }
    let id = vec![b'x'; 60000];
    let mut codes = Vec::with_capacity(1000);
    // Always release scratch gates, including assertion panic, to avoid runtime deadlock.
    struct Release(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0 .0.lock().unwrap() = true;
            self.0 .1.notify_all();
        }
    }
    let release = Release(gate.clone());
    // Synchronous TLS/session-construction errors must not leak quota permits.
    unsafe {
        let h = new_client(&id);
        let n = std::ptr::null();
        let bad = b"dummy-invalid-CA";
        for op in 1..=128 {
            assert_eq!(
                mqtt_c::mqtt_client_connect(
                    h,
                    op,
                    4,
                    0,
                    1,
                    bad.as_ptr(),
                    bad.len(),
                    n,
                    0,
                    n,
                    0,
                    0,
                    n,
                    0,
                    n,
                    0,
                    0,
                    0,
                    n,
                    0,
                    n,
                    0
                ),
                1 // tls::custom_roots rejects empty parsed cert list as InvalidArgument.
            );
        }
        assert_eq!(mqtt_c::mqtt_client_close(h), 0);
        assert_eq!(mqtt_c::mqtt_client_free(h), 0);
    }
    for _ in 0..1000 {
        unsafe {
            let h = new_client(&id);
            codes.push(connect(h, 1));
            let start = Instant::now();
            assert_eq!(mqtt_c::mqtt_client_close(h), 0);
            assert!(start.elapsed() < Duration::from_secs(1));
            assert_eq!(mqtt_c::mqtt_client_free(h), 0);
        }
    }
    let accepted = codes.iter().filter(|&&c| c == 0).count();
    println!(
        "worker-blocked closed/freed1000 accepted={accepted} rejected={}",
        1000 - accepted
    );
    assert_eq!(
        accepted, 64,
        "retired future generations must remain charged before first poll"
    );
    assert!(codes[..64].iter().all(|&c| c == 0));
    assert!(codes[64..].iter().all(|&c| c == 5));
    // Rejected churn must not retain another generation worth of captures.
    let plateau = LIVE.load(SeqCst);
    for _ in 0..1000 {
        unsafe {
            let h = new_client(&id);
            assert_eq!(connect(h, 1), 5);
            assert_eq!(mqtt_c::mqtt_client_close(h), 0);
            assert_eq!(mqtt_c::mqtt_client_free(h), 0);
        }
    }
    let after = LIVE.load(SeqCst);
    assert!(
        after <= plateau + 65536,
        "quota-rejected churn accumulated own backing: {plateau}->{after}"
    );
    println!("rejected-churn requested plateau={plateau} after={after}");
    drop(release);
    unsafe {
        let h = new_client(&id);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let code = connect(h, 1);
            if code == 0 {
                break;
            }
            assert_eq!(code, 5);
            assert!(
                Instant::now() < deadline,
                "permit not recovered after actual destruction"
            );
            std::thread::yield_now();
        }
        assert_eq!(mqtt_c::mqtt_client_close(h), 0);
        assert_eq!(mqtt_c::mqtt_client_free(h), 0);
    }
    drop(rt);
}
