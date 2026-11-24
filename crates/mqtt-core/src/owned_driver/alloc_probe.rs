//! Requested-allocation instrumentation, private test binary only, not RSS.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};
thread_local! {static LIVE:Cell<usize>=const{Cell::new(0)};static PEAK:Cell<usize>=const{Cell::new(0)};static MAX:Cell<usize>=const{Cell::new(0)};static TRACK:Cell<bool>=const{Cell::new(false)};}
struct Counted;
fn add(n: usize) {
    let _ = LIVE.try_with(|l| {
        l.set(l.get() + n);
        let _ = TRACK.try_with(|t| {
            if t.get() {
                PEAK.with(|p| p.set(p.get().max(l.get())));
                MAX.with(|m| m.set(m.get().max(n)));
            }
        });
    });
}
fn sub(n: usize) {
    let _ = LIVE.try_with(|l| l.set(l.get().saturating_sub(n)));
}
// SAFETY: exact forwarding of System pointers/Layout/size; const TLS Cells allocate
// nothing. Measurements require current-thread ownership, not cross-thread heaps.
unsafe impl GlobalAlloc for Counted {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = System.alloc(l);
        if !p.is_null() {
            add(l.size());
        }
        p
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(l);
        if !p.is_null() {
            add(l.size());
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l);
        sub(l.size());
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let q = System.realloc(p, l, n);
        if !q.is_null() {
            if n >= l.size() {
                add(n - l.size());
            } else {
                sub(l.size() - n);
            }
            TRACK.with(|t| {
                if t.get() {
                    MAX.with(|m| m.set(m.get().max(n)));
                }
            });
        }
        q
    }
}
#[global_allocator]
static ALLOC: Counted = Counted;
pub(super) fn measure(f: impl FnOnce()) -> (usize, usize, bool) {
    let baseline = LIVE.with(Cell::get);
    PEAK.with(|p| p.set(baseline));
    MAX.with(|m| m.set(0));
    TRACK.with(|t| t.set(true));
    f();
    TRACK.with(|t| t.set(false));
    (
        PEAK.with(Cell::get).saturating_sub(baseline),
        MAX.with(Cell::get),
        LIVE.with(Cell::get) == baseline,
    )
}
