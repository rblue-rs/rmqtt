//! Opt-in test diagnostics: fixed counters, no payload retention or injected IO.
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
static VALUES: [AtomicU64; 9] = [const { AtomicU64::new(0) }; 9];
pub fn read(index: u32) -> u64 {
    VALUES.get(index as usize).map_or(0, |v| v.load(SeqCst))
}
pub fn reset() {
    for v in &VALUES {
        v.store(0, SeqCst);
    }
}
pub(crate) fn created() {
    VALUES[0].fetch_add(1, SeqCst);
    VALUES[5].fetch_add(1, SeqCst);
}
pub(crate) fn pending(value: bool, bytes: usize) {
    VALUES[1].store(value as u64, SeqCst);
    VALUES[6].store(bytes as u64, SeqCst);
    if value {
        VALUES[2].fetch_add(1, SeqCst);
    }
}
pub(crate) fn dropped() {
    VALUES[0].fetch_sub(1, SeqCst);
    VALUES[4].fetch_add(1, SeqCst);
    VALUES[1].store(0, SeqCst);
    VALUES[6].store(0, SeqCst);
}
pub(crate) fn closing() {
    if read(1) != 0 {
        VALUES[3].fetch_add(1, SeqCst);
    }
    VALUES[8].fetch_add(1, SeqCst);
}
pub(crate) struct Actor;
impl Actor {
    pub fn new() -> Self {
        VALUES[7].fetch_add(1, SeqCst);
        Self
    }
}
impl Drop for Actor {
    fn drop(&mut self) {
        VALUES[7].fetch_sub(1, SeqCst);
    }
}
