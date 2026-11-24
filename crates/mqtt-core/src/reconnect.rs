use mqtt_api::ReconnectPolicy;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// `min(max, base * 2^(attempt-1))` with saturation. Attempt 1 uses `base`.
pub fn delay_cap(attempt: u32, base: Duration, max: Duration) -> Duration {
    if attempt == 0 || base.is_zero() {
        return Duration::ZERO;
    }
    let exp = attempt.saturating_sub(1).min(16);
    let cap = base.saturating_mul(1u32 << exp);
    if cap > max {
        max
    } else {
        cap
    }
}

/// Equal jitter: `cap/2 + sample * cap/2` for `sample` in `[0, 1)`.
pub fn equal_jitter(cap: Duration, sample: f64) -> Duration {
    if cap.is_zero() {
        return cap;
    }
    let sample = sample.clamp(0.0, 0.999_999_999);
    let nanos = cap.as_nanos() as f64;
    let half = nanos / 2.0;
    Duration::from_nanos((half + half * sample) as u64)
}

fn random_unit() -> f64 {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let s = SEQ.fetch_add(1, Ordering::Relaxed);
    let mut x = t ^ s.wrapping_mul(0x9E3779B97F4A7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58476D1CE4E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D049BB133111EB);
    x ^= x >> 31;
    (x as f64) / (u64::MAX as f64)
}

pub fn jittered(policy: &ReconnectPolicy, attempt: u32) -> Duration {
    equal_jitter(
        delay_cap(attempt, policy.base_delay, policy.max_delay),
        random_unit(),
    )
}
