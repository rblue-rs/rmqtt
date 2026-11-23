//! Shared admission counts LIVE + RETIRING supervisor futures, not client handles.
use crate::Error;
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};

// Shared by every Client using this linked core, all transports/protocols.
const LIMIT: usize = 64;
struct Quota {
    used: AtomicUsize,
    limit: usize,
}
static QUOTA: Quota = Quota {
    used: AtomicUsize::new(0),
    limit: LIMIT,
};
impl Quota {
    fn acquire(&self) -> Result<Permit<'_>, Error> {
        let mut n = self.used.load(Ordering::Acquire);
        loop {
            if n >= self.limit {
                return Err(Error::QueueFull);
            }
            match self
                .used
                .compare_exchange_weak(n, n + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(Permit { quota: self }),
                Err(actual) => n = actual,
            }
        }
    }
}
pub(crate) struct Permit<'a> {
    quota: &'a Quota,
}
pub(crate) fn acquire() -> Result<Permit<'static>, Error> {
    QUOTA.acquire()
}
impl Drop for Permit<'_> {
    fn drop(&mut self) {
        self.quota.used.fetch_sub(1, Ordering::AcqRel);
    }
}
impl<'a> Permit<'a> {
    pub(crate) fn own<F: Future>(self, future: F) -> Generation<'a, F> {
        Generation {
            future: Box::pin(future),
            _permit: self,
        }
    }
}
// IMPORTANT: struct fields drop in declaration order. Drop the captured future
// AND its allocation before returning the permit. Also holds before first poll.
// Pin<Box<F>> permits safe projection without unsafe/private runtime APIs.
pub(crate) struct Generation<'a, F> {
    future: Pin<Box<F>>,
    _permit: Permit<'a>,
}
impl<F: Future> Future for Generation<'_, F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    struct Captured<'a>(&'a Quota);
    impl Future for Captured<'_> {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }
    impl Drop for Captured<'_> {
        fn drop(&mut self) {
            assert_eq!(
                self.0.used.load(Ordering::Acquire),
                1,
                "permit returned before captured future destruction"
            );
        }
    }
    #[test]
    fn never_polled_future_drops_before_permit() {
        let q = Quota {
            used: AtomicUsize::new(0),
            limit: 1,
        };
        let wrapped = q.acquire().unwrap().own(Captured(&q));
        assert!(matches!(q.acquire(), Err(Error::QueueFull)));
        drop(wrapped);
        assert_eq!(q.used.load(Ordering::Acquire), 0);
        drop(q.acquire().unwrap());
    }
    #[test]
    fn synchronous_preflight_failure_and_supersede_reservation() {
        let q = Quota {
            used: AtomicUsize::new(0),
            limit: 2,
        };
        let old = q.acquire().unwrap().own(std::future::pending::<()>());
        {
            let _preflight = q.acquire().unwrap();
            assert!(q.acquire().is_err());
        }
        let replacement = q.acquire().unwrap().own(std::future::pending::<()>());
        assert!(q.acquire().is_err());
        drop(old);
        assert_eq!(q.used.load(Ordering::Acquire), 1);
        drop(replacement);
        assert_eq!(q.used.load(Ordering::Acquire), 0);
    }
    #[test]
    fn completed_future_keeps_charge_until_future_storage_drop() {
        let q = Quota {
            used: AtomicUsize::new(0),
            limit: 1,
        };
        let mut wrapped = q.acquire().unwrap().own(std::future::ready(()));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(Pin::new(&mut wrapped).poll(&mut cx).is_ready());
        assert!(q.acquire().is_err());
        drop(wrapped);
        assert_eq!(q.used.load(Ordering::Acquire), 0);
    }
    #[test]
    fn spawn_unwind_drops_captures_before_returning_permit() {
        let q = Quota {
            used: AtomicUsize::new(0),
            limit: 1,
        };
        fn failing_spawn<F>(_future: F) {
            panic!("simulated spawn unwind");
        }
        assert!(std::panic::catch_unwind(|| {
            failing_spawn(q.acquire().unwrap().own(Captured(&q)));
        })
        .is_err());
        assert_eq!(q.used.load(Ordering::Acquire), 0);
    }
}
