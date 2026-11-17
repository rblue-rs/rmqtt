//! Process-wide runtime. Shutdown refuses active leases and runtime worker threads.
use mqtt_api::Error;
use std::sync::{Arc, Mutex};
use tokio::runtime::{Builder, Runtime};
static STATE: Mutex<(Option<Arc<Runtime>>, usize)> = Mutex::new((None, 0));
pub fn init() -> Result<(), Error> {
    let mut state = STATE.lock().map_err(|_| Error::Internal)?;
    if state.0.is_none() {
        state.0 = Some(Arc::new(
            Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|_| Error::Internal)?,
        ));
    }
    state.1 = state.1.checked_add(1).ok_or(Error::Internal)?;
    Ok(())
}
pub fn shutdown() -> Result<(), Error> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::Busy);
    }
    let runtime = {
        let mut s = STATE.lock().map_err(|_| Error::Internal)?;
        if s.1 == 0 {
            return Ok(());
        }
        if s.1 > 1 {
            s.1 -= 1;
            return Ok(());
        }
        if s.0.as_ref().is_some_and(|r| Arc::strong_count(r) > 1) {
            return Err(Error::Busy);
        }
        s.1 = 0;
        s.0.take()
    };
    if let Some(r) = runtime {
        if let Ok(r) = Arc::try_unwrap(r) {
            r.shutdown_timeout(std::time::Duration::from_secs(2));
        }
    }
    Ok(())
}
pub fn acquire() -> Result<Arc<Runtime>, Error> {
    STATE
        .lock()
        .map_err(|_| Error::Internal)?
        .0
        .clone()
        .ok_or(Error::NotInitialized)
}
