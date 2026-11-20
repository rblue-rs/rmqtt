use mqtt_api::Error;
use std::{sync::mpsc, time::Duration};
#[test]
fn background_release_is_safe_on_a_runtime_worker_and_reinitializes() {
    mqtt_runtime::init().unwrap();
    let runtime = mqtt_runtime::acquire().unwrap();
    let (go, ready) = tokio::sync::oneshot::channel();
    let (done, result) = mpsc::channel();
    runtime.spawn(async move {
        ready.await.unwrap();
        assert_eq!(mqtt_runtime::shutdown(), Err(Error::Busy));
        mqtt_runtime::release_background().unwrap();
        done.send(()).unwrap();
    });
    drop(runtime);
    go.send(()).unwrap();
    result.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(matches!(
        mqtt_runtime::acquire(),
        Err(Error::NotInitialized)
    ));
    mqtt_runtime::init().unwrap();
    mqtt_runtime::shutdown().unwrap();
}
