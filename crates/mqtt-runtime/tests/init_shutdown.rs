use mqtt_api::Error;
#[test]
fn reference_count_and_live_client_guard() {
    assert!(mqtt_runtime::acquire().is_err());
    assert_eq!(mqtt_runtime::init(), Ok(()));
    assert_eq!(mqtt_runtime::init(), Ok(()));
    let lease = mqtt_runtime::acquire().unwrap();
    assert_eq!(mqtt_runtime::shutdown(), Ok(()));
    assert_eq!(mqtt_runtime::shutdown(), Err(Error::Busy));
    drop(lease);
    assert_eq!(mqtt_runtime::shutdown(), Ok(()));
    assert_eq!(mqtt_runtime::shutdown(), Ok(()));
    assert!(mqtt_runtime::acquire().is_err());
    mqtt_runtime::init().unwrap();
    mqtt_runtime::shutdown().unwrap();
    let threads: Vec<_> = (0..8)
        .map(|_| {
            std::thread::spawn(|| {
                for _ in 0..20 {
                    mqtt_runtime::init().unwrap();
                    let lease = mqtt_runtime::acquire().unwrap();
                    drop(lease);
                    mqtt_runtime::shutdown().unwrap();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(mqtt_runtime::acquire().is_err());
    mqtt_runtime::init().unwrap();
    let lease = mqtt_runtime::acquire().unwrap();
    let handle = lease.handle().clone();
    assert_eq!(
        handle.block_on(async { mqtt_runtime::shutdown() }),
        Err(Error::Busy)
    );
    drop(lease);
    mqtt_runtime::shutdown().unwrap();
}
