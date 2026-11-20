mod common;
use common::{next, packet};
use mqtt_api::{Error, Options};
use mqtt_core::{Client, Kind};
use std::{
    io::Write,
    net::TcpListener,
    time::{Duration, Instant},
};
#[test]
fn refused_connack_has_no_automatic_retry() {
    mqtt_runtime::init().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        packet(&mut socket);
        socket.write_all(&[0x20, 2, 0, 3]).unwrap();
        drop(socket);
        listener.set_nonblocking(true).unwrap();
        let until = Instant::now() + Duration::from_millis(200);
        while Instant::now() < until {
            assert!(listener.accept().is_err(), "first failure retried");
            std::thread::sleep(Duration::from_millis(5));
        }
    });
    let mut client = Client::new().unwrap();
    client
        .connect(1, Options::new("127.0.0.1", "refused", port))
        .unwrap();
    let failure = next(&client);
    assert_eq!(failure.kind, Kind::Failure);
    assert_eq!(failure.code, Error::Refused as i32);
    server.join().unwrap();
    assert_eq!(client.pop().unwrap_err(), Error::Empty);
    drop(client);
    mqtt_runtime::shutdown().unwrap();
}
