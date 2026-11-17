use mqtt_api::Error;
use mqtt_core::Client;
use std::{
    io::Read,
    net::TcpStream,
    time::{Duration, Instant},
};
pub fn packet(s: &mut TcpStream) -> Vec<u8> {
    let mut b = [0];
    s.read_exact(&mut b).unwrap();
    let h = b[0];
    let mut len = 0;
    let mut shift = 0;
    loop {
        s.read_exact(&mut b).unwrap();
        len += (b[0] as usize & 127) << shift;
        if b[0] & 128 == 0 {
            break;
        }
        shift += 7;
    }
    let mut v = vec![0; len];
    s.read_exact(&mut v).unwrap();
    let mut out = vec![h];
    out.extend(v);
    out
}
pub fn next(c: &Client) -> mqtt_core::Event {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        match c.pop() {
            Ok(e) => return e,
            Err(Error::Empty) => {
                assert!(Instant::now() < end, "event timeout");
                std::thread::sleep(Duration::from_millis(5));
            }
            e => panic!("{e:?}"),
        }
    }
}
