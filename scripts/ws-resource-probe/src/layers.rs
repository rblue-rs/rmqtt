//! Same async WS-to-byte bridge as rumqttc, isolated instrumentation only.
use super::*;
use futures::{
    io::{AsyncRead, AsyncWrite},
    FutureExt,
};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
impl AsyncRead for Wire {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        b: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match Read::read(&mut *self, b) {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Poll::Pending,
            v => Poll::Ready(v),
        }
    }
}
impl AsyncWrite for Wire {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Pending
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}
pub fn run() -> ExitCode {
    let bounded = std::env::args().any(|a| a == "--bounded");
    println!("async_bridge_fixture bounded_counterfactual={bounded}; NOT production memory cap");
    let w = futures::task::noop_waker();
    let mut cx = Context::from_waker(&w);
    // Warm per-thread masking RNG and notifier paths before attributing object
    // lifetime allocations. This does not subtract any connection-owned buffers.
    {
        let socket = async_tungstenite::WebSocketStream::from_raw_socket(
            Wire::new(Case::Pings(2)),
            Role::Client,
            None,
        )
        .now_or_never()
        .unwrap();
        let mut ws = ws_stream_tungstenite::WsStream::new(socket);
        let mut byte = [0];
        let _ = AsyncRead::poll_read(Pin::new(&mut ws), &mut cx, &mut byte);
    }
    let mut pass = 0;
    for (case, name) in [
        (Case::Frame, "unconsumed_3MiB_after_one_byte"),
        (Case::TinyMessages, "65536_one_byte_messages"),
        (Case::Pings(64), "64_controls_blocked_writer"),
    ] {
        let baseline = LIVE.load(Ordering::SeqCst);
        PEAK.store(baseline, Ordering::SeqCst);
        MAX_REQUEST.store(0, Ordering::SeqCst);
        TRACK.store(true, Ordering::SeqCst);
        let socket = async_tungstenite::WebSocketStream::from_raw_socket(
            Wire::new(case),
            Role::Client,
            Some(config(bounded, false)),
        )
        .now_or_never()
        .unwrap();
        let mut ws = ws_stream_tungstenite::WsStream::new(socket);
        let mut byte = [0];
        let mut reads = 0;
        let mut error = false;
        let mut eof = false;
        let mut pending = false;
        let n = if matches!(case, Case::TinyMessages) {
            65536
        } else {
            1
        };
        for _ in 0..n {
            match AsyncRead::poll_read(Pin::new(&mut ws), &mut cx, &mut byte) {
                Poll::Ready(Ok(1)) => reads += 1,
                Poll::Ready(Err(_)) => {
                    error = true;
                    break;
                }
                Poll::Ready(Ok(0)) => {
                    eof = true;
                    break;
                }
                Poll::Pending => {
                    pending = true;
                    break;
                }
                _ => break,
            }
        }
        let retained_before_drop = LIVE.load(Ordering::SeqCst).saturating_sub(baseline);
        drop(ws);
        TRACK.store(false, Ordering::SeqCst);
        let peak = PEAK.load(Ordering::SeqCst) - baseline;
        let released = LIVE.load(Ordering::SeqCst) == baseline;
        let ok = released
            && peak <= BUDGET
            && if matches!(case, Case::Frame) && bounded {
                (error || eof) && !pending
            } else {
                reads == n
            };
        if ok {
            pass += 1;
        }
        println!("{name} peak_requested_live={peak} retained_after_read_before_drop={retained_before_drop} max_allocation_request={} released={released} reads={reads} error={error} eof={eof} pending={pending} pass={ok}",MAX_REQUEST.load(Ordering::SeqCst));
    }
    println!("async bridge fixture {pass}/3; selected schedules only");
    if pass == 3 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
