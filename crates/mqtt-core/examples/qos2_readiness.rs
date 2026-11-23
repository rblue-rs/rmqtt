//! Explicit, independent release gate; NOT a product QoS2 implementation.
//! Run: cargo run -p mqtt-core --example qos2_readiness --locked
//! Nonzero means QoS2 MUST remain Unsupported. These assertions are not weakened
//! to bless current upstream behavior. Inputs/expected packets are fixed fixtures.
use rumqttc::{v5, Event, MqttState, Packet, PubComp, PubRec, PubRel, Publish, QoS, Request};
use std::process::ExitCode;
use v5::mqttbytes::{v5 as p5, QoS as Q5};

#[derive(Clone, Copy)]
enum Input {
    Send,
    Publish(bool),
    AckPublish,
    Rec(bool),
    Rel,
    Comp(bool),
}
#[derive(Debug, PartialEq)]
struct Wire(&'static str, u16, bool);
fn w4(p: Option<Packet>) -> Result<Wire, String> {
    match p {
        Some(Packet::Publish(p)) => Ok(Wire("PUBLISH", p.pkid, p.dup)),
        Some(Packet::PubRel(p)) => Ok(Wire("PUBREL", p.pkid, false)),
        Some(Packet::PubComp(p)) => Ok(Wire("PUBCOMP", p.pkid, false)),
        Some(Packet::PubRec(p)) => Ok(Wire("PUBREC", p.pkid, false)),
        None => Ok(Wire("none", 0, false)),
        p => Err(format!("unexpected packet {p:?}")),
    }
}
fn w5(p: Option<p5::Packet>) -> Result<Wire, String> {
    match p {
        Some(p5::Packet::Publish(p)) => Ok(Wire("PUBLISH", p.pkid, p.dup)),
        Some(p5::Packet::PubRel(p)) => Ok(Wire("PUBREL", p.pkid, false)),
        Some(p5::Packet::PubComp(p)) => Ok(Wire("PUBCOMP", p.pkid, false)),
        Some(p5::Packet::PubRec(p)) => Ok(Wire("PUBREC", p.pkid, false)),
        None => Ok(Wire("none", 0, false)),
        p => Err(format!("unexpected packet {p:?}")),
    }
}
enum Machine {
    V4(MqttState),
    V5(v5::MqttState),
}
impl Machine {
    fn new(v5: bool) -> Self {
        // max inflight1 permits deterministic id wrap AFTER terminal completion.
        if v5 {
            Self::V5(v5::MqttState::new(1, true))
        } else {
            Self::V4(MqttState::new(1, true))
        }
    }
    fn step(&mut self, i: Input) -> Result<Wire, String> {
        match self {
            Self::V4(s) => {
                let result = match i {
                    Input::Send => s.handle_outgoing_packet(Request::Publish(Publish::new(
                        "q2",
                        QoS::ExactlyOnce,
                        vec![0, 255, 128],
                    ))),
                    Input::Publish(dup) => {
                        let mut p = Publish::new("q2", QoS::ExactlyOnce, vec![0, 255, 128]);
                        p.pkid = 1;
                        p.dup = dup;
                        s.handle_incoming_packet(Packet::Publish(p))
                    }
                    Input::AckPublish => s.handle_outgoing_packet(Request::PubRec(PubRec::new(1))),
                    Input::Rec(_) => s.handle_incoming_packet(Packet::PubRec(PubRec::new(1))),
                    Input::Rel => s.handle_incoming_packet(Packet::PubRel(PubRel::new(1))),
                    Input::Comp(_) => s.handle_incoming_packet(Packet::PubComp(PubComp::new(1))),
                };
                w4(result.map_err(|e| format!("{e:?}"))?)
            }
            Self::V5(s) => {
                let result = match i {
                    Input::Send => s.handle_outgoing_packet(v5::Request::Publish(
                        p5::Publish::new("q2", Q5::ExactlyOnce, vec![0, 255, 128], None),
                    )),
                    Input::Publish(dup) => {
                        let mut p =
                            p5::Publish::new("q2", Q5::ExactlyOnce, vec![0, 255, 128], None);
                        p.pkid = 1;
                        p.dup = dup;
                        s.handle_incoming_packet(p5::Packet::Publish(p))
                    }
                    Input::AckPublish => {
                        s.handle_outgoing_packet(v5::Request::PubRec(p5::PubRec::new(1, None)))
                    }
                    Input::Rec(negative) => {
                        let mut p = p5::PubRec::new(1, None);
                        if negative {
                            p.reason = p5::PubRecReason::NotAuthorized;
                        }
                        s.handle_incoming_packet(p5::Packet::PubRec(p))
                    }
                    Input::Rel => {
                        s.handle_incoming_packet(p5::Packet::PubRel(p5::PubRel::new(1, None)))
                    }
                    Input::Comp(negative) => {
                        let mut p = p5::PubComp::new(1, None);
                        if negative {
                            p.reason = p5::PubCompReason::PacketIdentifierNotFound;
                        }
                        s.handle_incoming_packet(p5::Packet::PubComp(p))
                    }
                };
                w5(result.map_err(|e| format!("{e:?}"))?)
            }
        }
    }
    fn inflight(&self) -> u16 {
        match self {
            Self::V4(s) => s.inflight(),
            Self::V5(s) => s.inflight(),
        }
    }
    fn deliveries(&self) -> usize {
        match self {
            Self::V4(s) => s
                .events
                .iter()
                .filter(|e| matches!(e, Event::Incoming(Packet::Publish(_))))
                .count(),
            Self::V5(s) => s
                .events
                .iter()
                .filter(|e| matches!(e, v5::Event::Incoming(p5::Packet::Publish(_))))
                .count(),
        }
    }
    fn clean(&mut self) {
        match self {
            Self::V4(s) => {
                s.clean();
            }
            Self::V5(s) => {
                s.clean();
            }
        }
    }
    fn replay(&mut self) -> Result<Wire, String> {
        match self {
            Self::V4(s) => {
                let mut pending = s.clean();
                check(pending.len() == 1, "expected exactly one pending request")?;
                w4(s.handle_outgoing_packet(pending.remove(0))
                    .map_err(|e| format!("{e:?}"))?)
            }
            Self::V5(s) => {
                let mut pending = s.clean();
                check(pending.len() == 1, "expected exactly one pending request")?;
                w5(s.handle_outgoing_packet(pending.remove(0))
                    .map_err(|e| format!("{e:?}"))?)
            }
        }
    }
}
fn check(ok: bool, message: &str) -> Result<(), String> {
    if ok {
        Ok(())
    } else {
        Err(message.into())
    }
}
fn want(actual: Wire, expected: Wire) -> Result<(), String> {
    check(
        actual == expected,
        &format!("expected {expected:?}, actual {actual:?}"),
    )
}
fn run(v5: bool, case: &str) -> Result<(), String> {
    let mut s = Machine::new(v5);
    match case {
        "pubcomp_is_terminal_and_id_reusable" => {
            want(s.step(Input::Send)?, Wire("PUBLISH", 1, false))?;
            want(s.step(Input::Rec(false))?, Wire("PUBREL", 1, false))?;
            check(
                s.inflight() == 1,
                "PUBREC must not complete/release publish",
            )?;
            want(s.step(Input::Comp(false))?, Wire("none", 0, false))?;
            check(s.inflight() == 0, "PUBCOMP must release publish")?;
            want(s.step(Input::Send)?, Wire("PUBLISH", 1, false))
        }
        "duplicate_pubrec_resends_pubrel" => {
            s.step(Input::Send)?;
            s.step(Input::Rec(false))?;
            want(s.step(Input::Rec(false))?, Wire("PUBREL", 1, false))
        }
        "duplicate_pubrel_resends_pubcomp" => {
            s.step(Input::Publish(false))?;
            s.step(Input::AckPublish)?;
            want(s.step(Input::Rel)?, Wire("PUBCOMP", 1, false))?;
            want(s.step(Input::Rel)?, Wire("PUBCOMP", 1, false))
        }
        "duplicate_publish_has_one_delivery_notification" => {
            s.step(Input::Publish(false))?;
            s.step(Input::AckPublish)?;
            s.step(Input::Publish(true))?;
            check(
                s.deliveries() == 1,
                "two delivery notifications: adapter needs bounded duplicate suppression",
            )
        }
        "resumed_publish_retains_id_and_sets_dup" => {
            s.step(Input::Send)?;
            want(s.replay()?, Wire("PUBLISH", 1, true))
        }
        "resumed_pubrel_retains_id" => {
            s.step(Input::Send)?;
            s.step(Input::Rec(false))?;
            want(s.replay()?, Wire("PUBREL", 1, false))?;
            s.step(Input::Comp(false))?;
            check(s.inflight() == 0, "replayed PUBREL must complete")
        }
        "resumed_inbound_pubrel_after_clean" => {
            s.step(Input::Publish(false))?;
            s.step(Input::AckPublish)?;
            s.clean();
            want(s.step(Input::Rel)?, Wire("PUBCOMP", 1, false))
        }
        "late_pubcomp_does_not_release_new_publish" => {
            s.step(Input::Send)?;
            s.step(Input::Rec(false))?;
            s.step(Input::Comp(false))?;
            s.step(Input::Send)?;
            // Either safely ignore or reject the late ACK; never release new work.
            if let Ok(packet) = s.step(Input::Comp(false)) {
                want(packet, Wire("none", 0, false))?;
            }
            check(
                s.inflight() == 1,
                "late PUBCOMP released unrelated new PUBLISH",
            )
        }
        "negative_pubrec_releases_capacity" => {
            s.step(Input::Send)?;
            want(s.step(Input::Rec(true))?, Wire("none", 0, false))?;
            check(
                s.inflight() == 0,
                "negative PUBREC left inflight capacity owned",
            )
        }
        "negative_pubcomp_releases_capacity" => {
            s.step(Input::Send)?;
            s.step(Input::Rec(false))?;
            s.step(Input::Comp(true))?;
            check(
                s.inflight() == 0,
                "negative PUBCOMP left inflight capacity owned",
            )
        }
        _ => Err("unknown fixture".into()),
    }
}
fn main() -> ExitCode {
    let mut passed = 0;
    let mut failed = 0;
    for v5 in [false, true] {
        let cases = [
            "pubcomp_is_terminal_and_id_reusable",
            "duplicate_pubrec_resends_pubrel",
            "duplicate_pubrel_resends_pubcomp",
            "duplicate_publish_has_one_delivery_notification",
            "resumed_publish_retains_id_and_sets_dup",
            "resumed_pubrel_retains_id",
            "resumed_inbound_pubrel_after_clean",
            "late_pubcomp_does_not_release_new_publish",
            "negative_pubrec_releases_capacity",
            "negative_pubcomp_releases_capacity",
        ];
        for case in cases {
            if !v5 && case.starts_with("negative") {
                continue;
            }
            match run(v5, case) {
                Ok(()) => {
                    passed += 1;
                    println!("PASS protocol={} {case}", if v5 { 5 } else { 4 });
                }
                Err(e) => {
                    failed += 1;
                    println!("FAIL protocol={} {case}: {e}", if v5 { 5 } else { 4 });
                }
            }
        }
    }
    println!("readiness: {passed} passed, {failed} failed; these are upstream/adapter prerequisites, not product support");
    if failed == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
