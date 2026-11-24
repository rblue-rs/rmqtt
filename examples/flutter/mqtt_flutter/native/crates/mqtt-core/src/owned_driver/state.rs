//! Owned production QoS2 state. Public packet codecs; no upstream engine state.
use bytes::BytesMut;
use mqtt_api::Error;
use rumqttc::{mqttbytes::v4, v5::mqttbytes::v5};
use std::collections::BTreeMap;
const PACKET: usize = 131080;
const INBOUND_BYTES: usize = 1024 * 1024;
const INBOUND_COUNT: usize = 128;
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Send(Vec<u8>),
    Complete(u16),
    Failed(u16, Error),
    Deliver(u16, Vec<u8>, Vec<u8>),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Rec,
    Comp,
}
struct Outbound {
    id: u16,
    phase: Phase,
    wire: Box<[u8]>,
}
struct Inbound {
    payload: Box<[u8]>,
    topic: Box<[u8]>,
}
pub(crate) struct Machine {
    protocol: u8,
    outbound: Option<Outbound>,
    incoming: BTreeMap<u16, Inbound>,
    last: u16,
}
enum Input {
    Publish(u16, Vec<u8>, Vec<u8>),
    Control(u8, u16, u8),
}
impl Machine {
    pub(crate) fn new(protocol: u8) -> Self {
        Self {
            protocol,
            outbound: None,
            incoming: BTreeMap::new(),
            last: 0,
        }
    }
    pub(crate) fn allocate(&mut self) -> Result<u16, Error> {
        if self.outbound.is_some() {
            return Err(Error::QueueFull);
        }
        self.last = if self.last == u16::MAX {
            1
        } else {
            self.last + 1
        };
        Ok(self.last)
    }
    pub(crate) fn begin(&mut self, id: u16, wire: Vec<u8>) -> Result<(), Error> {
        if self.outbound.is_some() {
            return Err(Error::QueueFull);
        }
        if id == 0 || !matches!(self.decode(&wire)?,Input::Publish(n,_,_) if n==id) {
            return Err(Error::InvalidArgument);
        }
        self.last = id;
        self.outbound = Some(Outbound {
            id,
            phase: Phase::Rec,
            wire: wire.into_boxed_slice(),
        });
        Ok(())
    }
    fn ack(&self, kind: u8, id: u16, reason: u8) -> Vec<u8> {
        let [h, l] = id.to_be_bytes();
        if self.protocol == 5 {
            vec![kind, 4, h, l, reason, 0]
        } else {
            vec![kind, 2, h, l]
        }
    }
    fn decode(&self, wire: &[u8]) -> Result<Input, Error> {
        if wire.len() > PACKET {
            return Err(Error::Overflow);
        }
        if let Some(&b) = wire.first() {
            let k = b >> 4;
            if k != 3 && b & 15 != if k == 6 { 2 } else { 0 } {
                return Err(Error::Network);
            }
        }
        let mut bytes = BytesMut::from(wire);
        let input = if self.protocol == 5 {
            match v5::Packet::read(&mut bytes, Some(PACKET as u32)).map_err(|_| Error::Network)? {
                v5::Packet::Publish(p) if p.qos == rumqttc::v5::mqttbytes::QoS::ExactlyOnce => {
                    Input::Publish(p.pkid, p.topic.to_vec(), p.payload.to_vec())
                }
                v5::Packet::PubRec(p) => Input::Control(
                    0x50,
                    p.pkid,
                    if matches!(
                        p.reason,
                        v5::PubRecReason::Success | v5::PubRecReason::NoMatchingSubscribers
                    ) {
                        0
                    } else {
                        0x80
                    },
                ),
                v5::Packet::PubRel(p) => Input::Control(
                    0x62,
                    p.pkid,
                    if p.reason == v5::PubRelReason::Success {
                        0
                    } else {
                        0x80
                    },
                ),
                v5::Packet::PubComp(p) => Input::Control(
                    0x70,
                    p.pkid,
                    if p.reason == v5::PubCompReason::Success {
                        0
                    } else {
                        0x80
                    },
                ),
                _ => return Err(Error::Unsupported),
            }
        } else if self.protocol == 4 {
            match v4::Packet::read(&mut bytes, PACKET).map_err(|_| Error::Network)? {
                v4::Packet::Publish(p) if p.qos == rumqttc::QoS::ExactlyOnce => {
                    Input::Publish(p.pkid, p.topic.into_bytes(), p.payload.to_vec())
                }
                v4::Packet::PubRec(p) => Input::Control(0x50, p.pkid, 0),
                v4::Packet::PubRel(p) => Input::Control(0x62, p.pkid, 0),
                v4::Packet::PubComp(p) => Input::Control(0x70, p.pkid, 0),
                _ => return Err(Error::Unsupported),
            }
        } else {
            return Err(Error::Unsupported);
        };
        if !bytes.is_empty() {
            return Err(Error::Network);
        }
        Ok(input)
    }
    pub(crate) fn receive(&mut self, wire: &[u8]) -> Result<Vec<Action>, Error> {
        match self.decode(wire)? {
            Input::Publish(id, topic, payload) => {
                if id == 0 {
                    return Err(Error::Network);
                }
                if topic.len() > 65535 || payload.len() > 65536 {
                    return Err(Error::Overflow);
                }
                if let Some(old) = self.incoming.get(&id) {
                    if old.payload.as_ref() != payload || old.topic.as_ref() != topic {
                        return Err(Error::Network);
                    }
                } else {
                    let used: usize = self
                        .incoming
                        .values()
                        .map(|p| p.topic.len() + p.payload.len())
                        .sum();
                    if self.incoming.len() >= INBOUND_COUNT
                        || topic.len() + payload.len() > INBOUND_BYTES - used
                    {
                        return Err(Error::Overflow);
                    }
                    self.incoming.insert(
                        id,
                        Inbound {
                            payload: payload.into_boxed_slice(),
                            topic: topic.into_boxed_slice(),
                        },
                    );
                }
                Ok(vec![Action::Send(self.ack(0x50, id, 0))])
            }
            Input::Control(0x62, id, reason) => {
                if id == 0 {
                    return Err(Error::Network);
                }
                let old = self.incoming.remove(&id);
                let mut actions = Vec::new();
                let known = old.is_some();
                if let Some(old) = old {
                    if reason < 0x80 {
                        actions.push(Action::Deliver(
                            id,
                            old.topic.into_vec(),
                            old.payload.into_vec(),
                        ));
                    }
                }
                actions.push(Action::Send(self.ack(
                    0x70,
                    id,
                    if !known && self.protocol == 5 {
                        0x92
                    } else {
                        0
                    },
                )));
                Ok(actions)
            }
            Input::Control(kind, id, reason) => {
                if id == 0 {
                    return Err(Error::Network);
                }
                let Some(out) = self.outbound.as_mut().filter(|p| p.id == id) else {
                    return Ok(if kind == 0x50 && reason < 0x80 {
                        vec![Action::Send(self.ack(
                            0x62,
                            id,
                            if self.protocol == 5 { 0x92 } else { 0 },
                        ))]
                    } else {
                        vec![]
                    });
                };
                if kind == 0x50 {
                    if reason >= 0x80 {
                        self.outbound = None;
                        return Ok(vec![Action::Failed(id, Error::Refused)]);
                    }
                    // A successful duplicate repeats PUBREL; any negative PUBREC is terminal.
                    out.phase = Phase::Comp;
                    Ok(vec![Action::Send(self.ack(0x62, id, 0))])
                } else if kind == 0x70 && out.phase == Phase::Comp {
                    self.outbound = None;
                    Ok(vec![if reason >= 0x80 {
                        Action::Failed(id, Error::Refused)
                    } else {
                        Action::Complete(id)
                    }])
                } else {
                    Ok(vec![])
                }
            }
        }
    }
    pub(crate) fn resume(&mut self, present: bool, clean: bool) -> Vec<Action> {
        if !present || clean || self.protocol == 5 {
            self.incoming.clear();
            return self
                .outbound
                .take()
                .map(|p| vec![Action::Failed(p.id, Error::Network)])
                .unwrap_or_default();
        }
        self.outbound
            .as_ref()
            .map(|p| {
                vec![Action::Send(if p.phase == Phase::Comp {
                    self.ack(0x62, p.id, 0)
                } else {
                    let mut wire = p.wire.to_vec();
                    wire[0] |= 8;
                    wire
                })]
            })
            .unwrap_or_default()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn ack(p: u8, kind: u8, id: u16, reason: u8) -> Vec<u8> {
        let [hi, lo] = id.to_be_bytes();
        if p == 5 {
            vec![kind, 4, hi, lo, reason, 0]
        } else {
            vec![kind, 2, hi, lo]
        }
    }
    fn publish(p: u8, id: u16) -> Vec<u8> {
        let [h, l] = id.to_be_bytes();
        if p == 5 {
            vec![0x34, 7, 0, 1, b'q', h, l, 0, 255]
        } else {
            vec![0x34, 6, 0, 1, b'q', h, l, 255]
        }
    }
    #[test]
    fn outbound_only_comp_and_duplicate_rec_same_id() {
        for p in [4, 5] {
            let mut m = Machine::new(p);
            m.begin(17, publish(p, 17)).unwrap();
            let rel = ack(p, 0x62, 17, 0);
            assert_eq!(
                m.receive(&ack(p, 0x50, 17, 0)),
                Ok(vec![Action::Send(rel.clone())])
            );
            assert_eq!(m.receive(&ack(p, 0x50, 17, 0)), Ok(vec![Action::Send(rel)]));
            assert_eq!(m.receive(&ack(p, 0x70, 18, 0)), Ok(vec![]));
            assert_eq!(
                m.receive(&ack(p, 0x70, 17, 0)),
                Ok(vec![Action::Complete(17)])
            );
            assert_eq!(m.receive(&ack(p, 0x70, 17, 0)), Ok(vec![]));
        }
    }
    #[test]
    fn inbound_duplicate_publish_and_release_deliver_once() {
        for p in [4, 5] {
            let mut m = Machine::new(p);
            let data = publish(p, 91);
            let rec = ack(p, 0x50, 91, 0);
            assert_eq!(m.receive(&data), Ok(vec![Action::Send(rec.clone())]));
            let mut dup = data;
            dup[0] |= 8;
            assert_eq!(m.receive(&dup), Ok(vec![Action::Send(rec)]));
            assert_eq!(
                m.receive(&ack(p, 0x62, 91, 0)),
                Ok(vec![
                    Action::Deliver(91, vec![b'q'], vec![255]),
                    Action::Send(ack(p, 0x70, 91, 0))
                ])
            );
            assert_eq!(
                m.receive(&ack(p, 0x62, 91, 0)),
                Ok(vec![Action::Send(ack(
                    p,
                    0x70,
                    91,
                    if p == 5 { 0x92 } else { 0 }
                ))])
            );
        }
    }
    #[test]
    fn negative_rec_and_comp_release_original_ticket() {
        for kind in [0x50, 0x70] {
            let mut m = Machine::new(5);
            m.begin(4, publish(5, 4)).unwrap();
            if kind == 0x70 {
                m.receive(&ack(5, 0x50, 4, 0)).unwrap();
            }
            assert_eq!(
                m.receive(&ack(5, kind, 4, if kind == 0x50 { 0x97 } else { 0x92 })),
                Ok(vec![Action::Failed(4, Error::Refused)])
            );
            m.begin(5, publish(5, 5)).unwrap();
        }
    }
    #[test]
    fn session_resume_replays_the_owned_phase_not_new_id() {
        for phase in [false, true] {
            let mut m = Machine::new(4);
            let mut data = publish(4, 7);
            m.begin(7, data.clone()).unwrap();
            if phase {
                m.receive(&ack(4, 0x50, 7, 0)).unwrap();
            }
            data[0] |= 8;
            assert_eq!(
                m.resume(true, false),
                vec![Action::Send(if phase { ack(4, 0x62, 7, 0) } else { data })]
            );
            assert_eq!(
                m.resume(false, false),
                vec![Action::Failed(7, Error::Network)]
            );
        }
    }

    #[test]
    fn allocator_never_reallocates_owned_id_and_wrong_phase_comp_ignored() {
        let mut m = Machine::new(4);
        assert_eq!(m.allocate(), Ok(1));
        m.begin(1, publish(4, 1)).unwrap();
        assert_eq!(m.allocate(), Err(Error::QueueFull));
        assert_eq!(m.receive(&ack(4, 0x70, 1, 0)), Ok(vec![]));
        m.receive(&ack(4, 0x50, 1, 0)).unwrap();
        m.receive(&ack(4, 0x70, 1, 0)).unwrap();
        assert_eq!(m.allocate(), Ok(2));
        m.last = u16::MAX;
        assert_eq!(m.allocate(), Ok(1));
    }
    #[test]
    fn bounded_inbound_releases_quota_and_new_session_clears_dedup() {
        for p in [4, 5] {
            let mut m = Machine::new(p);
            for id in 1..=128 {
                m.receive(&publish(p, id)).unwrap();
            }
            assert_eq!(m.receive(&publish(p, 129)), Err(Error::Overflow));
            m.receive(&ack(p, 0x62, 1, 0)).unwrap();
            m.receive(&publish(p, 129)).unwrap();
            assert!(m.resume(false, false).is_empty());
            assert!(m.incoming.is_empty());
            assert_eq!(
                m.receive(&ack(p, 0x62, 2, 0)),
                Ok(vec![Action::Send(ack(
                    p,
                    0x70,
                    2,
                    if p == 5 { 0x92 } else { 0 }
                ))])
            );
        }
    }
    #[test]
    fn oversize_wire_rejected_before_decode_and_busy_keeps_original() {
        let mut m = Machine::new(4);
        assert_eq!(m.receive(&vec![0; PACKET + 1]), Err(Error::Overflow));
        m.begin(9, publish(4, 9)).unwrap();
        assert_eq!(m.begin(10, publish(4, 10)), Err(Error::QueueFull));
        assert!(matches!(m.resume(true,false).as_slice(),[Action::Send(w)] if w[5..7]==[0,9]));
    }

    #[test]
    fn unknown_pubrec_releases_peer_without_touching_owned_ticket() {
        for p in [4, 5] {
            let mut m = Machine::new(p);
            m.begin(7, publish(p, 7)).unwrap();
            let a = m.receive(&[0x50, 2, 0, 99]).unwrap();
            assert_eq!(
                a,
                vec![Action::Send(if p == 5 {
                    vec![0x62, 4, 0, 99, 0x92, 0]
                } else {
                    vec![0x62, 2, 0, 99]
                })]
            );
            assert_eq!(m.allocate(), Err(Error::QueueFull));
            assert!(m.receive(&[0x70, 2, 0, 99]).unwrap().is_empty());
            assert_eq!(m.outbound.as_ref().unwrap().id, 7);
        }
    }
    #[test]
    fn negative_duplicate_pubrec_terminates_without_success_or_replay() {
        let mut m = Machine::new(5);
        m.begin(7, publish(5, 7)).unwrap();
        m.receive(&[0x50, 2, 0, 7]).unwrap();
        assert_eq!(
            m.receive(&[0x50, 4, 0, 7, 0x97, 0]).unwrap(),
            vec![Action::Failed(7, Error::Refused)]
        );
        assert!(m.receive(&[0x70, 2, 0, 7]).unwrap().is_empty());
        assert!(m.resume(true, false).is_empty());
        assert!(m.allocate().is_ok());
    }

    #[test]
    fn incoming_byte_budget_is_exact_and_pubrel_releases_it() {
        for p in [4, 5] {
            let large = |id: u16| {
                let [h, l] = id.to_be_bytes();
                let mut body = vec![0, 1, b't', h, l];
                if p == 5 {
                    body.push(0)
                }
                body.resize(body.len() + 65535, 7);
                let mut wire = vec![0x34];
                let mut n = body.len();
                loop {
                    let mut b = (n % 128) as u8;
                    n /= 128;
                    if n != 0 {
                        b |= 128
                    }
                    wire.push(b);
                    if n == 0 {
                        break;
                    }
                }
                wire.extend(body);
                wire
            };
            let mut m = Machine::new(p);
            for id in 1..=16 {
                m.receive(&large(id)).unwrap();
            }
            assert_eq!(
                m.incoming
                    .values()
                    .map(|v| v.topic.len() + v.payload.len())
                    .sum::<usize>(),
                INBOUND_BYTES
            );
            assert_eq!(m.receive(&publish(p, 17)), Err(Error::Overflow));
            assert_eq!(m.incoming.len(), 16);
            assert!(matches!(
                m.receive(&[0x62, 2, 0, 1]).unwrap().first(),
                Some(Action::Deliver(1, _, _))
            ));
            m.receive(&publish(p, 17)).unwrap();
        }
    }
    #[test]
    fn zero_control_ids_and_conflicting_publish_do_not_destroy_owned_state() {
        for p in [4, 5] {
            let mut m = Machine::new(p);
            m.begin(7, publish(p, 7)).unwrap();
            for kind in [0x50, 0x62, 0x70] {
                assert_eq!(m.receive(&[kind, 2, 0, 0]), Err(Error::Network));
                assert_eq!(m.outbound.as_ref().unwrap().id, 7);
            }
            m.receive(&publish(p, 3)).unwrap();
            let mut conflict = publish(p, 3);
            *conflict.last_mut().unwrap() ^= 1;
            assert_eq!(m.receive(&conflict), Err(Error::Network));
            assert_eq!(m.incoming.len(), 1);
            assert!(matches!(
                m.receive(&[0x62, 2, 0, 3]).unwrap().first(),
                Some(Action::Deliver(3, _, _))
            ));
        }
    }

    #[test]
    fn invalid_reserved_control_flags_are_rejected_without_state_change() {
        for p in [4, 5] {
            let mut m = Machine::new(p);
            m.begin(7, publish(p, 7)).unwrap();
            for kind in [0x51, 0x60, 0x71] {
                assert_eq!(m.receive(&[kind, 2, 0, 7]), Err(Error::Network));
                assert!(m.outbound.is_some());
            }
        }
    }
}
