//! Public packet codecs only; no upstream event loop or hidden state.
use crate::{engine::Request, MAX_WIRE_PACKET};
use bytes::BytesMut;
use mqtt_api::{Error, Options};
use rumqttc::{
    mqttbytes::v4 as p4,
    v5::mqttbytes::{v5 as p5, QoS as Q5},
    Packet, QoS,
};
#[derive(Clone, Copy)]
pub(crate) struct Limits {
    pub packet: usize,
    pub qos: u8,
    pub retain: bool,
    pub wildcard: bool,
    pub shared: bool,
    pub keep_alive: Option<u16>,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            packet: MAX_WIRE_PACKET,
            qos: 2,
            retain: true,
            wildcard: true,
            shared: true,
            keep_alive: None,
        }
    }
}
fn q5(q: QoS) -> Q5 {
    match q {
        QoS::AtMostOnce => Q5::AtMostOnce,
        QoS::AtLeastOnce => Q5::AtLeastOnce,
        QoS::ExactlyOnce => Q5::ExactlyOnce,
    }
}
pub(crate) fn connect(o: &Options) -> Result<Vec<u8>, Error> {
    let mut b = BytesMut::new();
    if o.protocol == 5 {
        let mut properties = p5::ConnectProperties::new();
        properties.max_packet_size = Some(MAX_WIRE_PACKET as u32);
        properties.receive_maximum = Some(128);
        let c = p5::Connect {
            keep_alive: o.keep_alive.as_secs() as u16,
            client_id: o.client_id.clone(),
            clean_start: true,
            properties: Some(properties),
        };
        let w = o.will.as_ref().map(|w| {
            p5::LastWill::new(
                &w.topic,
                w.payload.clone(),
                q5(crate::qos_value(w.qos).expect("validated will")),
                w.retain,
                None,
            )
        });
        let login = o
            .username
            .as_ref()
            .map(|u| p5::Login::new(u, o.password.as_deref().unwrap_or("")));
        p5::Packet::Connect(c, w, login)
            .write(&mut b, None)
            .map_err(|_| Error::InvalidArgument)?;
    } else {
        let mut c = p4::Connect::new(&o.client_id);
        c.keep_alive = o.keep_alive.as_secs() as u16;
        c.clean_session = o.clean_session;
        if let Some(u) = &o.username {
            c.set_login(u, o.password.as_deref().unwrap_or(""));
        }
        c.last_will = o.will.as_ref().map(|w| {
            p4::LastWill::new(
                &w.topic,
                w.payload.clone(),
                crate::qos_value(w.qos).expect("validated will"),
                w.retain,
            )
        });
        c.write(&mut b).map_err(|_| Error::InvalidArgument)?;
    }
    if b.len() > super::owned_driver::transport::SEND_LIMIT {
        return Err(Error::Overflow);
    }
    Ok(b.to_vec())
}
pub(crate) fn encode(proto: u8, r: &Request, id: u16) -> Result<Vec<u8>, Error> {
    let mut b = BytesMut::new();
    match r {
        Request::Publish(t, q, retain, p) => {
            if proto == 5 {
                let mut m = p5::Publish::new(t, q5(*q), p.clone(), None);
                m.pkid = id;
                m.retain = *retain;
                m.write(&mut b).map_err(|_| Error::Network)?;
            } else {
                let mut m = p4::Publish::new(t, *q, p.clone());
                m.pkid = id;
                m.retain = *retain;
                m.write(&mut b).map_err(|_| Error::Network)?;
            }
        }
        Request::Subscribe(t, q) => {
            if proto == 5 {
                let mut m = p5::Subscribe::new(p5::Filter::new(t, q5(*q)), None);
                m.pkid = id;
                m.write(&mut b).map_err(|_| Error::Network)?;
            } else {
                let mut m = p4::Subscribe::new(t, *q);
                m.pkid = id;
                m.write(&mut b).map_err(|_| Error::Network)?;
            }
        }
        Request::Unsubscribe(t) => {
            if proto == 5 {
                let mut m = p5::Unsubscribe::new(t, None);
                m.pkid = id;
                m.write(&mut b).map_err(|_| Error::Network)?;
            } else {
                let mut m = p4::Unsubscribe::new(t);
                m.pkid = id;
                m.write(&mut b).map_err(|_| Error::Network)?;
            }
        }
        Request::Ack(id) => return Ok(ack(proto, 0x40, *id, 0)),
        Request::Disconnect => return Ok(vec![0xe0, 0]),
    }
    if b.len() > MAX_WIRE_PACKET {
        return Err(Error::Overflow);
    }
    Ok(b.to_vec())
}
pub(crate) fn ack(proto: u8, kind: u8, id: u16, reason: u8) -> Vec<u8> {
    let [h, l] = id.to_be_bytes();
    if proto == 5 && !(kind == 0x40 && reason == 0) {
        // Preserve the previous minimal successful QoS1 PUBACK wire form.
        // MQTT5 permits omitted success reason/properties; QoS2 controls are
        // encoded independently by Machine with explicit reason properties.
        vec![kind, 4, h, l, reason, 0]
    } else {
        vec![kind, 2, h, l]
    }
}
pub(crate) fn decode(proto: u8, wire: &[u8]) -> Result<(Packet, Option<Limits>), Error> {
    if wire.len() > MAX_WIRE_PACKET {
        return Err(Error::Overflow);
    }
    let Some(&first) = wire.first() else {
        return Err(Error::Network);
    };
    let kind = first >> 4;
    if (kind != 3 && first & 15 != if kind == 6 { 2 } else { 0 })
        || (kind == 3 && ((first >> 1) & 3 == 3 || (first & 6 == 0 && first & 8 != 0)))
    {
        return Err(Error::Network);
    }
    let mut b = BytesMut::from(wire);
    let mut limits = None;
    let p = if proto == 4 {
        let p = p4::Packet::read(&mut b, MAX_WIRE_PACKET).map_err(|_| Error::Network)?;
        if let Packet::ConnAck(a) = &p {
            match a.code {
                rumqttc::ConnectReturnCode::Success => {}
                rumqttc::ConnectReturnCode::BadUserNamePassword
                | rumqttc::ConnectReturnCode::NotAuthorized => return Err(Error::BadCredentials),
                _ => return Err(Error::Refused),
            }
        }
        p
    } else {
        match p5::Packet::read(&mut b, Some(MAX_WIRE_PACKET as u32)).map_err(|_| Error::Network)? {
            p5::Packet::ConnAck(a) => {
                match a.code {
                    p5::ConnectReturnCode::Success => {}
                    p5::ConnectReturnCode::BadUserNamePassword
                    | p5::ConnectReturnCode::NotAuthorized => return Err(Error::BadCredentials),
                    _ => return Err(Error::Refused),
                }
                let mut l = Limits::default();
                if let Some(p) = a.properties {
                    if p.authentication_method.is_some()
                        || p.authentication_data.is_some()
                        || p.session_expiry_interval.is_some_and(|v| v != 0)
                    {
                        return Err(Error::Unsupported);
                    }
                    if p.receive_max == Some(0)
                        || p.max_packet_size == Some(0)
                        || p.max_qos.is_some_and(|q| q > 1)
                        || p.retain_available.is_some_and(|v| v > 1)
                        || p.wildcard_subscription_available.is_some_and(|v| v > 1)
                        || p.shared_subscription_available.is_some_and(|v| v > 1)
                    {
                        return Err(Error::Network);
                    }
                    l.packet = p
                        .max_packet_size
                        .map(|n| (n as usize).min(MAX_WIRE_PACKET))
                        .unwrap_or(MAX_WIRE_PACKET);
                    l.qos = p.max_qos.unwrap_or(2);
                    l.retain = p.retain_available != Some(0);
                    l.wildcard = p.wildcard_subscription_available != Some(0);
                    l.shared = p.shared_subscription_available != Some(0);
                    l.keep_alive = p.server_keep_alive;
                }
                limits = Some(l);
                Packet::ConnAck(rumqttc::ConnAck {
                    session_present: a.session_present,
                    code: rumqttc::ConnectReturnCode::Success,
                })
            }
            p5::Packet::Publish(p) => {
                if p.properties
                    .as_ref()
                    .is_some_and(|p| p.topic_alias.is_some())
                {
                    return Err(Error::Unsupported);
                }
                let mut out = rumqttc::Publish::new(
                    String::from_utf8(p.topic.to_vec()).map_err(|_| Error::Network)?,
                    match p.qos {
                        Q5::AtMostOnce => QoS::AtMostOnce,
                        Q5::AtLeastOnce => QoS::AtLeastOnce,
                        Q5::ExactlyOnce => QoS::ExactlyOnce,
                    },
                    p.payload.to_vec(),
                );
                out.pkid = p.pkid;
                out.dup = p.dup;
                out.retain = p.retain;
                Packet::Publish(out)
            }
            p5::Packet::PubAck(a) => {
                if !matches!(
                    a.reason,
                    p5::PubAckReason::Success | p5::PubAckReason::NoMatchingSubscribers
                ) {
                    return Err(Error::Refused);
                }
                Packet::PubAck(rumqttc::PubAck::new(a.pkid))
            }
            p5::Packet::SubAck(a) => {
                if a.return_codes.len() != 1
                    || a.return_codes
                        .iter()
                        .any(|q| !matches!(q, p5::SubscribeReasonCode::Success(_)))
                {
                    return Err(Error::Refused);
                }
                let codes = a
                    .return_codes
                    .into_iter()
                    .map(|q| match q {
                        p5::SubscribeReasonCode::Success(q) => {
                            rumqttc::SubscribeReasonCode::Success(match q {
                                Q5::AtMostOnce => QoS::AtMostOnce,
                                Q5::AtLeastOnce => QoS::AtLeastOnce,
                                Q5::ExactlyOnce => QoS::ExactlyOnce,
                            })
                        }
                        _ => rumqttc::SubscribeReasonCode::Failure,
                    })
                    .collect();
                Packet::SubAck(rumqttc::SubAck::new(a.pkid, codes))
            }
            p5::Packet::UnsubAck(a) => {
                if a.reasons.len() != 1
                    || a.reasons.iter().any(|r| {
                        !matches!(
                            r,
                            p5::UnsubAckReason::Success | p5::UnsubAckReason::NoSubscriptionExisted
                        )
                    })
                {
                    return Err(Error::Refused);
                }
                Packet::UnsubAck(rumqttc::UnsubAck::new(a.pkid))
            }
            p5::Packet::PingResp(_) => Packet::PingResp,
            p5::Packet::Disconnect(_) => return Err(Error::Network),
            _ => return Err(Error::Unsupported),
        }
    };
    if !b.is_empty() {
        return Err(Error::Network);
    }
    if let Packet::Publish(p) = &p {
        crate::validate_topic(&p.topic, false).map_err(|_| Error::Network)?;
        if p.payload.len() > crate::MAX_PAYLOAD {
            return Err(Error::Overflow);
        }
        if p.qos != QoS::AtMostOnce && p.pkid == 0 {
            return Err(Error::Network);
        }
    }
    Ok((p, limits))
}
