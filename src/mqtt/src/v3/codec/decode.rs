use std::num::NonZeroU16;

use bytes::{Buf, Bytes};
use bytestring::ByteString;

use crate::ensure;
use crate::error::DecodeError;
use crate::types::{packet_type, QoS, MQTT, MQTT_LEVEL_3, WILL_QOS_SHIFT};
use crate::utils::Decode;

use super::packet::{Connect, ConnectAck, LastWill, Packet, Publish, SubscribeReturnCode};
use super::{ConnectAckFlags, ConnectFlags};

pub(crate) fn decode_packet(mut src: Bytes, first_byte: u8) -> Result<Packet, DecodeError> {
    match first_byte {
        packet_type::CONNECT => decode_connect_packet(&mut src),
        packet_type::CONNACK => decode_connect_ack_packet(&mut src),
        packet_type::PUBLISH_START..=packet_type::PUBLISH_END => {
            decode_publish_packet(&mut src, first_byte & 0b0000_1111)
        }
        packet_type::PUBACK => decode_ack(src, |packet_id| Packet::PublishAck { packet_id }),
        packet_type::PUBREC => decode_ack(src, |packet_id| Packet::PublishReceived { packet_id }),
        packet_type::PUBREL => decode_ack(src, |packet_id| Packet::PublishRelease { packet_id }),
        packet_type::PUBCOMP => decode_ack(src, |packet_id| Packet::PublishComplete { packet_id }),
        packet_type::SUBSCRIBE => decode_subscribe_packet(&mut src),
        packet_type::SUBACK => decode_subscribe_ack_packet(&mut src),
        packet_type::UNSUBSCRIBE => decode_unsubscribe_packet(&mut src),
        packet_type::UNSUBACK => decode_ack(src, |packet_id| Packet::UnsubscribeAck { packet_id }),
        packet_type::PINGREQ => Ok(Packet::PingRequest),
        packet_type::PINGRESP => Ok(Packet::PingResponse),
        packet_type::DISCONNECT => Ok(Packet::Disconnect),
        _ => Err(DecodeError::UnsupportedPacketType),
    }
}

#[inline]
fn decode_ack(mut src: Bytes, f: impl Fn(NonZeroU16) -> Packet) -> Result<Packet, DecodeError> {
    let packet_id = NonZeroU16::decode(&mut src)?;
    ensure!(!src.has_remaining(), DecodeError::InvalidLength);
    Ok(f(packet_id))
}

fn decode_connect_packet(src: &mut Bytes) -> Result<Packet, DecodeError> {
    ensure!(src.remaining() >= 10, DecodeError::InvalidLength);
    let len = src.get_u16();

    ensure!(
        len == 4 && &src.as_ref()[0..4] == MQTT,
        DecodeError::InvalidProtocol
    );
    src.advance(4);

    let level = src.get_u8();
    ensure!(level == MQTT_LEVEL_3, DecodeError::UnsupportedProtocolLevel);

    let flags = ConnectFlags::from_bits(src.get_u8()).ok_or(DecodeError::ConnectReservedFlagSet)?;

    let keep_alive = u16::decode(src)?;
    let client_id = ByteString::decode(src)?;

    ensure!(
        !client_id.is_empty() || flags.contains(ConnectFlags::CLEAN_START),
        DecodeError::InvalidClientId
    );

    let last_will = if flags.contains(ConnectFlags::WILL) {
        let topic = ByteString::decode(src)?;
        let message = Bytes::decode(src)?;
        Some(LastWill {
            qos: QoS::try_from((flags & ConnectFlags::WILL_QOS).bits() >> WILL_QOS_SHIFT)?,
            retain: flags.contains(ConnectFlags::WILL_RETAIN),
            topic,
            message,
        })
    } else {
        None
    };
    let username = if flags.contains(ConnectFlags::USERNAME) {
        Some(ByteString::decode(src)?)
    } else {
        None
    };
    let password = if flags.contains(ConnectFlags::PASSWORD) {
        Some(Bytes::decode(src)?)
    } else {
        None
    };
    Ok(Connect {
        clean_session: flags.contains(ConnectFlags::CLEAN_START),
        keep_alive,
        client_id,
        last_will,
        username,
        password,
    }
    .into())
}

fn decode_connect_ack_packet(src: &mut Bytes) -> Result<Packet, DecodeError> {
    ensure!(src.remaining() >= 2, DecodeError::InvalidLength);
    let flags =
        ConnectAckFlags::from_bits(src.get_u8()).ok_or(DecodeError::ConnAckReservedFlagSet)?;

    let return_code = src.get_u8().try_into()?;
    Ok(Packet::ConnectAck(ConnectAck {
        return_code,
        session_present: flags.contains(ConnectAckFlags::SESSION_PRESENT),
    }))
}

fn decode_publish_packet(src: &mut Bytes, packet_flags: u8) -> Result<Packet, DecodeError> {
    let topic = ByteString::decode(src)?;
    let qos = QoS::try_from((packet_flags & 0b0110) >> 1)?;
    let packet_id = if qos == QoS::AtMostOnce {
        None
    } else {
        Some(NonZeroU16::decode(src)?) // packet id = 0 encountered
    };

    Ok(Packet::Publish(Publish {
        dup: (packet_flags & 0b1000) == 0b1000,
        qos,
        retain: (packet_flags & 0b0001) == 0b0001,
        topic,
        packet_id,
        payload: src.split_off(0),
    }))
}

fn decode_subscribe_packet(src: &mut Bytes) -> Result<Packet, DecodeError> {
    let packet_id = NonZeroU16::decode(src)?;
    let mut topic_filters = Vec::new();
    while src.has_remaining() {
        let topic = ByteString::decode(src)?;
        ensure!(src.remaining() >= 1, DecodeError::InvalidLength);
        let qos = (src.get_u8() & 0b0000_0011).try_into()?;
        topic_filters.push((topic, qos));
    }

    Ok(Packet::Subscribe {
        packet_id,
        topic_filters,
    })
}

fn decode_subscribe_ack_packet(src: &mut Bytes) -> Result<Packet, DecodeError> {
    let packet_id = NonZeroU16::decode(src)?;
    let mut status = Vec::with_capacity(src.len());
    for code in src.as_ref().iter() {
        status.push(if *code == 0x80 {
            SubscribeReturnCode::Failure
        } else {
            SubscribeReturnCode::Success(QoS::try_from(*code)?)
        });
    }
    Ok(Packet::SubscribeAck { packet_id, status })
}

fn decode_unsubscribe_packet(src: &mut Bytes) -> Result<Packet, DecodeError> {
    let packet_id = NonZeroU16::decode(src)?;
    let mut topic_filters = Vec::new();
    while src.remaining() > 0 {
        topic_filters.push(ByteString::decode(src)?);
    }
    Ok(Packet::Unsubscribe {
        packet_id,
        topic_filters,
    })
}
