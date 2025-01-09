use bytes::{BufMut, BytesMut};
use bytestring::ByteString;

use crate::{
    error::EncodeError,
    types::{packet_type, MQTT, MQTT_LEVEL_3, WILL_QOS_SHIFT},
    utils::{write_variable_length, Encode},
};

use super::{packet::*, ConnectFlags, QoS};

pub(crate) fn get_encoded_publish_size(p: &Publish) -> usize {
    // Topic + Packet Id + Payload
    if p.qos == QoS::AtLeastOnce || p.qos == QoS::ExactlyOnce {
        4 + p.topic.len() + p.payload.len()
    } else {
        2 + p.topic.len() + p.payload.len()
    }
}

pub(crate) fn get_encoded_subscribe_size(topic_filters: &[(ByteString, QoS)]) -> usize {
    2 + topic_filters
        .iter()
        .fold(0, |acc, (filter, _)| acc + 2 + filter.len() + 1)
}

pub(crate) fn get_encoded_unsubscribe_size(topic_filters: &[ByteString]) -> usize {
    2 + topic_filters
        .iter()
        .fold(0, |acc, filter| acc + 2 + filter.len())
}

pub(crate) fn get_encoded_size(packet: &Packet) -> usize {
    match *packet {
        Packet::Connect ( ref connect ) => {
            let Connect {ref last_will, ref client_id, ref username, ref password, ..} = **connect;

            // Protocol Name + Protocol Level + Connect Flags + Keep Alive
            let mut n = 2 + 4 + 1 + 1 + 2;

            // Client Id
            n += 2 + client_id.len();

            // Will Topic + Will Message
            if let Some(LastWill { ref topic, ref message, .. }) = *last_will {
                n += 2 + topic.len() + 2 + message.len();
            }

            if let Some(ref s) = *username {
                n += 2 + s.len();
            }

            if let Some(ref s) = *password {
                n += 2 + s.len();
            }

            n
        }

        Packet::Publish( ref publish ) => get_encoded_publish_size(publish),
        Packet::ConnectAck { .. } | // Flags + Return Code
        Packet::PublishAck { .. } | // Packet Id
        Packet::PublishReceived { .. } | // Packet Id
        Packet::PublishRelease { .. } | // Packet Id
        Packet::PublishComplete { .. } | // Packet Id
        Packet::UnsubscribeAck { .. } => 2, // Packet Id
        Packet::Subscribe { ref topic_filters, .. } => get_encoded_subscribe_size(topic_filters),
        Packet::SubscribeAck { ref status, .. } => 2 + status.len(),

        Packet::Unsubscribe { ref topic_filters, .. } => get_encoded_unsubscribe_size(topic_filters),

        Packet::PingRequest | Packet::PingResponse | Packet::Disconnect => 0,
    }
}

pub(crate) fn encode(
    packet: &Packet,
    dst: &mut BytesMut,
    content_size: u32,
) -> Result<(), EncodeError> {
    match packet {
        Packet::Connect(connect) => {
            dst.put_u8(packet_type::CONNECT);
            write_variable_length(content_size, dst);
            encode_connect(connect, dst)?;
        }
        Packet::ConnectAck(ack) => {
            dst.put_u8(packet_type::CONNACK);
            write_variable_length(content_size, dst);
            let flags_byte = u8::from(ack.session_present);
            let code: u8 = From::from(ack.return_code);
            dst.put_slice(&[flags_byte, code]);
        }
        Packet::Publish(publish) => {
            dst.put_u8(
                packet_type::PUBLISH_START
                    | (u8::from(publish.qos) << 1)
                    | ((publish.dup as u8) << 3)
                    | (publish.retain as u8),
            );
            write_variable_length(content_size, dst);
            publish.topic.encode(dst)?;
            if publish.qos == QoS::AtMostOnce {
                if publish.packet_id.is_some() {
                    return Err(EncodeError::MalformedPacket); // packet id must not be set
                }
            } else {
                publish
                    .packet_id
                    .ok_or(EncodeError::PacketIdRequired)?
                    .encode(dst)?;
            }
            dst.put(publish.payload.as_ref());
        }

        Packet::PublishAck { packet_id } => {
            dst.put_u8(packet_type::PUBACK);
            write_variable_length(content_size, dst);
            packet_id.encode(dst)?;
        }
        Packet::PublishReceived { packet_id } => {
            dst.put_u8(packet_type::PUBREC);
            write_variable_length(content_size, dst);
            packet_id.encode(dst)?;
        }
        Packet::PublishRelease { packet_id } => {
            dst.put_u8(packet_type::PUBREL);
            write_variable_length(content_size, dst);
            packet_id.encode(dst)?;
        }
        Packet::PublishComplete { packet_id } => {
            dst.put_u8(packet_type::PUBCOMP);
            write_variable_length(content_size, dst);
            packet_id.encode(dst)?;
        }
        Packet::Subscribe {
            packet_id,
            ref topic_filters,
        } => {
            dst.put_u8(packet_type::SUBSCRIBE);
            write_variable_length(content_size, dst);
            packet_id.encode(dst)?;
            for &(ref filter, qos) in topic_filters {
                filter.encode(dst)?;
                dst.put_u8(qos.into());
            }
        }
        Packet::SubscribeAck {
            packet_id,
            ref status,
        } => {
            dst.put_u8(packet_type::SUBACK);
            write_variable_length(content_size, dst);
            packet_id.encode(dst)?;
            let buf: Vec<u8> = status
                .iter()
                .map(|s| match *s {
                    SubscribeReturnCode::Success(qos) => qos.into(),
                    _ => 0x80u8,
                })
                .collect();
            dst.put_slice(&buf);
        }
        Packet::Unsubscribe {
            packet_id,
            ref topic_filters,
        } => {
            dst.put_u8(packet_type::UNSUBSCRIBE);
            write_variable_length(content_size, dst);
            packet_id.encode(dst)?;
            for filter in topic_filters {
                filter.encode(dst)?;
            }
        }
        Packet::UnsubscribeAck { packet_id } => {
            dst.put_u8(packet_type::UNSUBACK);
            write_variable_length(content_size, dst);
            packet_id.encode(dst)?;
        }
        Packet::PingRequest => dst.put_slice(&[packet_type::PINGREQ, 0]),
        Packet::PingResponse => dst.put_slice(&[packet_type::PINGRESP, 0]),
        Packet::Disconnect => dst.put_slice(&[packet_type::DISCONNECT, 0]),
    }

    Ok(())
}

fn encode_connect(connect: &Connect, dst: &mut BytesMut) -> Result<(), EncodeError> {
    let Connect {
        clean_session,
        keep_alive,
        ref last_will,
        ref client_id,
        ref username,
        ref password,
    } = *connect;

    MQTT.as_ref().encode(dst)?;

    let mut flags = ConnectFlags::empty();

    if username.is_some() {
        flags |= ConnectFlags::USERNAME;
    }
    if password.is_some() {
        flags |= ConnectFlags::PASSWORD;
    }

    if let Some(LastWill { qos, retain, .. }) = *last_will {
        flags |= ConnectFlags::WILL;

        if retain {
            flags |= ConnectFlags::WILL_RETAIN;
        }

        let b: u8 = qos as u8;

        flags |= ConnectFlags::from_bits_truncate(b << WILL_QOS_SHIFT);
    }

    if clean_session {
        flags |= ConnectFlags::CLEAN_START;
    }

    dst.put_slice(&[MQTT_LEVEL_3, flags.bits()]);
    dst.put_u16(keep_alive);
    client_id.encode(dst)?;

    if let Some(LastWill {
        ref topic,
        ref message,
        ..
    }) = *last_will
    {
        topic.encode(dst)?;
        message.encode(dst)?;
    }

    if let Some(ref s) = *username {
        s.encode(dst)?;
    }

    if let Some(ref s) = *password {
        s.encode(dst)?;
    }
    Ok(())
}
