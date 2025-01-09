use std::slice::Iter;

use bytes::BytesMut;

use crate::{error::DecodeError, types::FixedHeader};

/// MQTT Control Packets
pub enum MicroPacket {
    /// Connect acknowledgment
    ConnectAck,

    /// Publish message
    Publish,

    /// Publish acknowledgment
    PublishAck,

    /// Publish received (assured delivery part 1)
    PublishReceived,

    /// Publish release (assured delivery part 2)
    PublishRelease,

    /// Publish complete (assured delivery part 3)
    PublishComplete,

    /// Subscribe acknowledgment
    SubscribeAck,

    /// Unsubscribe acknowledgment
    UnsubscribeAck,

    /// PING request
    PingRequest,
    /// PING response
    PingResponse,
    /// Client is disconnecting
    Disconnect,
}

impl MicroPacket {
    pub fn read(stream: &mut BytesMut) -> Result<Self, DecodeError> {
        let fixed_header = check(stream.iter())?;

        // Test with a stream with exactly the size to check border panics
        let packet = stream.split_to(fixed_header.frame_length());
        let packet_type = fixed_header.packet_type()?;

        if fixed_header.remaining_len == 0 {
            // no payload packets
            return match packet_type {
                PacketType::PingReq => Ok(Packet::PingReq),
                PacketType::PingResp => Ok(Packet::PingResp),
                PacketType::Disconnect => Ok(Packet::Disconnect),
                _ => Err(Error::PayloadRequired),
            };
        }

        let packet = packet.freeze();
        let packet = match packet_type {
            PacketType::Connect => Packet::Connect(Connect::read(fixed_header, packet)?),
            PacketType::ConnAck => Packet::ConnAck(ConnAck::read(fixed_header, packet)?),
            PacketType::Publish => Packet::Publish(Publish::read(fixed_header, packet)?),
            PacketType::PubAck => Packet::PubAck(PubAck::read(fixed_header, packet)?),
            PacketType::PubRec => Packet::PubRec(PubRec::read(fixed_header, packet)?),
            PacketType::PubRel => Packet::PubRel(PubRel::read(fixed_header, packet)?),
            PacketType::PubComp => Packet::PubComp(PubComp::read(fixed_header, packet)?),
            PacketType::Subscribe => Packet::Subscribe(Subscribe::read(fixed_header, packet)?),
            PacketType::SubAck => Packet::SubAck(SubAck::read(fixed_header, packet)?),
            PacketType::Unsubscribe => {
                Packet::Unsubscribe(Unsubscribe::read(fixed_header, packet)?)
            }
            PacketType::UnsubAck => Packet::UnsubAck(UnsubAck::read(fixed_header, packet)?),
            PacketType::PingReq => Packet::PingReq,
            PacketType::PingResp => Packet::PingResp,
            PacketType::Disconnect => Packet::Disconnect,
        };

        Ok(packet)
    }
}

pub fn check(stream: Iter<u8>) -> Result<FixedHeader, DecodeError> {
    // Create fixed header if there are enough bytes in the stream
    // to frame full packet
    let stream_len = stream.len();
    let fixed_header = parse_fixed_header(stream)?;

    // Don't let rogue connections attack with huge payloads.
    // Disconnect them before reading all that data
    if fixed_header.remaining_len > max_packet_size {
        return Err(Error::PayloadSizeLimitExceeded(fixed_header.remaining_len));
    }

    // If the current call fails due to insufficient bytes in the stream,
    // after calculating remaining length, we extend the stream
    let frame_length = fixed_header.frame_length();
    if stream_len < frame_length {
        return Err(Error::InsufficientBytes(frame_length - stream_len));
    }

    Ok(fixed_header)
}

fn parse_fixed_header(mut stream: Iter<u8>) -> Result<FixedHeader, DecodeError> {
    // At least 2 bytes are necessary to frame a packet
    let stream_len = stream.len();
    if stream_len < 2 {
        return Err(DecodeError::InsufficientBytes(2 - stream_len));
    }

    let byte1 = stream.next().unwrap();
    let (len_len, len) = length(stream)?;

    Ok(FixedHeader::new(*byte1, len_len, len))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub struct FixedHeader {
    /// First byte of the stream. Used to identify packet types and
    /// several flags
    byte1: u8,
    /// Length of fixed header. Byte 1 + (1..4) bytes. So fixed header
    /// len can vary from 2 bytes to 5 bytes
    /// 1..4 bytes are variable length encoded to represent remaining length
    fixed_header_len: usize,
    /// Remaining length of the packet. Doesn't include fixed header bytes
    /// Represents variable header + payload size
    remaining_len: usize,
}
