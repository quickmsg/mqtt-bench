use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use super::{Error, Packet};

/// MQTT v4 codec
#[derive(Debug, Clone)]
pub struct Codec {
    /// Maximum packet size allowed by client
    pub max_incoming_size: Option<u32>,
    /// Maximum packet size allowed by broker
    pub max_outgoing_size: Option<u32>,
}

impl Decoder for Codec {
    type Item = Packet;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match Packet::read(src, self.max_incoming_size) {
            Ok(packet) => Ok(Some(packet)),
            Err(Error::InsufficientBytes(b)) => {
                // Get more packets to construct the incomplete packet
                src.reserve(b);
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
}

impl Encoder<Packet> for Codec {
    type Error = Error;

    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.write(dst, self.max_outgoing_size)?;

        Ok(())
    }
}