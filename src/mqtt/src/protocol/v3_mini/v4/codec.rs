use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::{Error, Packet};

/// MQTT v4 codec
#[derive(Debug, Clone)]
pub struct Codec {
    /// Maximum packet size allowed by client
    pub max_incoming_size: usize,
    /// Maximum packet size allowed by broker
    pub max_outgoing_size: usize,
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

// impl Encoder<Packet> for Codec {
//     type Error = Error;

//     fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
//         item.write(dst);
//         // dst.put_slice(&item);

//         Ok(())
//     }
// }

impl Encoder<Packet> for Codec {
    type Error = Error;

    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.write(dst)?;
        Ok(())
    }
}
