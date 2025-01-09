use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::{Error, Packet};

/// MQTT v4 codec
#[derive(Debug, Clone)]
pub struct Codec;

impl Decoder for Codec {
    type Item = Packet;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match Packet::read(src) {
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

impl Encoder<Arc<Vec<u8>>> for Codec {
    type Error = Error;

    fn encode(&mut self, item: Arc<Vec<u8>>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.put_slice(&item);

        Ok(())
    }
}
