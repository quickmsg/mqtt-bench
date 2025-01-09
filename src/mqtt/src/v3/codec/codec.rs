use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::{DecodeError, EncodeError};

use super::micro_packet::MicroPacket;

#[derive(Debug, Clone)]
/// Mqtt v3.1.1 protocol codec
pub struct Codec;

impl Default for Codec {
    fn default() -> Self {
        Self {}
    }
}

impl Decoder for Codec {
    type Item = MicroPacket;
    type Error = DecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, DecodeError> {
        match Self::Item::read(src) {
            Ok(packet) => Ok(Some(packet)),
            Err(DecodeError::InsufficientBytes(b)) => {
                src.reserve(b);
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
}

impl Encoder<Arc<Vec<u8>>> for Codec {
    type Error = EncodeError;

    fn encode(&mut self, item: Arc<Vec<u8>>, dst: &mut BytesMut) -> Result<(), EncodeError> {
        dst.put_slice(&item);
        Ok(())
    }
}
