use std::sync::Arc;

use super::*;
use bytes::{Buf, Bytes};

/// Publish packet
#[derive(Clone, PartialEq, Eq)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: String,
    pub pkid: u16,
    pub payload: Option<Arc<Bytes>>,
}

impl Publish {
    pub fn new<S: Into<String>>(topic: S, qos: QoS, payload: Arc<Bytes>) -> Publish {
        Publish {
            dup: false,
            qos,
            retain: false,
            pkid: 0,
            topic: topic.into(),
            payload: Some(payload),
        }
    }

    pub fn set_pkid(&mut self, pkid: u16) {
        self.pkid = pkid;
    }

    fn len(&self) -> usize {
        let payload_len = match self.payload {
            Some(ref payload) => payload.len(),
            None => 0,
        };
        let len = 2 + self.topic.len() + payload_len;
        if self.qos != QoS::AtMostOnce && self.pkid != 0 {
            len + 2
        } else {
            len
        }
    }

    pub fn size(&self) -> usize {
        let len = self.len();
        let remaining_len_size = len_len(len);

        1 + remaining_len_size + len
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let qos = qos((fixed_header.byte1 & 0b0110) >> 1)?;
        let dup = (fixed_header.byte1 & 0b1000) != 0;
        let retain = (fixed_header.byte1 & 0b0001) != 0;

        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);
        let topic = read_mqtt_string(&mut bytes)?;

        // Packet identifier exists where QoS > 0
        let pkid = match qos {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce | QoS::ExactlyOnce => {
                let pkid = read_u16(&mut bytes)?;
                if pkid == 0 {
                    return Err(Error::PacketIdZero);
                }
                pkid
            }
        };

        let publish = Publish {
            dup,
            retain,
            qos,
            pkid,
            topic,
            payload: None,
        };

        Ok(publish)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = self.len();

        let dup = self.dup as u8;
        let qos = self.qos as u8;
        let retain = self.retain as u8;
        buffer.put_u8(0b0011_0000 | retain | qos << 1 | dup << 3);

        let count = write_remaining_length(buffer, len)?;
        write_mqtt_string(buffer, self.topic.as_str());

        buffer.put_u16(self.pkid);

        // if self.qos != QoS::AtMostOnce {
        //     let pkid = self.pkid;
        //     if pkid == 0 {
        //         return Err(Error::PacketIdZero);
        //     }

        //     buffer.put_u16(pkid);
        // }
        buffer.put_slice(&self.payload.as_ref().unwrap());

        Ok(1 + count + len)
    }
}

impl fmt::Debug for Publish {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Topic = {}, Qos = {:?}, Retain = {}, Pkid = {:?}",
            self.topic,
            self.qos,
            self.retain,
            self.pkid,
            // self.payload.len()
        )
    }
}
