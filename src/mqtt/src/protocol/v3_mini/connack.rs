use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Return code in connack
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectReturnCode {
    Success = 0,
    RefusedProtocolVersion,
    BadClientId,
    ServiceUnavailable,
    BadUserNamePassword,
    NotAuthorized,
}

/// Acknowledgement to connect packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck;

impl ConnAck {
    // pub fn new(code: ConnectReturnCode, session_present: bool) -> ConnAck {
    //     ConnAck {
    //         session_present,
    //         code,
    //     }
    // }

    fn len(&self) -> usize {
        // sesssion present + code

        1 + 1
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);

        // let flags = read_u8(&mut bytes)?;
        // let return_code = read_u8(&mut bytes)?;

        // let session_present = (flags & 0x01) == 1;
        // let code = connect_return(return_code)?;
        // let connack = ConnAck {
        //     session_present,
        //     code,
        // };

        Ok(Self {})
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = self.len();
        buffer.put_u8(0x20);

        let count = write_remaining_length(buffer, len)?;
        buffer.put_u8(self.session_present as u8);
        buffer.put_u8(self.code as u8);

        Ok(1 + count + len)
    }

    pub fn size(&self) -> usize {
        let len = self.len();
        let remaining_len_size = len_len(len);

        1 + remaining_len_size + len
    }
}

/// Connection return code type
fn connect_return(num: u8) -> Result<ConnectReturnCode, Error> {
    match num {
        0 => Ok(ConnectReturnCode::Success),
        1 => Ok(ConnectReturnCode::RefusedProtocolVersion),
        2 => Ok(ConnectReturnCode::BadClientId),
        3 => Ok(ConnectReturnCode::ServiceUnavailable),
        4 => Ok(ConnectReturnCode::BadUserNamePassword),
        5 => Ok(ConnectReturnCode::NotAuthorized),
        num => Err(Error::InvalidConnectReturnCode(num)),
    }
}
