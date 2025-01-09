use super::*;
use bytes::{Buf, Bytes};

/// Subscription packet
#[derive(Clone, PartialEq, Eq)]
pub struct Subscribe {
    pub pkid: u16,
    pub filters: Vec<SubscribeFilter>,
}

impl Subscribe {
    pub fn new<S: Into<String>>(path: S, qos: QoS) -> Subscribe {
        let filter = SubscribeFilter {
            path: path.into(),
            qos,
        };

        Subscribe {
            pkid: 0,
            filters: vec![filter],
        }
    }

    pub fn new_many<T>(topics: T) -> Subscribe
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let filters: Vec<SubscribeFilter> = topics.into_iter().collect();

        Subscribe { pkid: 0, filters }
    }

    pub fn add(&mut self, path: String, qos: QoS) -> &mut Self {
        let filter = SubscribeFilter { path, qos };

        self.filters.push(filter);
        self
    }

    fn len(&self) -> usize {
        // len of pkid + vec![subscribe filter len]
        2 + self.filters.iter().fold(0, |s, t| s + t.len())
    }

    pub fn size(&self) -> usize {
        let len = self.len();
        let remaining_len_size = len_len(len);

        1 + remaining_len_size + len
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);

        let pkid = read_u16(&mut bytes)?;

        // variable header size = 2 (packet identifier)
        let mut filters = Vec::new();

        while bytes.has_remaining() {
            let path = read_mqtt_string(&mut bytes)?;
            let options = read_u8(&mut bytes)?;
            let requested_qos = options & 0b0000_0011;

            filters.push(SubscribeFilter {
                path,
                qos: qos(requested_qos)?,
            });
        }

        match filters.len() {
            0 => Err(Error::EmptySubscription),
            _ => Ok(Subscribe { pkid, filters }),
        }
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        // write packet type
        buffer.put_u8(0x82);

        // write remaining length
        let remaining_len = self.len();
        let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

        // write packet id
        buffer.put_u16(self.pkid);

        // write filters
        for filter in self.filters.iter() {
            filter.write(buffer);
        }

        Ok(1 + remaining_len_bytes + remaining_len)
    }
}

///  Subscription filter
#[derive(Clone, PartialEq, Eq)]
pub struct SubscribeFilter {
    pub path: String,
    pub qos: QoS,
}

impl SubscribeFilter {
    pub fn new(path: String, qos: QoS) -> SubscribeFilter {
        SubscribeFilter { path, qos }
    }

    fn len(&self) -> usize {
        // filter len + filter + options
        2 + self.path.len() + 1
    }

    fn write(&self, buffer: &mut BytesMut) {
        let mut options = 0;
        options |= self.qos as u8;

        write_mqtt_string(buffer, self.path.as_str());
        buffer.put_u8(options);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetainForwardRule {
    OnEverySubscribe,
    OnNewSubscribe,
    Never,
}

impl fmt::Debug for Subscribe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Filters = {:?}, Packet id = {:?}",
            self.filters, self.pkid
        )
    }
}

impl fmt::Debug for SubscribeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Filter = {}, Qos = {:?}", self.path, self.qos)
    }
}