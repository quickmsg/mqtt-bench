use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BrokerUpdateReq {
    pub addrs: Vec<(String, u16)>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_id: Option<String>,
    // 每秒的速率
    pub connect_rate: u64,
    pub protocol_version: ProtocolVersion,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupCreateUpdateReq {
    pub name: String,
    pub client_count: usize,
}

#[derive(Serialize)]
pub struct ListGroupResp {
    // pub count: usize,
    pub list: Vec<ListGroupRespItem>,
}

#[derive(Serialize)]
pub struct ListGroupRespItem {
    pub id: String,
    pub name: String,
    pub client_count: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolVersion {
    V311,
    V5,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PublishCreateUpdateReq {
    pub name: String,
    pub topic: String,
    pub qos: Qos,
    pub retain: bool,
    // 毫秒
    pub interval: u64,
    pub payload: String,
}

#[derive(Deserialize_repr, Serialize_repr, Debug, Clone)]
#[repr(u8)]
pub enum Qos {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientStatus {
    pub ts: u64,
    pub succeed: usize,
    pub failed: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscribeCreateUpdateReq {
    pub name: String,
    pub topic: String,
    pub qos: Qos,
}
