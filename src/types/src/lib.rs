use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BrokerUpdateReq {
    pub hosts: Vec<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_id: Option<String>,
    // 每毫秒
    pub connect_interval: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupCreateUpdateReq {
    pub name: String,
    pub protocol_version: ProtocolVersion,
    pub protocol: Protocol,
    pub port: u16,
    pub client_count: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolVersion {
    V311,
    V50,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Mqtt,
    Websocket,
    Http,
}

#[derive(Serialize)]
pub struct ListGroupResp {
    // pub count: usize,
    pub list: Vec<ListGroupRespItem>,
}

#[derive(Serialize)]
pub struct ListGroupRespItem {
    pub id: String,
    pub conf: GroupCreateUpdateReq,
    // pub name: String,
    // pub protocol_version: ProtocolVersion,
    // pub protocol: Protocol,
    // pub port: u16,
    // pub client_count: usize,
}

#[derive(Serialize)]
pub struct ReadGroupResp {
    pub id: String,
    pub conf: GroupCreateUpdateReq,
    pub status: Vec<GroupStatus>,
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

    pub v311: Option<PublishV311>,
    pub v50: Option<PublishV50>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PublishV311 {
    // pub name: String,
    // pub topic: String,
    // pub qos: Qos,
    // pub retain: bool,
    // // 毫秒
    // pub interval: u64,
    // pub payload: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PublishV50 {
    // pub name: String,
    // pub topic: String,
    // pub qos: Qos,
    // pub retain: bool,
    // // 毫秒
    // pub interval: u64,
    // pub payload: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListPublishResp {
    pub list: Vec<ListPublishRespItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListPublishRespItem {
    pub id: String,
    pub conf: PublishCreateUpdateReq,
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
    pub status: Status,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupStatus {
    pub ts: u64,
    pub succeed: usize,
    pub failed: usize,
    pub status: Status,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscribeCreateUpdateReq {
    pub name: String,
    pub topic: String,
    pub qos: Qos,
    pub v311: Option<SubscribeV311>,
    pub v50: Option<SubscribeV50>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscribeV311 {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscribeV50 {}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Status {
    // 连接确认
    pub conn_ack: usize,
    // 发布确认
    pub pub_ack: usize,
    // 取消订阅确认
    pub unsub_ack: usize,
    // ping请求
    pub ping_req: usize,
    // ping响应
    pub ping_resp: usize,
    // 发布
    pub publish: usize,
    // 订阅
    pub subscribe: usize,
    // 取消订阅
    pub unsubscribe: usize,
    // 连接断开
    pub disconnect: usize,
}
