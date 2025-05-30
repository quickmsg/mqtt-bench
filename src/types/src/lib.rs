use std::{
    ops::{Add, AddAssign},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

pub mod group;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BrokerUpdateReq {
    pub hosts: Vec<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_id: Option<String>,
    // 每毫秒
    pub connect_interval: u64,
    // 秒
    pub statistics_interval: u64,

    pub local_ips: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupCreateReq {
    pub name: String,
    pub client_id: Option<String>,
    pub protocol_version: ProtocolVersion,
    pub protocol: Protocol,
    pub port: u16,
    pub client_count: Option<usize>,
    pub ssl_conf: Option<SslConf>,

    // client_id, username, password, topic
    pub clients: Option<Vec<(String, String, String, String)>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupUpdateReq {
    pub name: String,
    pub client_id: String,
    pub port: u16,
    pub client_count: usize,
    pub ssl_conf: Option<SslConf>,
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

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SslConf {
    pub verify: bool,
    pub ca_cert: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
}

#[derive(Serialize)]
pub struct GroupListResp {
    pub list: Vec<GroupListRespItem>,
}

#[derive(Serialize)]
pub struct GroupListRespItem {
    pub id: String,
    pub status: Status,
    pub conf: GroupCreateReq,
}

#[derive(Serialize)]
pub struct ReadGroupResp {
    pub id: String,
    pub conf: GroupCreateReq,
}

#[derive(Debug, Clone)]
pub struct PublishConf {
    pub name: String,
    pub topic: String,
    pub qos: Qos,
    pub retain: bool,
    pub tps: usize,
    pub payload: Arc<Vec<u8>>,

    pub v311: Option<PublishV311>,
    pub v50: Option<PublishV50>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PublishCreateUpdateReq {
    pub name: String,
    pub topic: String,
    pub qos: Qos,
    pub retain: bool,
    pub tps: usize,
    pub payload: Option<String>,
    pub size: Option<usize>,
    pub range: Option<usize>,

    pub v311: Option<PublishV311>,
    pub v50: Option<PublishV50>,
}

#[derive(Serialize)]
pub struct ListPublishResp {
    pub list: Vec<ListPublishRespItem>,
}

#[derive(Serialize)]
pub struct ListPublishRespItem {
    pub id: String,
    pub conf: PublishCreateUpdateReq,
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

#[derive(Deserialize_repr, Serialize_repr, Debug, Clone, Copy)]
#[repr(u8)]
pub enum Qos {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

// impl Into<mqtt::protocol::v3_mini::QoS> for Qos {
//     fn into(self) -> mqtt::protocol::v3_mini::QoS {
//         todo!()
//     }
// }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientUsizeMetrics {
    pub running_cnt: usize,
    pub waiting_cnt: usize,
    pub error_cnt: usize,
    pub stopped_cnt: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PacketUsizeMetrics {
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
    pub outgoing_publish: usize,

    pub incoming_publish: usize,

    pub pub_rel: usize,
    pub pub_rec: usize,
    pub pub_comp: usize,
    // 订阅
    pub subscribe: usize,
    // 订阅确认
    pub sub_ack: usize,
    // 取消订阅
    pub unsubscribe: usize,
    // 连接断开
    pub disconnect: usize,
}

impl AddAssign for PacketUsizeMetrics {
    fn add_assign(&mut self, rhs: Self) {
        self.conn_ack += rhs.conn_ack;
        self.pub_ack += rhs.pub_ack;
        self.unsub_ack += rhs.unsub_ack;
        self.ping_req += rhs.ping_req;
        self.ping_resp += rhs.ping_resp;
        self.outgoing_publish += rhs.outgoing_publish;
        self.incoming_publish += rhs.incoming_publish;
        self.pub_rel += rhs.pub_rel;
        self.pub_rec += rhs.pub_rec;
        self.pub_comp += rhs.pub_comp;
        self.subscribe += rhs.subscribe;
        self.sub_ack += rhs.sub_ack;
        self.unsubscribe += rhs.unsubscribe;
        self.disconnect += rhs.disconnect;
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscribeCreateUpdateReq {
    pub name: String,
    pub topic: String,
    pub qos: Qos,
    pub v311: Option<SubscribeV311>,
    pub v50: Option<SubscribeV50>,
}

#[derive(Serialize)]
pub struct ListSubscribeResp {
    pub list: Vec<ListSubscribeRespItem>,
}

#[derive(Serialize)]
pub struct ListSubscribeRespItem {
    pub id: String,
    pub conf: SubscribeCreateUpdateReq,
}

#[derive(Serialize)]
pub struct ReadSubscribeResp {
    pub id: String,
    pub conf: SubscribeCreateUpdateReq,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscribeV311 {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscribeV50 {}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Starting,
    Running,
    Stopped,
    Waiting,
    Updating,
}

#[derive(Deserialize)]
pub struct ClientsQueryParams {
    pub p: usize,
    pub s: usize,
    // TODO status
}

#[derive(Serialize)]
pub struct ClientsListResp {
    pub count: usize,
    pub list: Vec<ClientsListRespItem>,
}

#[derive(Serialize)]
pub struct ClientsListRespItem {
    pub client_id: String,
    pub status: Status,
    pub addr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
}

#[derive(Deserialize)]
pub struct MetricsQueryParams {
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
}

#[derive(Serialize)]
pub struct MetricsListResp {
    pub list: Vec<MetricsListItem>,
}

#[derive(Serialize)]
pub struct MetricsListItem {
    pub ts: u64,
    pub client: ClientMetrics,
    pub packet: PacketMetrics,
}

#[derive(Serialize)]
pub struct ClientMetrics {
    pub running_cnt: usize,
    pub stopped_cnt: usize,
    pub error_cnt: usize,
    pub waiting_cnt: usize,
}

#[derive(Serialize)]
pub struct PacketMetrics {
    pub conn_ack_total: usize,
    pub conn_ack_cnt: usize,

    pub pub_ack_total: usize,
    pub pub_ack_cnt: usize,

    pub unsub_ack_total: usize,
    pub unsub_ack_cnt: usize,

    pub ping_req_total: usize,
    pub ping_req_cnt: usize,

    pub ping_resp_total: usize,
    pub ping_resp_cnt: usize,

    pub outgoing_publish_total: usize,
    pub outgoing_publish_cnt: usize,

    pub incoming_publish_total: usize,
    pub incoming_publish_cnt: usize,

    pub pub_rel_total: usize,
    pub pub_rel_cnt: usize,

    pub pub_rec_total: usize,
    pub pub_rec_cnt: usize,

    pub pub_comp_total: usize,
    pub pub_comp_cnt: usize,

    pub subscribe_total: usize,
    pub subscribe_cnt: usize,

    pub sub_ack_total: usize,
    pub sub_ack_cnt: usize,

    pub unsubscribe_total: usize,
    pub unsubscribe_cnt: usize,

    pub disconnect_total: usize,
    pub disconnect_cnt: usize,
}
