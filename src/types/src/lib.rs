use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskCreateReq {
    pub mqtt_server_info: MqttServerInfo,
    pub connect_bench_info: ConnectBenchInfo,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MqttServerInfo {
    pub addrs: Vec<(String, u16)>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_id: Option<String>,
    pub mqtt_protocol_version: MqttProtocolVersion,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConnectBenchInfo {
    pub client_count: usize,
    // 每秒的速率
    pub rate: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PubInfo {
    pub topic: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum MqttProtocolVersion {
    V311,
    V5,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum BenchType {
    Conn,
    Pub,
    Sub,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Qos {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientStatus {
    pub ts: u64,
    pub succeed: usize,
    pub failed: usize,
}
