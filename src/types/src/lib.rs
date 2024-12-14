use serde::{Deserialize, Serialize};

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
    pub client_count: u32,
    // 每秒的速率
    pub rate: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PubInfo {
    pub topic: String,
}

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientStatus {
    pub succeed: usize,
    pub failed: usize,
}
