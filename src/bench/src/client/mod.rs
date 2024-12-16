use std::sync::Arc;

use async_trait::async_trait;
use types::{PublishCreateUpdateReq, SubscribeCreateUpdateReq};

pub mod http;
pub mod mqtt_v311;
pub mod mqtt_v50;
pub mod websocket_v311;
pub mod websocket_v50;

#[async_trait]
pub trait Client: Sync + Send {
    async fn start(&mut self);
    fn stop(&mut self);
    fn get_status(&self) -> ClientStatus;
    fn create_publish(&mut self, req: Arc<PublishCreateUpdateReq>);
    async fn create_subscribe(&mut self, req: Arc<SubscribeCreateUpdateReq>);
}

pub struct ClientConf {
    pub index: usize,
    pub id: String,
    pub host: String,
    pub port: u16,
    pub keep_alive: u64,
    pub username: Option<String>,
    pub password: Option<String>,
}

pub struct ClientStatus {
    pub success: bool,
    pub conn_ack: usize,
    pub pub_ack: usize,
    pub unsub_ack: usize,
    pub ping_req: usize,
    pub ping_resp: usize,
    pub publish: usize,
    pub subscribe: usize,
    pub unsubscribe: usize,
    pub disconnect: usize,
}
