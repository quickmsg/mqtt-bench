use std::sync::Arc;

use async_trait::async_trait;
use types::{
    ClientsListRespItem, GroupCreateUpdateReq, PublishCreateUpdateReq, SubscribeCreateUpdateReq,
};

use crate::UsizeMetrics;

pub mod mqtt_v311;
pub mod mqtt_v50;
mod ssl;
mod v311;
mod v50;
pub mod websocket_v311;
pub mod websocket_v50;

#[async_trait]
pub trait Client: Sync + Send {
    async fn start(&mut self);
    async fn stop(&mut self);
    async fn update(&mut self, group_conf: Arc<GroupCreateUpdateReq>);
    fn get_metrics(&self) -> ClientMetrics;

    fn create_publish(&mut self, id: Arc<String>, req: Arc<PublishCreateUpdateReq>);
    fn update_publish(&mut self, id: &String, req: Arc<PublishCreateUpdateReq>);
    fn delete_publish(&mut self, id: &String);

    async fn create_subscribe(&mut self, id: Arc<String>, req: Arc<SubscribeCreateUpdateReq>);
    async fn update_subscribe(&mut self, subscribe_id: &String, req: Arc<SubscribeCreateUpdateReq>);
    async fn delete_subscribe(&mut self, subscribe_id: &String);

    async fn read(&self) -> ClientsListRespItem;
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

pub struct ClientMetrics {
    pub success: bool,
    pub usize_metrics: UsizeMetrics,
}
