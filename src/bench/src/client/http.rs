use std::sync::Arc;

use async_trait::async_trait;
use rumqttc::AsyncClient;
use tokio::sync::watch;
use types::{PublishCreateUpdateReq, SubscribeCreateUpdateReq};

use crate::Status;

use super::{
    mqtt_v311::{Publish, Subscribe},
    Client, ClientConf, ClientStatus,
};

pub struct HttpClient {
    running: bool,
    conf: ClientConf,
    client: Option<AsyncClient>,
    err: Option<String>,
    publishes: Vec<Publish>,
    subscribes: Vec<Subscribe>,
    stop_signal_tx: Option<watch::Sender<()>>,
    status: Arc<Status>,
}

pub fn new(conf: ClientConf) -> Box<dyn Client> {
    Box::new(HttpClient {
        running: false,
        conf,
        client: None,
        err: None,
        publishes: vec![],
        subscribes: vec![],
        stop_signal_tx: None,
        status: Arc::new(Status::default()),
    })
}

#[async_trait]
impl Client for HttpClient {
    async fn start(&mut self) {
        todo!()
    }

    fn stop(&mut self) {
        todo!()
    }

    fn get_status(&self) -> ClientStatus {
        todo!()
    }

    fn create_publish(&mut self, req: Arc<PublishCreateUpdateReq>) {
        todo!()
    }

    async fn create_subscribe(&mut self, req: Arc<SubscribeCreateUpdateReq>) {
        todo!()
    }
}
