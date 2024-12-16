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

pub struct MqttClientV311 {
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
    Box::new(MqttClientV311 {
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
impl Client for MqttClientV311 {
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn start<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<dyn ::core::future::Future<Output = ()> + ::core::marker::Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
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
