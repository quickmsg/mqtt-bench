use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use async_trait::async_trait;
use rumqttc::v5;
use tokio::{select, sync::watch};
use types::{PublishCreateUpdateReq, SubscribeCreateUpdateReq};

use crate::{generate_id, Status};

use super::{get_v50_qos, Client, ClientConf, ClientStatus};

pub struct MqttClientV50 {
    running: bool,
    conf: ClientConf,
    client: Option<v5::AsyncClient>,
    err: Option<String>,
    publishes: Vec<Publish>,
    subscribes: Vec<Subscribe>,
    stop_signal_tx: Option<watch::Sender<()>>,
    status: Arc<Status>,
}

pub fn new(conf: ClientConf) -> Box<dyn Client> {
    Box::new(MqttClientV50 {
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

impl MqttClientV50 {
    pub async fn start(&mut self) {
        if self.running {
            return;
        } else {
            self.running = true;
        }

        let mut mqtt_options =
            v5::MqttOptions::new(self.conf.id.clone(), self.conf.host.clone(), self.conf.port);
        mqtt_options.set_keep_alive(Duration::from_secs(self.conf.keep_alive));
        match (&self.conf.username, &self.conf.password) {
            (Some(username), Some(password)) => {
                mqtt_options.set_credentials(username.clone(), password.clone());
            }
            (None, Some(password)) => {
                mqtt_options.set_credentials("", password.clone());
            }
            (Some(username), None) => {
                mqtt_options.set_credentials(username.clone(), "");
            }
            _ => {}
        }

        let (stop_signal_tx, mut stop_signal_rx) = watch::channel(());
        let (client, mut eventloop) = v5::AsyncClient::new(mqtt_options, 8);
        self.client = Some(client);
        self.stop_signal_tx = Some(stop_signal_tx);
        let status = self.status.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return;
                    }

                    event = eventloop.poll() => {
                        Self::handle_event(&status, event);
                    }
                }
            }
        });

        for publish in self.publishes.iter() {
            publish.start(self.client.clone().unwrap());
        }

        for subscribe in self.subscribes.iter() {
            subscribe.start(self.client.as_ref().unwrap()).await;
        }
    }

    fn handle_event(status: &Arc<Status>, res: Result<v5::Event, v5::ConnectionError>) {
        match res {
            Ok(event) => status.handle_v50_event(event),
            Err(_) => todo!(),
        }
    }

    pub fn stop(&mut self) {
        if !self.running {
            return;
        } else {
            self.running = false;
        }
        if let Some(stop_signal_tx) = &self.stop_signal_tx {
            stop_signal_tx.send(()).unwrap();
        }
    }

    pub fn running(&self) -> bool {
        self.err.is_none()
    }

    // 获取状态并重置统计数据
    pub fn get_status(&self) -> ClientStatus {
        let success = self.err.is_none();
        ClientStatus {
            success,
            conn_ack: self.status.conn_ack.swap(0, Ordering::SeqCst),
            pub_ack: self.status.pub_ack.swap(0, Ordering::SeqCst),
            unsub_ack: self.status.unsub_ack.swap(0, Ordering::SeqCst),
            ping_req: self.status.ping_req.swap(0, Ordering::SeqCst),
            ping_resp: self.status.ping_resp.swap(0, Ordering::SeqCst),
            publish: self.status.publish.swap(0, Ordering::SeqCst),
            subscribe: self.status.subscribe.swap(0, Ordering::SeqCst),
            unsubscribe: self.status.unsubscribe.swap(0, Ordering::SeqCst),
            disconnect: self.status.disconnect.swap(0, Ordering::SeqCst),
        }
    }

    pub fn get_err_info(&self) -> Result<(), String> {
        if self.err.is_some() {
            Err(self.err.clone().unwrap())
        } else {
            Ok(())
        }
    }

    pub fn create_publish(&mut self, req: Arc<PublishCreateUpdateReq>) {
        let publish = Publish::new(generate_id(), req);
        if let Some(client) = &self.client {
            publish.start(client.clone());
        }
        self.publishes.push(publish);
    }

    pub async fn create_subscribe(&mut self, req: Arc<SubscribeCreateUpdateReq>) {
        let subscribe = Subscribe::new(generate_id(), req);
        if let Some(client) = &self.client {
            subscribe.start(&client).await;
        }
        self.subscribes.push(subscribe);
    }
}

pub struct Publish {
    pub id: String,
    pub running: bool,
    pub conf: Arc<PublishCreateUpdateReq>,
}

impl Publish {
    pub fn new(id: String, conf: Arc<PublishCreateUpdateReq>) -> Self {
        Self {
            id,
            running: false,
            conf,
        }
    }

    pub fn start(&self, client: v5::AsyncClient) {
        if self.running {
            return;
        }

        let conf = self.conf.clone();
        tokio::spawn(async move {
            let qos = get_v50_qos(&conf.qos);
            let mut interval = tokio::time::interval(Duration::from_millis(conf.interval));
            loop {
                interval.tick().await;
                let _ = client
                    .publish(conf.topic.clone(), qos, conf.retain, conf.payload.clone())
                    .await;
            }
        });
    }

    pub fn stop(&self) {
        todo!()
    }
}

pub struct Subscribe {
    pub id: String,
    pub conf: Arc<SubscribeCreateUpdateReq>,
}

impl Subscribe {
    pub fn new(id: String, conf: Arc<SubscribeCreateUpdateReq>) -> Self {
        Self { id, conf }
    }

    pub async fn start(&self, client: &v5::AsyncClient) {
        let qos = get_v50_qos(&self.conf.qos);
        client.subscribe(&self.conf.topic, qos).await.unwrap();
    }

    pub async fn stop(&self, client: &v5::AsyncClient) {
        client.unsubscribe(&self.conf.topic).await.unwrap();
    }
}

#[async_trait]
impl Client for MqttClientV50 {
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
