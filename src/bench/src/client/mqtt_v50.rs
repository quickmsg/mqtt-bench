use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use async_trait::async_trait;
use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions};
use tokio::{select, sync::watch};
use types::{PublishCreateUpdateReq, SubscribeCreateUpdateReq};

use crate::{generate_id, Status};

use super::{Client, ClientConf, ClientStatus};

pub struct ClientV311 {
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
    Box::new(ClientV311 {
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

impl ClientV311 {
    pub async fn start(&mut self) {
        if self.running {
            return;
        } else {
            self.running = true;
        }

        let mut mqtt_options =
            MqttOptions::new(self.conf.id.clone(), self.conf.host.clone(), self.conf.port);
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
        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 8);
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

    fn handle_event(status: &Arc<Status>, event: Result<Event, ConnectionError>) {
        match event {
            Ok(event) => match event {
                Event::Incoming(packet) => match packet {
                    rumqttc::Packet::Connect(connect) => todo!(),
                    rumqttc::Packet::ConnAck(_) => {
                        status.conn_ack.fetch_add(1, Ordering::SeqCst);
                    }
                    rumqttc::Packet::Publish(publish) => todo!(),
                    rumqttc::Packet::PubAck(_) => {
                        status.pub_ack.fetch_add(1, Ordering::SeqCst);
                    }
                    rumqttc::Packet::PubRec(pub_rec) => todo!(),
                    rumqttc::Packet::PubRel(pub_rel) => todo!(),
                    rumqttc::Packet::PubComp(pub_comp) => todo!(),
                    rumqttc::Packet::Subscribe(subscribe) => todo!(),
                    rumqttc::Packet::SubAck(sub_ack) => todo!(),
                    rumqttc::Packet::Unsubscribe(_) => todo!(),
                    rumqttc::Packet::UnsubAck(_) => {
                        status.unsub_ack.fetch_add(1, Ordering::SeqCst);
                    }
                    rumqttc::Packet::PingReq => {}
                    rumqttc::Packet::PingResp => {
                        status.ping_resp.fetch_add(1, Ordering::SeqCst);
                    }
                    rumqttc::Packet::Disconnect => todo!(),
                },
                Event::Outgoing(outgoing) => match outgoing {
                    rumqttc::Outgoing::Publish(_) => {
                        status.publish.fetch_add(1, Ordering::SeqCst);
                    }
                    rumqttc::Outgoing::Subscribe(_) => {
                        status.subscribe.fetch_add(1, Ordering::SeqCst);
                    }
                    rumqttc::Outgoing::Unsubscribe(_) => {
                        status.unsubscribe.fetch_add(1, Ordering::SeqCst);
                    }
                    rumqttc::Outgoing::PubAck(_) => todo!(),
                    rumqttc::Outgoing::PubRec(_) => todo!(),
                    rumqttc::Outgoing::PubRel(_) => todo!(),
                    rumqttc::Outgoing::PubComp(_) => todo!(),
                    rumqttc::Outgoing::PingReq => {
                        status.ping_req.fetch_add(1, Ordering::SeqCst);
                    }
                    rumqttc::Outgoing::PingResp => todo!(),
                    rumqttc::Outgoing::Disconnect => {
                        status.disconnect.fetch_add(1, Ordering::SeqCst);
                    }
                    rumqttc::Outgoing::AwaitAck(_) => todo!(),
                },
            },
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

    pub fn start(&self, client: AsyncClient) {
        if self.running {
            return;
        }

        let conf = self.conf.clone();
        tokio::spawn(async move {
            let qos = match conf.qos {
                types::Qos::AtMostOnce => rumqttc::QoS::AtMostOnce,
                types::Qos::AtLeastOnce => rumqttc::QoS::AtLeastOnce,
                types::Qos::ExactlyOnce => rumqttc::QoS::ExactlyOnce,
            };
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

    pub async fn start(&self, client: &AsyncClient) {
        let qos = match self.conf.qos {
            types::Qos::AtMostOnce => rumqttc::QoS::AtMostOnce,
            types::Qos::AtLeastOnce => rumqttc::QoS::AtLeastOnce,
            types::Qos::ExactlyOnce => rumqttc::QoS::ExactlyOnce,
        };
        client.subscribe(&self.conf.topic, qos).await.unwrap();
    }

    pub async fn stop(&self, client: &AsyncClient) {
        client.unsubscribe(&self.conf.topic).await.unwrap();
    }
}

#[async_trait]
impl Client for ClientV311 {
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
