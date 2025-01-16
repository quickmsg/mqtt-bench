use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use futures::lock::BiLock;
use mqtt::{protocol::v3_mini::v4::Packet, AsyncClient, MqttOptions};
use tokio::{select, sync::watch};
use tracing::{debug, error, warn};
use types::{
    group::{ClientAtomicMetrics, PacketAtomicMetrics},
    ClientsListRespItem, PublishConf, Status, SubscribeCreateUpdateReq,
};

use crate::{
    create_publish, create_subscribe, delete_publish, delete_subscribe, group::ClientGroupConf,
    read, stop, update, update_publish, update_status, update_subscribe,
};

use super::{
    ssl_new::get_ssl_config,
    v311::{Publish, Subscribe},
    Client, ClientConf,
};

pub struct MqttClientV311 {
    status: Status,
    client_conf: ClientConf,
    group_conf: Arc<ClientGroupConf>,
    client: Option<AsyncClient>,
    err: Option<BiLock<Option<String>>>,
    publishes: Vec<Publish>,
    subscribes: Vec<Subscribe>,
    stop_signal_tx: Option<watch::Sender<()>>,
    client_metrics: Arc<ClientAtomicMetrics>,
    packet_metrics: Arc<PacketAtomicMetrics>,
}

pub fn new(
    client_conf: ClientConf,
    group_conf: Arc<ClientGroupConf>,
    client_metrics: Arc<ClientAtomicMetrics>,
    packet_metrics: Arc<PacketAtomicMetrics>,
) -> Box<dyn Client> {
    Box::new(MqttClientV311 {
        status: Status::Stopped,
        client_conf,
        group_conf,
        client: None,
        err: None,
        publishes: vec![],
        subscribes: vec![],
        stop_signal_tx: None,
        client_metrics,
        packet_metrics,
    })
}

#[async_trait]
impl Client for MqttClientV311 {
    async fn start(&mut self) {
        self.update_status(Status::Running);
        let mut mqtt_options = MqttOptions::new(
            self.client_conf.client_id.clone(),
            self.client_conf.host.clone(),
            self.group_conf.port,
        );

        mqtt_options.set_max_packet_size(20240, 20240);

        if let Some(ssl_conf) = &self.group_conf.ssl_conf {
            let config = get_ssl_config(ssl_conf);
            let transport = mqtt::Transport::Tls(mqtt::TlsConfiguration::Rustls(Arc::new(config)));
            mqtt_options.set_transport(transport);
        }

        mqtt_options.set_keep_alive(self.client_conf.keep_alive);

        match (&self.client_conf.username, &self.client_conf.password) {
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

        if let Some(local_ip) = &self.client_conf.local_ip {
            mqtt_options.set_local_ip(local_ip);
        }

        // let (stop_signal_tx, mut stop_signal_rx) = watch::channel(());
        let client = AsyncClient::new(
            self.client_metrics.clone(),
            mqtt_options,
            1024,
            self.packet_metrics.clone(),
        )
        .await;
        //     Ok(client) => client,
        //     Err(e) => {
        //         self.client_metrics
        //             .running_cnt
        //             .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        //         error!("客户端连接错误: {:?}", e);
        //         return;
        //     }
        // };

        self.client = Some(client);
        // self.stop_signal_tx = Some(stop_signal_tx);

        // let packet_metrics = self.packet_metrics.clone();

        // let (err1, err2) = BiLock::new(None);
        // self.err = Some(err1);

        // for publish in self.publishes.iter_mut() {
        //     publish.start(self.client.clone().unwrap());
        // }

        // for subscribe in self.subscribes.iter_mut() {
        //     subscribe.start(self.client.as_ref().unwrap()).await;
        // }
    }

    async fn publish(
        &self,
        topic: String,
        qos: mqtt::protocol::v3_mini::QoS,
        payload: Arc<Bytes>,
        pkid: u16,
    ) {
        match &self.client {
            Some(client) => {
                // debug!("client publish");
                let packet = mqtt::protocol::v3_mini::v4::Publish::new(topic, qos, payload, pkid);
                client.publish(Packet::Publish(packet)).await;
            }
            None => {}
        }
    }

    async fn subscribe(&self, sub: mqtt::protocol::v3_mini::v4::Subscribe) {
        debug!("client subscirbe");
        match &self.client {
            Some(client) => {
                client.subscribe(sub).await;
            }
            None => {}
        }
    }

    async fn stop(&mut self) {
        // stop!(self);
    }

    async fn update(&mut self, group_conf: Arc<ClientGroupConf>) {
        update!(self, group_conf);
    }

    fn update_status(&mut self, status: Status) {
        update_status!(self, status);
    }

    fn create_publish(&mut self, id: Arc<String>, req: Arc<PublishConf>) {
        create_publish!(self, id, req);
    }

    fn update_publish(&mut self, id: &String, req: Arc<PublishConf>) {
        update_publish!(self, id, req);
    }

    fn delete_publish(&mut self, id: &String) {
        delete_publish!(self, id);
    }

    async fn create_subscribe(&mut self, id: Arc<String>, req: Arc<SubscribeCreateUpdateReq>) {
        create_subscribe!(self, id, req);
    }

    async fn update_subscribe(
        &mut self,
        subscribe_id: &String,
        conf: Arc<SubscribeCreateUpdateReq>,
    ) {
        update_subscribe!(self, subscribe_id, conf);
    }

    async fn delete_subscribe(&mut self, subscribe_id: &String) {
        delete_subscribe!(self, subscribe_id);
    }

    async fn read(&self) -> ClientsListRespItem {
        read!(self)
    }
}
