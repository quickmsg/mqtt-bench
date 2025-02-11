use std::sync::{
    atomic::{AtomicU16, AtomicU8},
    Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::lock::BiLock;
use mqtt::{
    protocol::v3_mini::v4::{Packet, Publish},
    AsyncClient, MqttOptions,
};
use tokio::sync::{watch, RwLock};
use types::{
    group::{ClientAtomicMetrics, PacketAtomicMetrics},
    ClientsListRespItem, Status,
};

use crate::{group::ClientGroupConf, update};

use super::{ssl_new::get_ssl_config, v311::Subscribe, Client, ClientConf};

pub struct MqttClientV311 {
    status: AtomicU8,
    client_conf: ClientConf,
    group_conf: Arc<ClientGroupConf>,
    client: RwLock<Option<AsyncClient>>,
    err: Option<BiLock<Option<String>>>,
    stop_signal_tx: Option<watch::Sender<()>>,
    client_metrics: Arc<ClientAtomicMetrics>,
    packet_metrics: Arc<PacketAtomicMetrics>,
    pkid: AtomicU16,
}

pub fn new(
    client_conf: ClientConf,
    group_conf: Arc<ClientGroupConf>,
    client_metrics: Arc<ClientAtomicMetrics>,
    packet_metrics: Arc<PacketAtomicMetrics>,
) -> Box<dyn Client> {
    Box::new(MqttClientV311 {
        status: AtomicU8::new(Status::Stopped as u8),
        client_conf,
        group_conf,
        client: RwLock::new(None),
        err: None,
        stop_signal_tx: None,
        client_metrics,
        packet_metrics,
        pkid: AtomicU16::new(1),
    })
}

impl MqttClientV311 {
    fn get_pkid(&self) -> u16 {
        let mut pkid = self.pkid.load(std::sync::atomic::Ordering::SeqCst);
        if pkid == u16::MAX {
            self.pkid.store(1, std::sync::atomic::Ordering::SeqCst);
            pkid = 1;
        } else {
            self.pkid.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            pkid += 1;
        }
        todo!()
    }
}

#[async_trait]
impl Client for MqttClientV311 {
    async fn start(&self) {
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

        let client = AsyncClient::new(
            self.client_metrics.clone(),
            mqtt_options,
            self.packet_metrics.clone(),
        )
        .await;
        self.client.write().await.replace(client);
    }

    async fn publish(&self, topic: String, qos: u8, payload: Arc<Bytes>) {
        let qos = match qos {
            0 => mqtt::protocol::v3_mini::QoS::AtMostOnce,
            1 => mqtt::protocol::v3_mini::QoS::AtLeastOnce,
            2 => mqtt::protocol::v3_mini::QoS::ExactlyOnce,
            _ => unreachable!(),
        };

        let mut packet = mqtt::protocol::v3_mini::v4::Publish::new(topic, qos, payload);
        match qos {
            mqtt::protocol::v3_mini::QoS::AtMostOnce => {}
            mqtt::protocol::v3_mini::QoS::AtLeastOnce
            | mqtt::protocol::v3_mini::QoS::ExactlyOnce => {
                let pkid = self.get_pkid();
                packet.set_pkid(pkid);
            }
        }

        self.client
            .read()
            .await
            .as_ref()
            .unwrap()
            .send(Packet::Publish(packet));
    }

    // fn subscribe(&self, sub: mqtt::protocol::v3_mini::v4::Subscribe) {
    //     debug!("client subscirbe");
    //     // match &self.client {
    //     //     Some(client) => {
    //     //         client.send(Packet::Subscribe(sub));
    //     //     }
    //     //     None => {}
    //     // }
    // }

    async fn stop(&self) {
        // stop!(self);
    }

    async fn update(&mut self, group_conf: Arc<ClientGroupConf>) {
        update!(self, group_conf);
    }

    fn update_status(&self, status: Status) {
        self.status
            .store(status as u8, std::sync::atomic::Ordering::SeqCst);
    }

    // fn create_publish(&mut self, id: Arc<String>, req: Arc<PublishConf>) {
    //     create_publish!(self, id, req);
    // }

    // fn update_publish(&mut self, id: &String, req: Arc<PublishConf>) {
    //     update_publish!(self, id, req);
    // }

    // fn delete_publish(&mut self, id: &String) {
    //     delete_publish!(self, id);
    // }

    // async fn create_subscribe(&mut self, id: Arc<String>, req: Arc<SubscribeCreateUpdateReq>) {
    //     create_subscribe!(self, id, req);
    // }

    // async fn update_subscribe(
    //     &mut self,
    //     subscribe_id: &String,
    //     conf: Arc<SubscribeCreateUpdateReq>,
    // ) {
    //     update_subscribe!(self, subscribe_id, conf);
    // }

    // async fn delete_subscribe(&mut self, subscribe_id: &String) {
    //     delete_subscribe!(self, subscribe_id);
    // }

    async fn read(&self) -> ClientsListRespItem {
        todo!()
        // read!(self)
    }
}
