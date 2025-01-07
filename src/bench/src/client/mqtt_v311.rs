use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::lock::BiLock;
use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions};
use tokio::{select, sync::watch};
use tracing::{debug, warn};
use types::{ClientsListRespItem, PublishConf, Status, SubscribeCreateUpdateReq};

use crate::{
    create_publish, create_subscribe, delete_publish, delete_subscribe, group::ClientGroupConf,
    read, stop, update, update_publish, update_status, update_subscribe, ClientAtomicMetrics,
    ErrorManager, PacketAtomicMetrics,
};

use super::{
    ssl::get_ssl_config,
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

impl MqttClientV311 {
    async fn handle_event(
        packet_metrics: &Arc<PacketAtomicMetrics>,
        res: Result<Event, ConnectionError>,
        error_manager: &mut ErrorManager,
    ) -> bool {
        match res {
            Ok(event) => {
                packet_metrics.handle_v311_event(event);
                error_manager.put_ok().await;
                true
            }
            Err(e) => {
                warn!("客户端连接错误：{:?}", e);
                error_manager.put_err(e.to_string()).await;
                false
            }
        }
    }
}

#[async_trait]
impl Client for MqttClientV311 {
    async fn start(&mut self) {
        self.update_status(Status::Running);
        let mut mqtt_options = MqttOptions::new(
            self.client_conf.id.clone(),
            self.client_conf.host.clone(),
            self.group_conf.port,
        );

        mqtt_options.set_max_packet_size(20240, 20240);

        if let Some(ssl_conf) = &self.group_conf.ssl_conf {
            let config = get_ssl_config(ssl_conf);
            let transport =
                rumqttc::Transport::Tls(rumqttc::TlsConfiguration::Rustls(Arc::new(config)));
            mqtt_options.set_transport(transport);
        }

        mqtt_options.set_keep_alive(Duration::from_secs(self.client_conf.keep_alive));
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

        let (stop_signal_tx, mut stop_signal_rx) = watch::channel(());
        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 8);

        if let Some(local_ip) = &self.client_conf.local_ip {
            eventloop.network_options.set_local_ip(local_ip);
        }

        self.client = Some(client);
        self.stop_signal_tx = Some(stop_signal_tx);

        let packet_metrics = self.packet_metrics.clone();

        let (err1, err2) = BiLock::new(None);
        self.err = Some(err1);

        tokio::spawn(async move {
            let mut error_manager = ErrorManager::new(err2);
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return;
                    }

                    event = eventloop.poll() => {
                        if !Self::handle_event(&packet_metrics, event, &mut error_manager).await {
                            return;
                        }
                    }
                }
            }
        });

        // for publish in self.publishes.iter_mut() {
        //     publish.start(self.client.clone().unwrap());
        // }

        for subscribe in self.subscribes.iter_mut() {
            subscribe.start(self.client.as_ref().unwrap()).await;
        }
    }

    async fn publish(&self, topic: String, qos: rumqttc::QoS, payload: Arc<Vec<u8>>) {
        self.client
            .as_ref()
            .unwrap()
            .publish(topic.clone(), qos, false, payload)
            .await
            .unwrap();
    }

    async fn stop(&mut self) {
        stop!(self);
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
