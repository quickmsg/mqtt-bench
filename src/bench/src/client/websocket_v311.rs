use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::lock::BiLock;
use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions, Transport};
use tokio::{select, sync::watch};
use types::{ClientsListRespItem, PublishCreateUpdateReq, SubscribeCreateUpdateReq};

use crate::{group::ClientGroupConf, AtomicMetrics, ErrorManager};

use super::{
    ssl::get_ssl_config,
    v311::{Publish, Subscribe},
    Client, ClientConf, ClientMetrics,
};

pub struct WebsocketClientV311 {
    running: bool,
    client_conf: ClientConf,
    group_conf: Arc<ClientGroupConf>,
    client: Option<AsyncClient>,
    err: Option<BiLock<Option<String>>>,
    publishes: Vec<Publish>,
    subscribes: Vec<Subscribe>,
    stop_signal_tx: Option<watch::Sender<()>>,
    metrics: Arc<AtomicMetrics>,
}

pub fn new(client_conf: ClientConf, group_conf: Arc<ClientGroupConf>) -> Box<dyn Client> {
    Box::new(WebsocketClientV311 {
        running: false,
        client_conf,
        group_conf,
        client: None,
        err: None,
        publishes: vec![],
        subscribes: vec![],
        stop_signal_tx: None,
        metrics: Arc::new(AtomicMetrics::default()),
    })
}

impl WebsocketClientV311 {
    async fn handle_event(
        metrics: &Arc<AtomicMetrics>,
        res: Result<Event, ConnectionError>,
        error_manager: &mut ErrorManager,
    ) {
        match res {
            Ok(event) => {
                metrics.handle_v311_event(event);
                error_manager.put_ok().await;
            }
            Err(e) => {
                error_manager.put_err(e.to_string()).await;
            }
        }
    }
}

#[async_trait]
impl Client for WebsocketClientV311 {
    async fn start(&mut self) {
        if self.running {
            return;
        } else {
            self.running = true;
        }

        let mut mqtt_options = match &&self.group_conf.ssl_conf {
            Some(ssl_conf) => {
                let mut mqtt_options = MqttOptions::new(
                    self.client_conf.id.clone(),
                    format!(
                        "wss://{}:{}/mqtt",
                        self.client_conf.host, self.group_conf.port
                    ),
                    self.group_conf.port,
                );
                let config = get_ssl_config(ssl_conf);
                let transport =
                    rumqttc::Transport::Wss(rumqttc::TlsConfiguration::Rustls(Arc::new(config)));
                mqtt_options.set_transport(transport);
                mqtt_options
            }
            None => {
                let mut mqtt_options = MqttOptions::new(
                    self.client_conf.id.clone(),
                    format!(
                        "ws://{}:{}/mqtt",
                        self.client_conf.host, self.group_conf.port
                    ),
                    self.group_conf.port,
                );
                mqtt_options.set_transport(Transport::Ws);
                mqtt_options
            }
        };

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
        self.client = Some(client);
        self.stop_signal_tx = Some(stop_signal_tx);
        let metrics = self.metrics.clone();

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
                        Self::handle_event(&metrics, event, &mut error_manager).await;
                    }
                }
            }
        });

        for publish in self.publishes.iter_mut() {
            publish.start(self.client.clone().unwrap());
        }

        for subscribe in self.subscribes.iter_mut() {
            subscribe.start(self.client.as_ref().unwrap()).await;
        }
    }

    async fn stop(&mut self) {
        if !self.running {
            return;
        } else {
            self.running = false;
        }

        for publish in self.publishes.iter_mut() {
            publish.stop();
        }

        for subscribe in self.subscribes.iter_mut() {
            subscribe.stop(self.client.as_ref().unwrap()).await;
        }

        _ = self.client.as_ref().unwrap().disconnect().await;

        if let Some(stop_signal_tx) = &self.stop_signal_tx {
            stop_signal_tx.send(()).unwrap();
        }
    }

    fn get_metrics(&self) -> ClientMetrics {
        let success = self.err.is_none();
        let usize_metrics = self.metrics.take_metrics();
        ClientMetrics {
            success,
            usize_metrics,
        }
    }

    async fn update(&mut self, group_conf: Arc<ClientGroupConf>) {
        self.group_conf = group_conf;
        if self.running {
            self.stop().await;
            self.start().await;
        }
    }

    fn create_publish(&mut self, id: Arc<String>, req: Arc<PublishCreateUpdateReq>) {
        let mut publish = Publish::new(id, req);
        if let Some(client) = &self.client {
            publish.start(client.clone());
        }
        self.publishes.push(publish);
    }

    fn update_publish(&mut self, id: &String, req: Arc<PublishCreateUpdateReq>) {
        let publish = self
            .publishes
            .iter_mut()
            .find(|publish| *publish.id == *id)
            .unwrap();
        publish.conf = req;
        if let Some(client) = &self.client {
            publish.stop();
            publish.start(client.clone());
        }
    }

    fn delete_publish(&mut self, id: &String) {
        let publish = self
            .publishes
            .iter_mut()
            .find(|publish| *publish.id == *id)
            .unwrap();
        publish.stop();
        self.publishes.retain(|publish| *publish.id != *id);
    }

    async fn create_subscribe(&mut self, id: Arc<String>, req: Arc<SubscribeCreateUpdateReq>) {
        let mut subscribe = Subscribe::new(id, req);
        if let Some(client) = &self.client {
            subscribe.start(&client).await;
        }
        self.subscribes.push(subscribe);
    }

    async fn update_subscribe(
        &mut self,
        subscribe_id: &String,
        conf: Arc<SubscribeCreateUpdateReq>,
    ) {
        let subscribe = self
            .subscribes
            .iter_mut()
            .find(|subscribe| *subscribe.id == *subscribe_id)
            .unwrap();
        subscribe.conf = conf;
        match self.client.as_ref() {
            Some(client) => {
                subscribe.stop(client).await;
                subscribe.start(client).await;
            }
            None => return,
        }
    }

    async fn delete_subscribe(&mut self, subscribe_id: &String) {
        let subscribe = self
            .subscribes
            .iter_mut()
            .find(|subscribe| *subscribe.id == *subscribe_id)
            .unwrap();
        if let Some(client) = &self.client {
            subscribe.stop(client).await;
        }
        self.subscribes
            .retain(|subscribe| *subscribe.id != *subscribe_id);
    }

    async fn read(&self) -> ClientsListRespItem {
        ClientsListRespItem {
            client_id: todo!(),
            status: todo!(),
            addr: todo!(),
            err: todo!(),
        }
    }
}
