use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use async_trait::async_trait;
use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions};
use tokio::{select, sync::watch};
use types::{GroupCreateUpdateReq, PublishCreateUpdateReq, SubscribeCreateUpdateReq};

use crate::Status;

use super::{
    ssl::get_ssl_config,
    v311::{Publish, Subscribe},
    Client, ClientConf, ClientStatus,
};

pub struct MqttClientV311 {
    running: bool,
    client_conf: ClientConf,
    group_conf: Arc<GroupCreateUpdateReq>,
    client: Option<AsyncClient>,
    err: Option<String>,
    publishes: Vec<Publish>,
    subscribes: Vec<Subscribe>,
    stop_signal_tx: Option<watch::Sender<()>>,
    status: Arc<Status>,
}

pub fn new(client_conf: ClientConf, group_conf: Arc<GroupCreateUpdateReq>) -> Box<dyn Client> {
    Box::new(MqttClientV311 {
        running: false,
        client_conf,
        group_conf,
        client: None,
        err: None,
        publishes: vec![],
        subscribes: vec![],
        stop_signal_tx: None,
        status: Arc::new(Status::default()),
    })
}

impl MqttClientV311 {
    fn handle_event(status: &Arc<Status>, res: Result<Event, ConnectionError>) {
        match res {
            Ok(event) => status.handle_v311_event(event),
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }

    // pub fn get_err_info(&self) -> Result<(), String> {
    //     if self.err.is_some() {
    //         Err(self.err.clone().unwrap())
    //     } else {
    //         Ok(())
    //     }
    // }
}

#[async_trait]
impl Client for MqttClientV311 {
    async fn start(&mut self) {
        if self.running {
            return;
        } else {
            self.running = true;
        }

        let mut mqtt_options = MqttOptions::new(
            self.client_conf.id.clone(),
            self.client_conf.host.clone(),
            self.client_conf.port,
        );

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

    async fn stop(&mut self) {
        if !self.running {
            return;
        } else {
            self.running = false;
        }

        for publish in self.publishes.iter() {
            publish.stop();
        }

        for subscribe in self.subscribes.iter() {
            subscribe.stop(self.client.as_ref().unwrap()).await;
        }

        _ = self.client.as_ref().unwrap().disconnect().await;

        if let Some(stop_signal_tx) = &self.stop_signal_tx {
            stop_signal_tx.send(()).unwrap();
        }
    }

    fn get_status(&self) -> ClientStatus {
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

    fn create_publish(&mut self, req: Arc<PublishCreateUpdateReq>) {
        let publish = Publish::new(req);
        if let Some(client) = &self.client {
            publish.start(client.clone());
        }
        self.publishes.push(publish);
    }

    async fn create_subscribe(&mut self, req: Arc<SubscribeCreateUpdateReq>) {
        let subscribe = Subscribe::new(req);
        if let Some(client) = &self.client {
            subscribe.start(&client).await;
        }
        self.subscribes.push(subscribe);
    }
}
