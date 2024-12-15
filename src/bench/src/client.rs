use std::{sync::Arc, time::Duration};

use rumqttc::{AsyncClient, MqttOptions};
use tokio::{select, sync::watch};
use types::PublishCreateUpdateReq;

use crate::generate_id;

pub struct ClientConf {
    pub index: usize,
    pub id: String,
    pub host: String,
    pub port: u16,
    pub keep_alive: u64,
    pub username: Option<String>,
    pub password: Option<String>,
}

pub struct ClientV311 {
    running: bool,
    conf: ClientConf,
    client: Option<AsyncClient>,
    err: Option<String>,
    publishes: Vec<Publish>,
    stop_signal_tx: Option<watch::Sender<()>>,
}

impl ClientV311 {
    pub fn new(conf: ClientConf) -> Self {
        Self {
            running: false,
            conf,
            client: None,
            err: None,
            publishes: vec![],
            stop_signal_tx: None,
        }
    }

    pub fn start(&mut self) {
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
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return;
                    }
                    Ok(event) = eventloop.poll() => {
                        println!("Received = {:?}", event);
                    }
                }
            }
        });
    }

    pub async fn stop(&mut self) {
        if let Some(stop_signal_tx) = &self.stop_signal_tx {
            stop_signal_tx.send(()).unwrap();
        }
    }

    pub fn get_status(&self) -> bool {
        self.err.is_none()
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
