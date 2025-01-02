use std::{sync::Arc, time::Duration};

use rumqttc::AsyncClient;
use tokio::{select, sync::watch};
use tracing::warn;
use types::{PublishConf, SubscribeCreateUpdateReq};

fn get_qos(qos: &types::Qos) -> rumqttc::QoS {
    match qos {
        types::Qos::AtMostOnce => rumqttc::QoS::AtMostOnce,
        types::Qos::AtLeastOnce => rumqttc::QoS::AtLeastOnce,
        types::Qos::ExactlyOnce => rumqttc::QoS::ExactlyOnce,
    }
}

#[derive(Debug)]
pub struct Publish {
    running: bool,
    pub id: Arc<String>,
    pub conf: Arc<PublishConf>,
    stop_signal_tx: watch::Sender<()>,
}

impl Publish {
    pub fn new(id: Arc<String>, conf: Arc<PublishConf>) -> Self {
        let (stop_signal_tx, _) = watch::channel(());
        Self {
            running: false,
            id,
            conf,
            stop_signal_tx,
        }
    }

    pub fn start(&mut self, client: AsyncClient) {
        if self.running {
            warn!("Publish is already running");
            return;
        } else {
            self.running = true;
        }

        let conf = self.conf.clone();
        let mut stop_signal_rx = self.stop_signal_tx.subscribe();
        tokio::spawn(async move {
            let qos = get_qos(&conf.qos);
            let mut interval = tokio::time::interval(Duration::from_millis(conf.interval));
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        break;
                    }

                    _ = interval.tick() => {
                        let _ = client
                            .publish(conf.topic.clone(), qos, conf.retain, conf.payload.clone())
                            .await;
                    }
                }
            }
        });
    }

    pub fn stop(&mut self) {
        if !self.running {
            warn!("Publish is not running");
            return;
        } else {
            self.running = false;
            self.stop_signal_tx.send(()).unwrap();
        }
    }
}

pub struct Subscribe {
    running: bool,
    pub id: Arc<String>,
    pub conf: Arc<SubscribeCreateUpdateReq>,
}

impl Subscribe {
    pub fn new(id: Arc<String>, conf: Arc<SubscribeCreateUpdateReq>) -> Self {
        Self {
            running: false,
            id,
            conf,
        }
    }

    pub async fn start(&mut self, client: &AsyncClient) {
        if self.running {
            warn!("Subscribe is already running");
            return;
        } else {
            self.running = true;
        }

        let qos = get_qos(&self.conf.qos);
        client.subscribe(&self.conf.topic, qos).await.unwrap();
    }

    pub async fn stop(&mut self, client: &AsyncClient) {
        if !self.running {
            warn!("Subscribe is not running");
            return;
        } else {
            self.running = false;
            client.unsubscribe(&self.conf.topic).await.unwrap();
        }
    }
}
