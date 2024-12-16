use std::{sync::Arc, time::Duration};

use rumqttc::AsyncClient;
use tokio::{select, sync::watch};
use types::{PublishCreateUpdateReq, SubscribeCreateUpdateReq};

fn get_qos(qos: &types::Qos) -> rumqttc::QoS {
    match qos {
        types::Qos::AtMostOnce => rumqttc::QoS::AtMostOnce,
        types::Qos::AtLeastOnce => rumqttc::QoS::AtLeastOnce,
        types::Qos::ExactlyOnce => rumqttc::QoS::ExactlyOnce,
    }
}

pub struct Publish {
    pub conf: Arc<PublishCreateUpdateReq>,
    stop_signal_tx: watch::Sender<()>,
}

impl Publish {
    pub fn new(conf: Arc<PublishCreateUpdateReq>) -> Self {
        let (stop_signal_tx, _) = watch::channel(());
        Self {
            conf,
            stop_signal_tx,
        }
    }

    pub fn start(&self, client: AsyncClient) {
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

    pub fn stop(&self) {
        self.stop_signal_tx.send(()).unwrap();
    }
}

pub struct Subscribe {
    pub conf: Arc<SubscribeCreateUpdateReq>,
}

impl Subscribe {
    pub fn new(conf: Arc<SubscribeCreateUpdateReq>) -> Self {
        Self { conf }
    }

    pub async fn start(&self, client: &AsyncClient) {
        let qos = get_qos(&self.conf.qos);
        client.subscribe(&self.conf.topic, qos).await.unwrap();
    }

    pub async fn stop(&self, client: &AsyncClient) {
        client.unsubscribe(&self.conf.topic).await.unwrap();
    }
}
