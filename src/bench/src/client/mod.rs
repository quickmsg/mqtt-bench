use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use mqtt::protocol::v3_mini::v4::Subscribe;
use types::{ClientsListRespItem, PublishConf, Status, SubscribeCreateUpdateReq};

use crate::group::ClientGroupConf;

pub mod mqtt_v311;
pub mod mqtt_v50;
mod ssl;
mod ssl_new;
mod v311;
mod v50;
pub mod websocket_v311;
pub mod websocket_v50;

#[async_trait]
pub trait Client: Sync + Send {
    async fn start(&mut self);
    async fn stop(&mut self);
    async fn update(&mut self, group_conf: Arc<ClientGroupConf>);
    fn update_status(&mut self, status: Status);

    fn create_publish(&mut self, id: Arc<String>, req: Arc<PublishConf>);
    fn update_publish(&mut self, id: &String, req: Arc<PublishConf>);
    fn delete_publish(&mut self, id: &String);

    async fn publish(
        &self,
        topic: String,
        qos: mqtt::protocol::v3_mini::QoS,
        payload: Arc<Bytes>,
        pkid: u16,
    );

    async fn subscribe(&self, sub: Subscribe);

    async fn create_subscribe(&mut self, id: Arc<String>, req: Arc<SubscribeCreateUpdateReq>);
    async fn update_subscribe(&mut self, subscribe_id: &String, req: Arc<SubscribeCreateUpdateReq>);
    async fn delete_subscribe(&mut self, subscribe_id: &String);

    async fn read(&self) -> ClientsListRespItem;
}

pub struct ClientConf {
    pub host: String,
    pub keep_alive: u16,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub local_ip: Option<String>,
}

#[macro_export]
macro_rules! stop {
    ($self:expr) => {
        match $self.status {
            Status::Running => {
                for publish in $self.publishes.iter_mut() {
                    publish.stop();
                }

                for subscribe in $self.subscribes.iter_mut() {
                    subscribe.stop($self.client.as_ref().unwrap()).await;
                }

                _ = $self.client.as_ref().unwrap().disconnect().await;

                if let Some(stop_signal_tx) = &$self.stop_signal_tx {
                    stop_signal_tx.send(()).unwrap();
                }
            }
            _ => {}
        }
    };
}

#[macro_export]
macro_rules! update {
    ($self:expr, $group_conf:expr) => {
        $self.group_conf = $group_conf;
    };
}

#[macro_export]
macro_rules! update_status {
    ($self:expr, $status:expr) => {
        match (&$self.status, &$status) {
            (Status::Running, Status::Stopped) => {
                $self
                    .client_metrics
                    .running_cnt
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                $self
                    .client_metrics
                    .stopped_cnt
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            (Status::Running, Status::Waiting) => {
                $self
                    .client_metrics
                    .running_cnt
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                $self
                    .client_metrics
                    .waiting_cnt
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            (Status::Stopped, Status::Running) => {
                $self
                    .client_metrics
                    .stopped_cnt
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                $self
                    .client_metrics
                    .running_cnt
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            (Status::Stopped, Status::Waiting) => {
                $self
                    .client_metrics
                    .stopped_cnt
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                $self
                    .client_metrics
                    .waiting_cnt
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            (Status::Waiting, Status::Running) => {
                $self
                    .client_metrics
                    .waiting_cnt
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                $self
                    .client_metrics
                    .running_cnt
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            (Status::Waiting, Status::Stopped) => {
                $self
                    .client_metrics
                    .waiting_cnt
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                $self
                    .client_metrics
                    .stopped_cnt
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            (Status::Waiting, Status::Waiting) => {}
            _ => unreachable!(),
        }
        $self.status = $status;
    };
}

#[macro_export]
macro_rules! create_publish {
    ($self:expr, $id:expr, $req:expr) => {
        let publish = Publish::new($id, $req);
        $self.publishes.push(publish);
    };
}

#[macro_export]
macro_rules! update_publish {
    ($self:expr, $id:expr, $req:expr) => {
        let publish = $self
            .publishes
            .iter_mut()
            .find(|publish| *publish.id == *$id)
            .unwrap();
        publish.conf = $req;
    };
}

#[macro_export]
macro_rules! delete_publish {
    ($self:expr, $id:expr) => {
        $self.publishes.retain(|publish| *publish.id != *$id);
    };
}

#[macro_export]
macro_rules! create_subscribe {
    ($self:expr, $id:expr, $req:expr) => {
        let subscribe = Subscribe::new($id, $req);
        $self.subscribes.push(subscribe);
    };
}

#[macro_export]
macro_rules! update_subscribe {
    ($self:expr, $id:expr, $req:expr) => {
        let subscribe = $self
            .subscribes
            .iter_mut()
            .find(|subscribe| *subscribe.id == *$id)
            .unwrap();
        subscribe.conf = $req;
    };
}

#[macro_export]
macro_rules! delete_subscribe {
    ($self:expr, $id:expr) => {
        $self.subscribes.retain(|subscribe| *subscribe.id != *$id);
    };
}

#[macro_export]
macro_rules! read {
    ($self:expr) => {{
        let err = match &$self.err {
            Some(err) => {
                let err = err.lock().await;
                match &*err {
                    Some(err) => Some(err.clone()),
                    None => None,
                }
            }
            None => None,
        };
        types::ClientsListRespItem {
            client_id: $self.client_conf.client_id.clone(),
            status: $self.status,
            addr: $self.client_conf.host.clone(),
            err,
        }
    }};
}
