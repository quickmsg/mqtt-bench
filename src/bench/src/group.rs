use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::SystemTime,
};

use futures::lock::BiLock;
use tokio::{select, sync::RwLock, time};
use types::{
    BrokerUpdateReq, ClientStatus, GroupCreateUpdateReq, GroupStatus, PublishCreateUpdateReq,
    ReadGroupResp, Status, SubscribeCreateUpdateReq,
};

use crate::{client, generate_id};

pub struct Group {
    pub id: String,
    pub conf: Arc<GroupCreateUpdateReq>,
    running: bool,

    clients: Arc<RwLock<Vec<client::ClientV311>>>,
    client_connected_count: Arc<AtomicUsize>,

    status: Option<BiLock<Vec<GroupStatus>>>,
    broker_info: Arc<BrokerUpdateReq>,
    stop_signal_tx: tokio::sync::broadcast::Sender<()>,

    publishes: Vec<Publish>,
}

impl Group {
    pub fn new(id: String, broker_info: Arc<BrokerUpdateReq>, conf: GroupCreateUpdateReq) -> Self {
        let mut clients = Vec::with_capacity(conf.client_count);
        for index in 0..conf.client_count {
            clients.push(client::ClientV311::new(client::ClientConf {
                index,
                id: format!("client-{}", index),
                host: broker_info.addrs[index % broker_info.addrs.len()].0.clone(),
                port: broker_info.addrs[index % broker_info.addrs.len()].1,
                keep_alive: 60,
                username: None,
                password: None,
            }));
        }
        let (stop_signal_tx, _) = tokio::sync::broadcast::channel(1);
        Self {
            id,
            conf: Arc::new(conf),
            running: false,
            clients: Arc::new(RwLock::new(clients)),
            status: None,
            broker_info,
            client_connected_count: Arc::new(AtomicUsize::new(0)),
            publishes: vec![],
            stop_signal_tx,
        }
    }

    pub async fn start(&mut self) {
        if self.running {
            return;
        } else {
            self.running = true;
        }

        let (status1, status2) = BiLock::new(Vec::new());
        self.start_clients();
        self.start_collect_status(status1);

        self.status = Some(status2);
    }

    pub async fn stop(&mut self) {
        if !self.running {
            return;
        } else {
            self.running = false;
        }

        self.clients.write().await.iter_mut().for_each(|client| {
            client.stop();
        });
        self.stop_signal_tx.send(()).unwrap();
    }

    fn start_collect_status(&mut self, status: BiLock<Vec<GroupStatus>>) {
        let mut stop_signal_rx = self.stop_signal_tx.subscribe();
        let mut status_interval = time::interval(time::Duration::from_secs(1));
        let clients = self.clients.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        break;
                    }

                    _ = status_interval.tick() => {
                        Self::collect_status(&clients, &status).await;
                    }
                }
            }
        });
    }

    async fn collect_status(
        clients: &Arc<RwLock<Vec<client::ClientV311>>>,
        group_status: &BiLock<Vec<GroupStatus>>,
    ) {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut succeed = 0;
        let mut failed = 0;
        let mut status = Status::default();
        {
            let clients_guard = clients.read().await;
            clients_guard.iter().for_each(|client| {
                let client_status = client.get_status();
                if client_status.success {
                    succeed += 1;
                } else {
                    failed += 1;
                }
                status.conn_ack += client_status.conn_ack;
                status.pub_ack += client_status.pub_ack;
                status.unsub_ack += client_status.unsub_ack;
                status.ping_req += client_status.ping_req;
                status.ping_resp += client_status.ping_resp;
                status.publish += client_status.publish;
                status.subscribe += client_status.subscribe;
                status.unsubscribe += client_status.unsubscribe;
                status.disconnect += client_status.disconnect;
            });
        }
        group_status.lock().await.push(GroupStatus {
            ts,
            succeed,
            failed,
            status,
        });
    }

    fn start_clients(&mut self) {
        let clients = self.clients.clone();
        let mut connect_interval = time::interval(time::Duration::from_millis(
            self.broker_info.connect_interval,
        ));
        let client_connected_count = self.client_connected_count.clone();
        let client_count = self.conf.client_count;
        let mut stop_signal_rx = self.stop_signal_tx.subscribe();
        tokio::spawn(async move {
            let mut index = 0;
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        break;
                    }

                    _ = connect_interval.tick() => {
                        if index < client_count {
                            clients.write().await[index].start();
                            index += 1;
                            client_connected_count.store(index, Ordering::Relaxed);
                        } else {
                            break;
                        }
                    }
                }
            }
        });
    }

    pub async fn read(&self) -> ReadGroupResp {
        ReadGroupResp {
            id: self.id.clone(),
            conf: (*self.conf).clone(),
            status: self.status.as_ref().unwrap().lock().await.clone(),
        }
    }

    pub async fn create_publish(&mut self, req: PublishCreateUpdateReq) {
        let req = Arc::new(req);
        if self.running {
            self.clients.write().await.iter_mut().for_each(|client| {
                client.create_publish(req.clone());
            });
        }

        self.publishes.push(Publish {
            id: generate_id(),
            conf: req,
        });
    }

    pub async fn list_publishes(&self) {
        todo!()
    }

    pub async fn read_publish(&self, publish_id: String) {
        todo!()
    }

    pub async fn update_publish(&mut self, publish_id: String, req: PublishCreateUpdateReq) {
        todo!()
    }

    pub async fn delete_publish(&mut self, publish_id: String) {
        todo!()
    }

    pub async fn create_subscribe(&mut self, req: SubscribeCreateUpdateReq) {
        todo!()
    }

    pub async fn list_subscribes(&self) {
        todo!()
    }

    pub async fn read_subscribe(&self, subscribe_id: String) {
        todo!()
    }

    pub async fn update_subscribe(&mut self, subscribe_id: String, req: SubscribeCreateUpdateReq) {
        todo!()
    }

    pub async fn delete_subscribe(&mut self, subscribe_id: String) {
        todo!()
    }
}

pub struct Publish {
    id: String,
    conf: Arc<PublishCreateUpdateReq>,
}

pub struct Subscribe {}
