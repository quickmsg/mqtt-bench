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
    BrokerUpdateReq, GroupCreateUpdateReq, GroupMetrics, PacketMetrics, PublishCreateUpdateReq,
    ReadGroupResp, SubscribeCreateUpdateReq,
};

use crate::{
    client::{self, Client},
    generate_id,
};

pub struct Group {
    pub id: String,
    pub conf: Arc<GroupCreateUpdateReq>,
    running: bool,

    clients: Arc<RwLock<Vec<Box<dyn Client>>>>,
    client_connected_count: Arc<AtomicUsize>,

    status: Option<BiLock<Vec<GroupMetrics>>>,
    broker_info: Arc<BrokerUpdateReq>,
    stop_signal_tx: tokio::sync::broadcast::Sender<()>,

    publishes: Vec<(Arc<String>, Arc<PublishCreateUpdateReq>)>,
    subscribes: Vec<(Arc<String>, Arc<SubscribeCreateUpdateReq>)>,
}

impl Group {
    pub fn new(id: String, broker_info: Arc<BrokerUpdateReq>, req: GroupCreateUpdateReq) -> Self {
        // TODO 优化req clone
        let group_conf = Arc::new(req.clone());
        let mut clients = Vec::with_capacity(req.client_count);
        for index in 0..req.client_count {
            let client_conf = client::ClientConf {
                index,
                id: format!("client-{}", index),
                host: broker_info.hosts[index % broker_info.hosts.len()].clone(),
                port: req.port,
                keep_alive: 60,
                username: None,
                password: None,
            };
            match (&req.protocol, &req.protocol_version) {
                (types::Protocol::Mqtt, types::ProtocolVersion::V311) => {
                    clients.push(client::mqtt_v311::new(client_conf, group_conf.clone()));
                }
                (types::Protocol::Mqtt, types::ProtocolVersion::V50) => {
                    clients.push(client::mqtt_v50::new(client_conf, group_conf.clone()));
                }
                (types::Protocol::Websocket, types::ProtocolVersion::V311) => {
                    clients.push(client::websocket_v311::new(client_conf, group_conf.clone()));
                }
                (types::Protocol::Websocket, types::ProtocolVersion::V50) => {
                    clients.push(client::websocket_v50::new(client_conf, group_conf.clone()));
                }
                (types::Protocol::Http, _) => {
                    todo!()
                }
            }
        }

        let (stop_signal_tx, _) = tokio::sync::broadcast::channel(1);
        Self {
            id,
            conf: Arc::new(req),
            running: false,
            clients: Arc::new(RwLock::new(clients)),
            status: None,
            broker_info,
            client_connected_count: Arc::new(AtomicUsize::new(0)),
            stop_signal_tx,
            publishes: vec![],
            subscribes: vec![],
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

        for client in self.clients.write().await.iter_mut() {
            client.stop().await;
        }

        self.stop_signal_tx.send(()).unwrap();
    }

    fn start_collect_status(&mut self, status: BiLock<Vec<GroupMetrics>>) {
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
        clients: &Arc<RwLock<Vec<Box<dyn Client>>>>,
        group_status: &BiLock<Vec<GroupMetrics>>,
    ) {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut succeed = 0;
        let mut failed = 0;
        let mut packet_metrics = PacketMetrics::default();
        {
            let clients_guard = clients.read().await;
            clients_guard.iter().for_each(|client| {
                let client_status = client.get_status();
                if client_status.success {
                    succeed += 1;
                } else {
                    failed += 1;
                }
                packet_metrics.conn_ack += client_status.conn_ack;
                packet_metrics.pub_ack += client_status.pub_ack;
                packet_metrics.unsub_ack += client_status.unsub_ack;
                packet_metrics.ping_req += client_status.ping_req;
                packet_metrics.ping_resp += client_status.ping_resp;
                packet_metrics.publish += client_status.publish;
                packet_metrics.subscribe += client_status.subscribe;
                packet_metrics.unsubscribe += client_status.unsubscribe;
                packet_metrics.disconnect += client_status.disconnect;
            });
        }
        group_status.lock().await.push(GroupMetrics {
            ts,
            succeed,
            failed,
            packet_metrics,
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
                            clients.write().await[index].start().await;
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

    pub async fn update(&self, req: GroupCreateUpdateReq) {
        let conf = Arc::new(req);
        match self.conf.client_count.cmp(&conf.client_count) {
            std::cmp::Ordering::Less => todo!(),
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => todo!(),
        }
    }

    pub async fn create_publish(&mut self, req: PublishCreateUpdateReq) {
        let id = Arc::new(generate_id());
        let conf = Arc::new(req);
        self.clients.write().await.iter_mut().for_each(|client| {
            client.create_publish(id.clone(), conf.clone());
        });

        self.publishes.push((id, conf));
    }

    pub async fn list_publishes(&self) {
        todo!()
    }

    pub async fn read_publish(&self, publish_id: String) {
        todo!()
    }

    pub async fn update_publish(&mut self, publish_id: String, req: PublishCreateUpdateReq) {
        let conf = Arc::new(req);
        for client in self.clients.write().await.iter_mut() {
            client.update_publish(&publish_id, conf.clone());
        }
        self.publishes
            .iter_mut()
            .find(|(id, _)| **id == publish_id)
            .unwrap()
            .1 = conf;
    }

    pub async fn delete_publish(&mut self, publish_id: String) {
        for client in self.clients.write().await.iter_mut() {
            client.delete_publish(&publish_id);
        }
        self.subscribes.retain(|(id, _)| **id != publish_id);
    }

    pub async fn create_subscribe(&mut self, req: SubscribeCreateUpdateReq) {
        let id = Arc::new(generate_id());
        let conf = Arc::new(req);
        for client in self.clients.write().await.iter_mut() {
            client.create_subscribe(id.clone(), conf.clone()).await;
        }

        self.subscribes.push((id, conf));
    }

    pub async fn list_subscribes(&self) {
        todo!()
    }

    pub async fn read_subscribe(&self, subscribe_id: String) {
        todo!()
    }

    pub async fn update_subscribe(&mut self, subscribe_id: String, req: SubscribeCreateUpdateReq) {
        // TODO 判断是否只是更改名称或完全没更改，避免全部无用更新

        let conf = Arc::new(req);
        for client in self.clients.write().await.iter_mut() {
            client.update_subscribe(&subscribe_id, conf.clone()).await;
        }
    }

    pub async fn delete_subscribe(&mut self, subscribe_id: String) {
        for client in self.clients.write().await.iter_mut() {
            client.delete_subscribe(&subscribe_id).await;
        }
        self.subscribes.retain(|(id, _)| **id != subscribe_id);
    }
}
