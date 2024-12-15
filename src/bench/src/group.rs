use std::{sync::Arc, time::SystemTime};

use futures::lock::BiLock;
use tokio::{select, time};
use types::{
    BrokerUpdateReq, ClientStatus, GroupCreateUpdateReq, PublishCreateUpdateReq,
    SubscribeCreateUpdateReq,
};

use crate::{
    client::{self, ClientV311},
    generate_id,
};

pub struct Group {
    pub id: String,
    pub conf: Arc<GroupCreateUpdateReq>,
    running: bool,
    clients: Option<BiLock<Vec<client::ClientV311>>>,
    status: Option<BiLock<Vec<ClientStatus>>>,
    broker_info: Arc<BrokerUpdateReq>,

    publishes: Vec<Publish>,
}

impl Group {
    pub fn new(id: String, broker_info: Arc<BrokerUpdateReq>, conf: GroupCreateUpdateReq) -> Self {
        Self {
            id,
            conf: Arc::new(conf),
            running: false,
            clients: None,
            status: None,
            broker_info,
            publishes: vec![],
        }
    }

    pub async fn start(&mut self) {
        self.running = true;

        let (clients1, clients2) = BiLock::new(Vec::with_capacity(self.conf.client_count));
        let (status1, status2) = BiLock::new(Vec::new());

        let broker_info = self.broker_info.clone();
        let conf = self.conf.clone();
        tokio::spawn(async move {
            let mut status_interval = time::interval(time::Duration::from_secs(1));
            let mut connect_interval =
                time::interval(time::Duration::from_millis(1000 / broker_info.connect_rate));
            let mut index = 0;
            loop {
                select! {
                    _ = status_interval.tick() => {
                        get_status(&clients1, &status1).await;
                    }
                    _ = connect_interval.tick() => {
                        if index < conf.client_count {
                            connect(index, &clients1, &broker_info).await;
                            index += 1;
                        }
                    }
                }
            }
        });

        self.clients = Some(clients2);
        self.status = Some(status2);
    }

    pub async fn status(&self) -> Vec<ClientStatus> {
        self.status.as_ref().unwrap().lock().await.clone()
    }

    pub async fn create_publish(&mut self, req: PublishCreateUpdateReq) {
        if self.running {
            let mut clients_guard = self.clients.as_ref().unwrap().lock().await;
            clients_guard
                .iter_mut()
                .for_each(|client| client.create_publish(req.clone()));
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

async fn get_status(clients: &BiLock<Vec<ClientV311>>, status: &BiLock<Vec<ClientStatus>>) {
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut succeed = 0;
    let mut failed = 0;
    {
        let clients_guard = clients.lock().await;
        clients_guard.iter().for_each(|client| {
            if client.get_status() {
                succeed += 1;
            } else {
                failed += 1;
            }
        });
    }
    status.lock().await.push(ClientStatus {
        ts,
        succeed,
        failed,
    });
}

async fn connect(
    index: usize,
    clients: &BiLock<Vec<ClientV311>>,
    broker_info: &Arc<BrokerUpdateReq>,
) {
    let client_conf = client::ClientConf {
        index,
        // TODO
        id: format!("client-{}", index),
        host: broker_info.addrs[index % broker_info.addrs.len()].0.clone(),
        port: broker_info.addrs[index % broker_info.addrs.len()].1,
        keep_alive: 60,
        username: broker_info.username.clone(),
        password: broker_info.password.clone(),
    };
    let client = client::ClientV311::new(client_conf).await;
    clients.lock().await.push(client);
}

pub struct Publish {
    id: String,
    conf: PublishCreateUpdateReq,
}

pub struct Subscribe {}
