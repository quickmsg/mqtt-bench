use std::{sync::Arc, time::SystemTime};

use futures::lock::BiLock;
use tokio::{select, time};
use types::{BrokerInfo, ClientStatus, GroupConf};

use crate::client::{self, ClientV311};

pub struct Group {
    running: bool,
    pub conf: Arc<GroupConf>,
    clients: Option<BiLock<Vec<client::ClientV311>>>,
    status: Option<BiLock<Vec<ClientStatus>>>,
    broker_info: Arc<BrokerInfo>,
}

impl Group {
    pub fn new(broker_info: Arc<BrokerInfo>, conf: GroupConf) -> Self {
        Self {
            running: false,
            conf: Arc::new(conf),
            clients: None,
            status: None,
            broker_info,
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

async fn connect(index: usize, clients: &BiLock<Vec<ClientV311>>, broker_info: &Arc<BrokerInfo>) {
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
