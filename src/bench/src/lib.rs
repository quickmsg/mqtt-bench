use std::{sync::LazyLock, time::SystemTime};

use client::ClientV311;
use futures::lock::BiLock;
use tokio::{select, time};
use types::{ClientStatus, ConnectBenchInfo, MqttServerInfo};

mod client;
mod mqtt;

static GLOBAL_CLIENT_MANAGER: LazyLock<ClientManager> = LazyLock::new(|| ClientManager::new());

pub struct ClientManager {
    running: bool,
    clients: Option<BiLock<Vec<client::ClientV311>>>,
    status: Option<BiLock<Vec<ClientStatus>>>,
}

impl ClientManager {
    pub fn new() -> Self {
        Self {
            running: false,
            clients: None,
            status: None,
        }
    }

    pub async fn start_task(
        &mut self,
        mqtt_server_info: MqttServerInfo,
        connect_bench_info: ConnectBenchInfo,
    ) {
        self.running = true;

        let (clients1, clients2) = BiLock::new(Vec::with_capacity(connect_bench_info.client_count));
        let (status1, status2) = BiLock::new(Vec::new());

        tokio::spawn(async move {
            let mut status_interval = time::interval(time::Duration::from_secs(1));
            let mut connect_interval =
                time::interval(time::Duration::from_millis(1000 / connect_bench_info.rate));
            let mut index = 0;
            loop {
                select! {
                    _ = status_interval.tick() => {
                        get_status(&clients1, &status1).await;
                    }
                    _ = connect_interval.tick() => {
                        if index <= connect_bench_info.client_count {
                            connect(index, &clients1, &mqtt_server_info).await;
                            index += 1;
                        }
                    }
                }
            }
        });

        self.clients = Some(clients2);
        self.status = Some(status2);
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
    mqtt_server_info: &MqttServerInfo,
) {
    let client_conf = client::ClientConf {
        index,
        // TODO
        id: format!("client-{}", index),
        host: mqtt_server_info.addrs[index % mqtt_server_info.addrs.len()]
            .0
            .clone(),
        port: mqtt_server_info.addrs[index % mqtt_server_info.addrs.len()].1,
        keep_alive: 60,
        username: mqtt_server_info.username.clone(),
        password: mqtt_server_info.password.clone(),
    };
    let client = client::ClientV311::new(client_conf).await;
    clients.lock().await.push(client);
}
