use std::{sync::Arc, time::SystemTime};

use client::ClientV311;
use futures::lock::BiLock;
use tokio::{select, time};
use types::{ClientStatus, ConnectBenchInfo, MqttServerInfo};

mod client;
mod mqtt;

pub struct ClientManager {
    running: bool,
    clients: Option<BiLock<Vec<client::ClientV311>>>,
    status: Option<BiLock<Vec<ClientStatus>>>,
    mqtt_server_info: Arc<MqttServerInfo>,
    connect_bench_info: Arc<ConnectBenchInfo>,
}

impl ClientManager {
    pub fn new(mqtt_server_info: MqttServerInfo, connect_bench_info: ConnectBenchInfo) -> Self {
        Self {
            running: false,
            clients: None,
            status: None,
            mqtt_server_info: Arc::new(mqtt_server_info),
            connect_bench_info: Arc::new(connect_bench_info),
        }
    }

    pub async fn start(&mut self) {
        self.running = true;

        let (clients1, clients2) =
            BiLock::new(Vec::with_capacity(self.connect_bench_info.client_count));
        let (status1, status2) = BiLock::new(Vec::new());

        let connect_bench_info = self.connect_bench_info.clone();
        let mqtt_server_info = self.mqtt_server_info.clone();
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
                        if index < connect_bench_info.client_count {
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

    pub async fn status(&self) -> Vec<ClientStatus> {
        self.status.as_ref().unwrap().lock().await.clone()
    }
}

async fn get_status(clients: &BiLock<Vec<ClientV311>>, status: &BiLock<Vec<ClientStatus>>) {
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    println!("{}", clients.lock().await.len());
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
