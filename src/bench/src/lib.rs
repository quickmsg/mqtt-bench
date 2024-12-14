use std::{sync::LazyLock, time::Duration};

use tokio::time;
use types::{ClientStatus, ConnectBenchInfo, MqttServerInfo};

mod client;
mod mqtt;

static GLOBAL_CLIENT_MANAGER: LazyLock<ClientManager> = LazyLock::new(|| ClientManager::new());

pub struct ClientManager {
    running: bool,
    clients: Vec<client::ClientV311>,
}

impl ClientManager {
    pub fn new() -> Self {
        Self {
            running: false,
            clients: vec![],
        }
    }

    pub async fn start_task(
        &mut self,
        mqtt_server_info: MqttServerInfo,
        connect_bench_info: ConnectBenchInfo,
    ) {
        let interval = 1000 / connect_bench_info.rate;

        self.running = true;
        for i in 0..connect_bench_info.client_count {
            let client_conf = client::ClientConf {
                index: i as usize,
                // TODO
                id: format!("client-{}", i),
                host: mqtt_server_info.addrs[i as usize % mqtt_server_info.addrs.len()]
                    .0
                    .clone(),
                port: mqtt_server_info.addrs[i as usize % mqtt_server_info.addrs.len()].1,
                keep_alive: 60,
                username: mqtt_server_info.username.clone(),
                password: mqtt_server_info.password.clone(),
            };
            let client = client::ClientV311::new(client_conf).await;
            self.clients.push(client);
            time::sleep(Duration::from_millis(interval)).await;
        }
    }

    pub fn get_status(&self) -> ClientStatus {
        let mut succeed = 0;
        let mut failed = 0;
        for client in &self.clients {
            if client.get_status() {
                succeed += 1;
            } else {
                failed += 1;
            }
        }
        ClientStatus { succeed, failed }
    }
}
