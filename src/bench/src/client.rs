use std::time::Duration;

use rumqttc::{AsyncClient, MqttOptions};

pub struct ClientConf {
    pub index: usize,
    pub id: String,
    pub host: String,
    pub port: u16,
    pub keep_alive: u64,
    pub username: Option<String>,
    pub password: Option<String>,
}

pub struct ClientV311 {
    client: AsyncClient,
    err: Option<String>,
}

impl ClientV311 {
    pub async fn new(conf: ClientConf) -> Self {
        let mut mqtt_options = MqttOptions::new(conf.id, conf.host, conf.port);
        mqtt_options.set_keep_alive(Duration::from_secs(conf.keep_alive));
        let username = conf.username.unwrap_or_default();
        let password = conf.password.unwrap_or_default();
        mqtt_options.set_credentials(username, password);

        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 8);
        tokio::spawn(async move {
            while let Ok(notification) = eventloop.poll().await {
                println!("Received = {:?}", notification);
            }
        });

        Self { client, err: None }
    }

    pub fn get_status(&self) -> bool {
        self.err.is_none()
    }

    pub fn get_err_info(&self) -> Result<(), String> {
        if self.err.is_some() {
            Err(self.err.clone().unwrap())
        } else {
            Ok(())
        }
    }
}
