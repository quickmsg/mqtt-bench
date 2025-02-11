use std::{
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};

use anyhow::{bail, Result};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Local};
use futures::lock::BiLock;
use mqtt::protocol::v3_mini::{
    v4::{Subscribe, SubscribeFilter},
    QoS,
};
use tokio::{
    select,
    sync::{
        mpsc::{self, UnboundedSender},
        oneshot,
    },
    time,
};
use tracing::{debug, info};
use types::{
    group::{ClientAtomicMetrics, PacketAtomicMetrics},
    BrokerUpdateReq, ClientMetrics, ClientUsizeMetrics, ClientsListResp, ClientsQueryParams,
    GroupCreateReq, ListPublishResp, ListPublishRespItem, ListSubscribeResp, ListSubscribeRespItem,
    MetricsListItem, MetricsListResp, MetricsQueryParams, PacketMetrics, PacketUsizeMetrics,
    PublishConf, PublishCreateUpdateReq, ReadGroupResp, SslConf, Status, SubscribeCreateUpdateReq,
};
use uuid::Uuid;

use crate::{
    client::{self, Client},
    generate_id,
};

// 运行中不允许更新，降低复杂度
pub struct Group {
    pub id: String,
    pub status: Status,
    pub conf: GroupCreateReq,

    clients: Arc<Vec<Box<dyn Client>>>,

    history_metrics: Option<BiLock<Vec<(u64, ClientUsizeMetrics, PacketUsizeMetrics)>>>,
    broker_info: Arc<BrokerUpdateReq>,
    stop_signal_tx: tokio::sync::broadcast::Sender<()>,

    publishes: Vec<(Arc<String>, PublishCreateUpdateReq)>,
    subscribes: Vec<(Arc<String>, Arc<SubscribeCreateUpdateReq>)>,

    client_metrics: Arc<ClientAtomicMetrics>,
    packet_metrics: Arc<PacketAtomicMetrics>,

    running_client: Arc<AtomicU32>,
}

pub struct ClientGroupConf {
    pub port: u16,
    pub ssl_conf: Option<SslConf>,
}

impl Group {
    pub fn new(id: String, broker_info: Arc<BrokerUpdateReq>, mut req: GroupCreateReq) -> Self {
        let clients_conf = req.clients.take();
        // TODO 优化req clone
        let group_conf = Arc::new(req.clone());
        let client_group_conf = Arc::new(ClientGroupConf {
            port: req.port,
            ssl_conf: req.ssl_conf.clone(),
        });

        let client_metrics = Arc::new(ClientAtomicMetrics::default());
        let packet_metrics = Arc::new(PacketAtomicMetrics::default());

        let (stop_signal_tx, _) = tokio::sync::broadcast::channel(1);

        let clients = Self::new_clients(
            &id,
            // group_conf.client_count,
            &broker_info,
            &group_conf,
            client_group_conf,
            &client_metrics,
            &packet_metrics,
            clients_conf,
        );
        let clients = Arc::new(clients);

        Self {
            id,
            status: Status::Stopped,
            conf: req,
            clients,
            history_metrics: None,
            broker_info,
            stop_signal_tx,
            publishes: vec![],
            subscribes: vec![],
            client_metrics,
            packet_metrics,
            running_client: Arc::new(AtomicU32::new(0)),
        }
    }

    fn new_clients(
        group_id: &String,
        broker_info: &Arc<BrokerUpdateReq>,
        group_conf: &GroupCreateReq,
        client_group_conf: Arc<ClientGroupConf>,
        client_metrics: &Arc<ClientAtomicMetrics>,
        packet_metrics: &Arc<PacketAtomicMetrics>,
        clients_conf: Option<Vec<(String, String, String, String)>>,
    ) -> Vec<Box<dyn Client>> {
        match clients_conf {
            Some(clients_conf) => Self::new_conf_clients(
                group_id,
                broker_info,
                group_conf,
                client_group_conf,
                client_metrics,
                packet_metrics,
                clients_conf,
            ),
            None => Self::new_random_clients(
                group_id,
                broker_info,
                group_conf,
                client_group_conf,
                client_metrics,
                packet_metrics,
            ),
        }
    }

    fn new_random_clients(
        group_id: &String,
        broker_info: &Arc<BrokerUpdateReq>,
        group_conf: &GroupCreateReq,
        client_group_conf: Arc<ClientGroupConf>,
        client_metrics: &Arc<ClientAtomicMetrics>,
        packet_metrics: &Arc<PacketAtomicMetrics>,
    ) -> Vec<Box<dyn Client>> {
        let client_count = group_conf.client_count.unwrap();
        let mut clients = Vec::with_capacity(client_count);

        let client_id = group_conf.client_id.as_ref().unwrap();

        let client_id_template = parse_template(client_id);

        for index in 0..client_count {
            let client_id = match client_id_template {
                Template::None => client_id.clone(),
                Template::Index => client_id.replace("{index}", &index.to_string()),
                Template::GroupId => client_id.replace("{group_id}", group_id),
                Template::Uuid => client_id.replace("{uuid}", &Uuid::new_v4().to_string()),
                Template::IndexGroupId => client_id
                    .replace("{index}", &index.to_string())
                    .replace("{group_id}", group_id),
                Template::IndexUuid => client_id
                    .replace("{index}", &index.to_string())
                    .replace("{uuid}", &Uuid::new_v4().to_string()),
                Template::UuidGroupId => client_id
                    .replace("{uuid}", &Uuid::new_v4().to_string())
                    .replace("{group_id}", group_id),
                Template::IndexGroupIdUuid => client_id
                    .replace("{index}", &index.to_string())
                    .replace("{group_id}", group_id)
                    .replace("{uuid}", &Uuid::new_v4().to_string()),
                Template::Range => unreachable!(),
            };
            let local_ip = match &broker_info.local_ips {
                Some(ips) => Some(ips[index % ips.len()].clone()),
                None => None,
            };
            let client_conf = client::ClientConf {
                client_id,
                host: broker_info.hosts[index % broker_info.hosts.len()].clone(),
                keep_alive: u16::MAX,
                username: None,
                password: None,
                local_ip,
            };
            match (&group_conf.protocol, &group_conf.protocol_version) {
                (types::Protocol::Mqtt, types::ProtocolVersion::V311) => {
                    clients.push(client::mqtt_v311::new(
                        client_conf,
                        client_group_conf.clone(),
                        client_metrics.clone(),
                        packet_metrics.clone(),
                    ));
                }
                (types::Protocol::Mqtt, types::ProtocolVersion::V50) => {
                    todo!()
                    // clients.push(client::mqtt_v50::new(
                    //     client_conf,
                    //     client_group_conf.clone(),
                    //     client_metrics.clone(),
                    //     packet_metrics.clone(),
                    // ));
                }
                (types::Protocol::Websocket, types::ProtocolVersion::V311) => {
                    todo!()
                    // clients.push(client::websocket_v311::new(
                    //     client_conf,
                    //     client_group_conf.clone(),
                    //     client_metrics.clone(),
                    //     packet_metrics.clone(),
                    // ));
                }
                (types::Protocol::Websocket, types::ProtocolVersion::V50) => {
                    // clients.push(client::websocket_v50::new(
                    //     client_conf,
                    //     client_group_conf.clone(),
                    //     client_metrics.clone(),
                    //     packet_metrics.clone(),
                    // ));
                    todo!()
                }
                (types::Protocol::Http, _) => {
                    todo!()
                }
            }
        }
        debug!("client created, count: {:?}", clients.len());
        clients
    }

    fn new_conf_clients(
        group_id: &String,
        broker_info: &Arc<BrokerUpdateReq>,
        group_conf: &GroupCreateReq,
        client_group_conf: Arc<ClientGroupConf>,
        client_metrics: &Arc<ClientAtomicMetrics>,
        packet_metrics: &Arc<PacketAtomicMetrics>,
        clients_conf: Vec<(String, String, String, String)>,
    ) -> Vec<Box<dyn Client>> {
        let mut clients = Vec::with_capacity(clients_conf.len());

        for (index, cli_client_conf) in clients_conf.into_iter().enumerate() {
            let local_ip = match &broker_info.local_ips {
                Some(ips) => Some(ips[index % ips.len()].clone()),
                None => None,
            };

            let client_conf = client::ClientConf {
                client_id: cli_client_conf.0,
                host: broker_info.hosts[index % broker_info.hosts.len()].clone(),
                keep_alive: u16::MAX,
                username: Some(cli_client_conf.1),
                password: Some(cli_client_conf.2),
                local_ip,
            };
            match (&group_conf.protocol, &group_conf.protocol_version) {
                (types::Protocol::Mqtt, types::ProtocolVersion::V311) => {
                    clients.push(client::mqtt_v311::new(
                        client_conf,
                        client_group_conf.clone(),
                        client_metrics.clone(),
                        packet_metrics.clone(),
                    ));
                }
                (types::Protocol::Mqtt, types::ProtocolVersion::V50) => {
                    todo!()
                    // clients.push(client::mqtt_v50::new(
                    //     client_conf,
                    //     client_group_conf.clone(),
                    //     client_metrics.clone(),
                    //     packet_metrics.clone(),
                    // ));
                }
                (types::Protocol::Websocket, types::ProtocolVersion::V311) => {
                    todo!()
                    // clients.push(client::websocket_v311::new(
                    //     client_conf,
                    //     client_group_conf.clone(),
                    //     client_metrics.clone(),
                    //     packet_metrics.clone(),
                    // ));
                }
                (types::Protocol::Websocket, types::ProtocolVersion::V50) => {
                    // clients.push(client::websocket_v50::new(
                    //     client_conf,
                    //     client_group_conf.clone(),
                    //     client_metrics.clone(),
                    //     packet_metrics.clone(),
                    // ));
                    todo!()
                }
                (types::Protocol::Http, _) => {
                    todo!()
                }
            }
        }

        clients
    }

    pub fn start(&mut self, done_tx: mpsc::UnboundedSender<()>) {
        tokio::spawn(async move {});
        match self.status {
            Status::Starting | Status::Running => return,
            Status::Stopped | Status::Waiting | Status::Updating => {
                self.status = Status::Starting;
            }
        }

        let (history_metrics_1, history_metrics_2) = BiLock::new(Vec::new());
        self.start_collect_metrics(history_metrics_1, self.broker_info.statistics_interval);
        self.history_metrics = Some(history_metrics_2);

        let (start_clients_done_tx, start_clients_done_rx) = oneshot::channel();
        self.start_clients(start_clients_done_tx);
        Self::wait_clients_start(
            done_tx,
            self.stop_signal_tx.clone(),
            self.clients.clone(),
            self.publishes.clone(),
            start_clients_done_rx,
        );
    }

    fn wait_clients_start(
        done_tx: UnboundedSender<()>,
        stop_sgianl_tx: tokio::sync::broadcast::Sender<()>,
        clients: Arc<Vec<Box<dyn Client>>>,
        publishes: Vec<(Arc<String>, PublishCreateUpdateReq)>,
        start_clients_done_rx: oneshot::Receiver<()>,
    ) {
        tokio::spawn(async move {
            let _ = start_clients_done_rx.await;
            done_tx.send(()).unwrap();
            for (_, publish) in publishes {
                Self::start_publish(stop_sgianl_tx.subscribe(), clients.clone(), publish);
            }
        });
    }

    fn start_publish(
        mut stop_sginal_rx: tokio::sync::broadcast::Receiver<()>,
        clients: Arc<Vec<Box<dyn Client>>>,
        publish: PublishCreateUpdateReq,
    ) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(1));
            let mill_cnt = publish.tps / 1000;
            debug!("mill cnt {:?}", mill_cnt);
            let topic = publish.topic.clone();
            let qos = match publish.qos {
                types::Qos::AtMostOnce => 0,
                types::Qos::AtLeastOnce => 1,
                types::Qos::ExactlyOnce => 2,
            };

            let payload = match (publish.size, publish.payload) {
                (None, Some(payload)) => payload.into(),
                (Some(size), None) => {
                    let mut buf = BytesMut::with_capacity(size);
                    for _ in 0..size {
                        buf.put_u8(0);
                    }
                    buf.freeze()
                }
                _ => panic!("请指定 payload 或 size"),
            };
            let payload = Arc::new(payload);
            let client_pos = 0;
            let client_len = clients.len();
            debug!("client start publish");
            loop {
                select! {
                    _ = stop_sginal_rx.recv() => {
                        break;
                    }

                    _ = interval.tick() => {
                        Self::client_publish(&clients, client_pos, mill_cnt, &topic, qos, &payload, client_len).await;
                    }
                }
            }
        });
    }

    async fn client_publish_range(
        clients: &Vec<Box<dyn Client>>,
        range: usize,
        range_pos: &mut usize,
        mill_cnt: usize,
        topic: &String,
        qos: u8,
        payload: &Arc<Bytes>,
        pkid: &mut u16,
    ) {
        for _ in 0..mill_cnt {
            *pkid += 1;
            if *pkid == u16::MAX {
                *pkid = 1;
            }

            let topic = topic.replace("{index}", range_pos.to_string().as_str());
            clients[0].publish(topic, qos, payload.clone());

            if *range_pos == range {
                *range_pos = 0;
            } else {
                *range_pos += 1;
            }
        }
    }

    async fn client_publish(
        clients: &Vec<Box<dyn Client>>,
        mut client_pos: usize,
        mill_cnt: usize,
        topic: &String,
        qos: u8,
        payload: &Arc<Bytes>,
        client_len: usize,
    ) {
        for _ in 0..mill_cnt {
            clients[client_pos]
                .publish(topic.clone(), qos, payload.clone())
                .await;
            client_pos += 1;
            client_pos %= client_len;
        }
    }

    async fn client_subscribe(
        clients: &Vec<Box<dyn Client>>,
        mut client_pos: usize,
        mill_cnt: usize,
        topic: &String,
        qos: u8,
        payload: &Arc<Bytes>,
        client_len: usize,
    ) {
        // for _ in 0..mill_cnt {
        //     clients[client_pos].publish(topic.clone(), qos, payload.clone());
        //     client_pos += 1;
        //     client_pos %= client_len;

        //     if client_pos == 0 {
        //         *pkid += 1;
        //         if *pkid == u16::MAX {
        //             *pkid = 1;
        //         }
        //     }
        // }
    }

    pub async fn stop(&mut self) {
        match self.status {
            Status::Stopped => return,
            Status::Starting | Status::Running | Status::Waiting | Status::Updating => {
                self.status = Status::Stopped;
            }
        }

        for client in self.clients.iter() {
            client.stop().await;
        }

        self.stop_signal_tx.send(()).unwrap();
    }

    fn start_collect_metrics(
        &mut self,
        history_metrics: BiLock<Vec<(u64, ClientUsizeMetrics, PacketUsizeMetrics)>>,
        statistics_interval: u64,
    ) {
        let mut stop_signal_rx = self.stop_signal_tx.subscribe();
        let mut status_interval = time::interval(time::Duration::from_secs(statistics_interval));
        let client_metrics = self.client_metrics.clone();
        let packet_metrics = self.packet_metrics.clone();
        let client_running_cnt = self.running_client.clone();

        let mut total_packet_metrics = PacketUsizeMetrics::default();
        let start_time: DateTime<Local> = Local::now();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        break;
                    }

                    _ = status_interval.tick() => {
                        Self::collect_metrics(&client_metrics,&packet_metrics, &history_metrics, &client_running_cnt, &mut total_packet_metrics, &start_time).await;
                    }
                }
            }
        });
    }

    async fn collect_metrics(
        client_metrics: &Arc<ClientAtomicMetrics>,
        packet_metrics: &Arc<PacketAtomicMetrics>,
        history_metrics: &BiLock<Vec<(u64, ClientUsizeMetrics, PacketUsizeMetrics)>>,
        running_client: &Arc<AtomicU32>,
        total_packet_metrics: &mut PacketUsizeMetrics,
        start_time: &DateTime<Local>,
    ) {
        let elapsed = Local::now().signed_duration_since(*start_time);
        let hour = elapsed.num_hours();
        let minute = elapsed.num_minutes();
        let seconds = elapsed.num_seconds();
        if hour > 0 {
            info!("运行时长: {}小时{}分{}秒", hour, minute, seconds);
        } else if minute > 0 {
            info!("运行时长: {}分{}秒", minute, seconds);
        } else {
            info!("运行时长: {}秒", seconds);
        }

        let client_usize_metrics = client_metrics.take_metrics();
        let prev = running_client.fetch_add(
            client_usize_metrics.running_cnt as u32,
            std::sync::atomic::Ordering::SeqCst,
        );
        info!(
            "running client: {:?}",
            prev + client_usize_metrics.running_cnt as u32
        );

        let pakcet_usize_metrics = packet_metrics.take_metrics();
        info!("当前包: {:?}", pakcet_usize_metrics);
        *total_packet_metrics += pakcet_usize_metrics;
        info!("历史包: {:?}", total_packet_metrics);
        // history_metrics
        //     .lock()
        //     .await
        //     .push((ts, client_usize_metrics, pakcet_usize_metrics));
    }

    fn start_clients(&mut self, done_tx: oneshot::Sender<()>) {
        let mut connect_interval = time::interval(time::Duration::from_micros(
            self.broker_info.connect_interval,
        ));

        let mut stop_signal_rx = self.stop_signal_tx.subscribe();
        let mut index = 0;
        let clients = self.clients.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        debug!("sotp signal rx recv");
                        // TODO 停止client
                        break;
                    }

                    _ = connect_interval.tick() => {
                        if index < clients.len() {
                            clients[index].start().await;
                            // match self.get_sub(index) {
                            //     Some(sub) => {
                            //         self.clients[index].subscribe(sub);
                            //     }
                            //     None => {}
                            // }
                            index += 1;
                        } else {
                            done_tx.send(()).unwrap();
                            break;
                        }
                    }
                }
            }
        });
    }

    fn get_sub(&self, index: usize) -> Option<Subscribe> {
        if self.subscribes.len() == 0 {
            return None;
        }
        let mut filters = Vec::with_capacity(self.subscribes.len());
        for subscribe in self.subscribes.iter() {
            let template = parse_template(&subscribe.1.topic);
            let topic_filter = match template {
                Template::None => subscribe.1.topic.clone(),
                Template::Index => subscribe
                    .1
                    .topic
                    .replace("{index}", index.to_string().as_str()),
                Template::GroupId => todo!(),
                Template::Uuid => todo!(),
                Template::IndexGroupId => todo!(),
                Template::IndexUuid => todo!(),
                Template::UuidGroupId => todo!(),
                Template::IndexGroupIdUuid => todo!(),
                Template::Range => todo!(),
            };

            let qos = match subscribe.1.qos {
                types::Qos::AtMostOnce => QoS::AtLeastOnce,
                types::Qos::AtLeastOnce => QoS::AtLeastOnce,
                types::Qos::ExactlyOnce => QoS::ExactlyOnce,
            };
            filters.push(SubscribeFilter {
                path: topic_filter,
                qos,
            });
        }
        Some(Subscribe { pkid: 1, filters })
    }

    pub async fn read(&self) -> ReadGroupResp {
        ReadGroupResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    // fn check_update_client(old_conf: &GroupCreateReq, new_conf: &GroupUpdateReq) -> bool {
    //     if old_conf.port != new_conf.port {
    //         return true;
    //     }

    //     if old_conf.client_id != new_conf.client_id {
    //         return true;
    //     }

    //     match (&old_conf.ssl_conf, &new_conf.ssl_conf) {
    //         (Some(old_ssl_conf), Some(new_ssl_conf)) => {
    //             if old_ssl_conf.verify != new_ssl_conf.verify {
    //                 return true;
    //             }

    //             match (&old_ssl_conf.ca_cert, &new_ssl_conf.ca_cert) {
    //                 (Some(old_ca_cert), Some(new_ca_cert)) => {
    //                     if old_ca_cert != new_ca_cert {
    //                         return true;
    //                     }
    //                 }
    //                 (None, Some(_)) => return true,
    //                 (Some(_), None) => return true,
    //                 _ => {}
    //             }

    //             match (&old_ssl_conf.client_cert, &new_ssl_conf.client_cert) {
    //                 (Some(old_client_cert), Some(new_client_cert)) => {
    //                     if old_client_cert != new_client_cert {
    //                         return true;
    //                     }
    //                 }
    //                 (None, Some(_)) => return true,
    //                 (Some(_), None) => return true,
    //                 _ => {}
    //             }

    //             match (&old_ssl_conf.client_key, &new_ssl_conf.client_key) {
    //                 (Some(old_client_key), Some(new_client_key)) => {
    //                     if old_client_key != new_client_key {
    //                         return true;
    //                     }
    //                 }
    //                 (None, Some(_)) => return true,
    //                 (Some(_), None) => return true,
    //                 _ => {}
    //             }

    //             return false;
    //         }
    //         (None, Some(_)) => true,
    //         (Some(_), None) => true,
    //         _ => false,
    //     }
    // }

    // pub async fn update(&mut self, req: GroupUpdateReq) -> Result<()> {
    //     self.check_stopped()?;

    //     let need_update_client = Self::check_update_client(&self.conf, &req);

    //     let client_group_conf = Arc::new(ClientGroupConf {
    //         port: req.port,
    //         ssl_conf: req.ssl_conf.clone(),
    //     });

    //     match self.conf.client_count.cmp(&req.client_count) {
    //         std::cmp::Ordering::Less => {
    //             let diff = req.client_count - self.conf.client_count;
    //             if need_update_client {
    //                 for client in self.clients.write().await.iter_mut() {
    //                     client.update(client_group_conf.clone()).await;
    //                 }
    //             }

    //             let new_clients = Self::new_clients(
    //                 &self.id,
    //                 self.conf.client_count,
    //                 diff,
    //                 &self.broker_info,
    //                 &self.conf,
    //                 client_group_conf,
    //                 &self.client_metrics,
    //                 &self.packet_metrics,
    //             );
    //             self.clients.write().await.extend(new_clients);
    //         }
    //         std::cmp::Ordering::Equal => {
    //             // TODO client_id 变更问题
    //             if need_update_client {
    //                 for client in self.clients.write().await.iter_mut() {
    //                     client.update(client_group_conf.clone()).await;
    //                 }
    //             }
    //         }
    //         std::cmp::Ordering::Greater => {
    //             let diff = self.conf.client_count - req.client_count;
    //             let mut client_guards = self.clients.write().await;
    //             for _ in 0..diff {
    //                 client_guards.pop().unwrap().stop().await;
    //             }
    //         }
    //     }

    //     self.conf.name = req.name;
    //     self.conf.client_count = req.client_count;
    //     self.conf.port = req.port;
    //     self.conf.ssl_conf = req.ssl_conf.clone();

    //     Ok(())
    // }

    pub async fn create_publish(&mut self, req: PublishCreateUpdateReq) -> Result<()> {
        self.check_stopped()?;
        let req2 = req.clone();
        let id = Arc::new(generate_id());

        let payload = match (req.size, req.payload) {
            (None, Some(payload)) => payload.into(),
            (Some(size), None) => {
                let mut payload = Vec::with_capacity(size);
                for _ in 0..size {
                    payload.push(0);
                }
                payload
            }
            _ => bail!("请指定 payload 或 size"),
        };

        let conf = Arc::new(PublishConf {
            name: req.name,
            topic: req.topic,
            qos: req.qos,
            retain: req.retain,
            tps: req.tps,
            payload: Arc::new(payload),
            v311: None,
            v50: None,
        });

        // self.clients.write().await.iter_mut().for_each(|client| {
        //     client.create_publish(id.clone(), conf.clone());
        // });

        self.publishes.push((id, req2));
        Ok(())
    }

    pub async fn list_publishes(&self) -> ListPublishResp {
        let mut list = Vec::with_capacity(self.publishes.len());
        for (id, conf) in self.publishes.iter() {
            list.push(ListPublishRespItem {
                id: (**id).clone(),
                conf: conf.clone(),
            });
        }

        ListPublishResp { list }
    }

    pub async fn update_publish(
        &mut self,
        publish_id: String,
        req: PublishCreateUpdateReq,
    ) -> Result<()> {
        todo!()
        // self.check_stopped()?;
        // let req2 = req.clone();
        // let payload = match (req.size, req.payload) {
        //     (None, Some(payload)) => payload.into(),
        //     (Some(size), None) => {
        //         let mut payload = Vec::with_capacity(size);
        //         for _ in 0..size {
        //             payload.push(0);
        //         }
        //         payload
        //     }
        //     _ => bail!("请指定 payload 或 size"),
        // };
        // let conf = Arc::new(PublishConf {
        //     name: req.name,
        //     topic: req.topic,
        //     qos: req.qos,
        //     retain: req.retain,
        //     tps: req.tps,
        //     payload: Arc::new(payload),
        //     v311: None,
        //     v50: None,
        // });
        // for client in self.clients.write().await.iter_mut() {
        //     client.update_publish(&publish_id, conf.clone());
        // }
        // self.publishes
        //     .iter_mut()
        //     .find(|(id, _)| **id == publish_id)
        //     .unwrap()
        //     .1 = req2;
        // Ok(())
    }

    pub async fn delete_publish(&mut self, publish_id: String) -> Result<()> {
        todo!()
        // self.check_stopped()?;
        // for client in self.clients.write().await.iter_mut() {
        //     client.delete_publish(&publish_id);
        // }
        // self.subscribes.retain(|(id, _)| **id != publish_id);
        // Ok(())
    }

    pub async fn create_subscribe(&mut self, req: SubscribeCreateUpdateReq) -> Result<()> {
        let id = Arc::new(generate_id());
        let conf = Arc::new(req);
        self.subscribes.push((id, conf));
        Ok(())
    }

    pub async fn list_subscribes(&self) -> ListSubscribeResp {
        let mut list = Vec::with_capacity(self.subscribes.len());
        for (id, conf) in self.subscribes.iter() {
            list.push(ListSubscribeRespItem {
                id: (**id).clone(),
                conf: (**conf).clone(),
            });
        }
        ListSubscribeResp { list }
    }

    pub async fn update_subscribe(
        &mut self,
        subscribe_id: String,
        req: SubscribeCreateUpdateReq,
    ) -> Result<()> {
        todo!()
        // self.check_stopped()?;
        // let conf = Arc::new(req);
        // for client in self.clients.write().await.iter_mut() {
        //     client.update_subscribe(&subscribe_id, conf.clone()).await;
        // }
        // Ok(())
    }

    pub async fn delete_subscribe(&mut self, subscribe_id: String) -> Result<()> {
        todo!()
        // self.check_stopped()?;
        // for client in self.clients.write().await.iter_mut() {
        //     client.delete_subscribe(&subscribe_id).await;
        // }
        // self.subscribes.retain(|(id, _)| **id != subscribe_id);
        // Ok(())
    }

    pub async fn list_clients(&self, query: ClientsQueryParams) -> ClientsListResp {
        todo!()
        // let mut list = vec![];
        // let offset = (query.p - 1) * query.s;
        // let mut i = 0;
        // for client in self.clients.read().await.iter().skip(offset) {
        //     i += 1;
        //     if i >= query.s {
        //         break;
        //     }
        //     list.push(client.read().await);
        // }
        // ClientsListResp {
        //     count: self.clients.read().await.len(),
        //     list,
        // }
    }

    pub async fn read_metrics(&self, query: MetricsQueryParams) -> MetricsListResp {
        match (query.start_time, query.end_time) {
            (None, None) => {
                let mut list = vec![];
                let mut conn_ack_total = 0;
                let mut pub_ack_total = 0;
                let mut unsub_ack_total = 0;
                let mut ping_req_total = 0;
                let mut ping_resp_total = 0;
                let mut outgoing_publish_total = 0;
                let mut incoming_publish_total = 0;
                let mut pub_rel_total = 0;
                let mut pub_rec_total = 0;
                let mut pub_comp_total = 0;
                let mut subscribe_total = 0;
                let mut sub_ack_total = 0;
                let mut unsubscribe_total = 0;
                let mut disconnect_total = 0;
                for metric in self.history_metrics.as_ref().unwrap().lock().await.iter() {
                    conn_ack_total += metric.2.conn_ack;
                    pub_ack_total += metric.2.pub_ack;
                    unsub_ack_total += metric.2.unsub_ack;
                    ping_req_total += metric.2.ping_req;
                    ping_resp_total += metric.2.ping_resp;
                    outgoing_publish_total += metric.2.outgoing_publish;
                    incoming_publish_total += metric.2.incoming_publish;
                    pub_rel_total += metric.2.pub_rel;
                    pub_rec_total += metric.2.pub_rec;
                    pub_comp_total += metric.2.pub_comp;
                    subscribe_total += metric.2.subscribe;
                    sub_ack_total += metric.2.sub_ack;
                    unsubscribe_total += metric.2.unsubscribe;
                    disconnect_total += metric.2.disconnect;

                    list.push(MetricsListItem {
                        ts: metric.0,
                        client: ClientMetrics {
                            running_cnt: metric.1.running_cnt,
                            error_cnt: metric.1.error_cnt,
                            stopped_cnt: metric.1.stopped_cnt,
                            waiting_cnt: metric.1.waiting_cnt,
                        },
                        packet: PacketMetrics {
                            conn_ack_total,
                            conn_ack_cnt: metric.2.conn_ack,
                            pub_ack_total,
                            pub_ack_cnt: metric.2.pub_ack,
                            unsub_ack_total,
                            unsub_ack_cnt: metric.2.unsub_ack,
                            ping_req_total,
                            ping_req_cnt: metric.2.ping_req,
                            ping_resp_total,
                            ping_resp_cnt: metric.2.ping_resp,
                            outgoing_publish_total,
                            outgoing_publish_cnt: metric.2.outgoing_publish,
                            incoming_publish_total,
                            incoming_publish_cnt: metric.2.incoming_publish,
                            pub_rel_total,
                            pub_rel_cnt: metric.2.pub_rel,
                            pub_rec_total,
                            pub_rec_cnt: metric.2.pub_rec,
                            pub_comp_total,
                            pub_comp_cnt: metric.2.pub_comp,
                            subscribe_total,
                            subscribe_cnt: metric.2.subscribe,
                            sub_ack_total,
                            sub_ack_cnt: metric.2.sub_ack,
                            unsubscribe_total,
                            unsubscribe_cnt: metric.2.unsubscribe,
                            disconnect_total,
                            disconnect_cnt: metric.2.disconnect,
                        },
                    });
                }
                MetricsListResp { list }
            }
            (None, Some(_)) => todo!(),
            (Some(_), None) => todo!(),
            (Some(_), Some(_)) => todo!(),
        }
    }

    pub async fn update_status(&mut self, status: Status) {
        self.status = status;
        let client_status = match status {
            Status::Starting => Status::Waiting,
            Status::Stopped => Status::Stopped,
            Status::Waiting => Status::Waiting,
            _ => {
                return;
            }
        };

        for client in self.clients.iter() {
            // client.update_status(client_status);
            todo!()
        }
    }

    fn check_stopped(&self) -> Result<()> {
        match self.status {
            Status::Stopped => Ok(()),
            _ => bail!("请先停止组后再进行操作！"),
        }
    }
}

enum Template {
    None,
    Index,
    GroupId,
    Uuid,
    IndexGroupId,
    IndexUuid,
    UuidGroupId,
    IndexGroupIdUuid,
    Range,
}

fn parse_template(template_str: &str) -> Template {
    let mut has_index = false;
    if template_str.contains("{index}") {
        has_index = true;
    }

    let mut has_group_id = false;
    if template_str.contains("{group_id}") {
        has_group_id = true;
    }

    let mut has_uuid = false;
    if template_str.contains("{uuid}") {
        has_uuid = true;
    }

    if template_str.contains("..") {
        return Template::Range;
    }

    match (has_index, has_group_id, has_uuid) {
        (true, true, true) => Template::IndexGroupIdUuid,
        (true, true, false) => Template::IndexGroupId,
        (true, false, true) => Template::IndexUuid,
        (true, false, false) => Template::Index,
        (false, true, true) => Template::UuidGroupId,
        (false, true, false) => Template::GroupId,
        (false, false, true) => Template::Uuid,
        (false, false, false) => Template::None,
    }
}
