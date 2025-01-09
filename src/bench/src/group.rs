use std::{
    sync::{atomic::AtomicU32, Arc},
    time::{Duration, SystemTime},
};

use anyhow::{bail, Result};
use bytes::Bytes;
use futures::lock::BiLock;
use tokio::{
    select,
    sync::{mpsc, oneshot, RwLock},
    time,
};
use tracing::{debug, info};
use types::{
    BrokerUpdateReq, ClientMetrics, ClientUsizeMetrics, ClientsListResp, ClientsQueryParams,
    GroupCreateReq, GroupUpdateReq, ListPublishResp, ListPublishRespItem, ListSubscribeResp,
    ListSubscribeRespItem, MetricsListItem, MetricsListResp, MetricsQueryParams, PacketMetrics,
    PacketUsizeMetrics, PublishConf, PublishCreateUpdateReq, ReadGroupResp, SslConf, Status,
    SubscribeCreateUpdateReq,
};
use uuid::Uuid;

use crate::{
    client::{self, Client},
    generate_id, ClientAtomicMetrics, PacketAtomicMetrics,
};

// 运行中不允许更新，降低复杂度
pub struct Group {
    pub id: String,
    pub status: Status,
    pub conf: GroupCreateReq,

    clients: Arc<RwLock<Vec<Box<dyn Client>>>>,

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
    pub fn new(id: String, broker_info: Arc<BrokerUpdateReq>, req: GroupCreateReq) -> Self {
        // TODO 优化req clone
        let group_conf = Arc::new(req.clone());
        let client_group_conf = Arc::new(ClientGroupConf {
            port: req.port,
            ssl_conf: req.ssl_conf.clone(),
        });

        let client_metrics = Arc::new(ClientAtomicMetrics::default());
        let packet_metrics = Arc::new(PacketAtomicMetrics::default());

        let clients = Self::new_clients(
            &id,
            0,
            group_conf.client_count,
            &broker_info,
            &group_conf,
            client_group_conf,
            &client_metrics,
            &packet_metrics,
        );

        let (stop_signal_tx, _) = tokio::sync::broadcast::channel(1);
        Self {
            id,
            status: Status::Stopped,
            conf: req,
            clients: Arc::new(RwLock::new(clients)),
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
        offset: usize,
        client_count: usize,
        broker_info: &Arc<BrokerUpdateReq>,
        group_conf: &GroupCreateReq,
        client_group_conf: Arc<ClientGroupConf>,
        client_metrics: &Arc<ClientAtomicMetrics>,
        packet_metrics: &Arc<PacketAtomicMetrics>,
    ) -> Vec<Box<dyn Client>> {
        let mut clients = Vec::with_capacity(client_count);

        let client_id_template = parse_id(&group_conf.client_id);

        for index in offset..client_count + offset {
            let client_id = match client_id_template {
                ClientIdTemplate::None => group_conf.client_id.clone(),
                ClientIdTemplate::Index => {
                    group_conf.client_id.replace("${index}", &index.to_string())
                }
                ClientIdTemplate::GroupId => group_conf.client_id.replace("${group_id}", group_id),
                ClientIdTemplate::Uuid => group_conf
                    .client_id
                    .replace("${uuid}", &Uuid::new_v4().to_string()),
                ClientIdTemplate::IndexGroupId => group_conf
                    .client_id
                    .replace("${index}", &index.to_string())
                    .replace("${group_id}", group_id),
                ClientIdTemplate::IndexUuid => group_conf
                    .client_id
                    .replace("${index}", &index.to_string())
                    .replace("${uuid}", &Uuid::new_v4().to_string()),
                ClientIdTemplate::UuidGroupId => group_conf
                    .client_id
                    .replace("${uuid}", &Uuid::new_v4().to_string())
                    .replace("${group_id}", group_id),
                ClientIdTemplate::IndexGroupIdUuid => group_conf
                    .client_id
                    .replace("${index}", &index.to_string())
                    .replace("${group_id}", group_id)
                    .replace("${uuid}", &Uuid::new_v4().to_string()),
            };
            let local_ip = match &broker_info.local_ips {
                Some(ips) => Some(ips[index % ips.len()].clone()),
                None => None,
            };
            let client_conf = client::ClientConf {
                id: client_id,
                host: broker_info.hosts[index % broker_info.hosts.len()].clone(),
                keep_alive: 60,
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
                    clients.push(client::mqtt_v50::new(
                        client_conf,
                        client_group_conf.clone(),
                        client_metrics.clone(),
                        packet_metrics.clone(),
                    ));
                }
                (types::Protocol::Websocket, types::ProtocolVersion::V311) => {
                    clients.push(client::websocket_v311::new(
                        client_conf,
                        client_group_conf.clone(),
                        client_metrics.clone(),
                        packet_metrics.clone(),
                    ));
                }
                (types::Protocol::Websocket, types::ProtocolVersion::V50) => {
                    clients.push(client::websocket_v50::new(
                        client_conf,
                        client_group_conf.clone(),
                        client_metrics.clone(),
                        packet_metrics.clone(),
                    ));
                }
                (types::Protocol::Http, _) => {
                    todo!()
                }
            }
        }
        clients
    }

    pub async fn start(&mut self, job_finished_signal_tx: mpsc::UnboundedSender<()>) {
        match self.status {
            Status::Starting | Status::Running => return,
            Status::Stopped | Status::Waiting | Status::Updating => {
                self.status = Status::Starting;
            }
        }

        let (history_metrics_1, history_metrics_2) = BiLock::new(Vec::new());
        self.start_collect_metrics(history_metrics_1, self.broker_info.statistics_interval);

        let (tx, rx) = oneshot::channel::<()>();

        self.history_metrics = Some(history_metrics_2);
        self.start_clients(job_finished_signal_tx, tx);

        let mill_cnt = self.publishes[0].1.tps / 1000;
        debug!("mill cnt {:?}", mill_cnt);
        let mut interval = tokio::time::interval(Duration::from_millis(1));
        let pulish = self.publishes[0].1.clone();

        let topic = pulish.topic.clone();
        let qos = match pulish.qos {
            types::Qos::AtMostOnce => mqtt::protocol::v3_mini::QoS::AtMostOnce,
            types::Qos::AtLeastOnce => mqtt::protocol::v3_mini::QoS::AtLeastOnce,
            types::Qos::ExactlyOnce => mqtt::protocol::v3_mini::QoS::ExactlyOnce,
        };

        let payload = match (pulish.size, pulish.payload) {
            (None, Some(payload)) => payload.into(),
            (Some(size), None) => {
                let mut payload = Bytes::new();
                for _ in 0..size {}
                payload
            }
            _ => panic!("请指定 payload 或 size"),
        };
        let payload = Arc::new(payload);
        let client_pos = 0;
        let client_len = self.clients.read().await.len();
        rx.await.unwrap();
        loop {
            select! {
                _ = interval.tick() => {
                    Self::client_publish(&self.clients, client_pos, mill_cnt, &topic, qos, &payload, client_len).await;
                }
            }
        }
    }

    async fn client_publish(
        clients: &Arc<RwLock<Vec<Box<dyn Client>>>>,
        mut client_pos: usize,
        mill_cnt: usize,
        topic: &String,
        qos: mqtt::protocol::v3_mini::QoS,
        payload: &Arc<Bytes>,
        client_len: usize,
    ) {
        for _ in 0..mill_cnt {
            clients.read().await[client_pos]
                .publish(topic.clone(), qos, payload.clone())
                .await;
            client_pos += 1;
            client_pos %= client_len;
        }
    }

    pub async fn stop(&mut self) {
        match self.status {
            Status::Stopped => return,
            Status::Starting | Status::Running | Status::Waiting | Status::Updating => {
                self.status = Status::Stopped;
            }
        }

        for client in self.clients.write().await.iter_mut() {
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
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        break;
                    }

                    _ = status_interval.tick() => {
                        Self::collect_metrics(&client_metrics,&packet_metrics, &history_metrics, &client_running_cnt).await;
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
    ) {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
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
        info!("pakcet_usize_metrics: {:?}", pakcet_usize_metrics);
        history_metrics
            .lock()
            .await
            .push((ts, client_usize_metrics, pakcet_usize_metrics));
    }

    fn start_clients(
        &mut self,
        job_finished_signal_tx: mpsc::UnboundedSender<()>,
        tx: oneshot::Sender<()>,
    ) {
        let clients = self.clients.clone();
        let mut connect_interval = time::interval(time::Duration::from_millis(
            self.broker_info.connect_interval,
        ));
        let client_count = self.conf.client_count;
        let mut stop_signal_rx = self.stop_signal_tx.subscribe();
        let mut index = 0;
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        break;
                    }

                    _ = connect_interval.tick() => {
                        if index < client_count {
                            clients.write().await[index].start().await;
                            index += 1;
                        } else {
                            job_finished_signal_tx.send(()).unwrap();
                            tx.send(()).unwrap();
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
            conf: self.conf.clone(),
        }
    }

    fn check_update_client(old_conf: &GroupCreateReq, new_conf: &GroupUpdateReq) -> bool {
        if old_conf.port != new_conf.port {
            return true;
        }

        if old_conf.client_id != new_conf.client_id {
            return true;
        }

        match (&old_conf.ssl_conf, &new_conf.ssl_conf) {
            (Some(old_ssl_conf), Some(new_ssl_conf)) => {
                if old_ssl_conf.verify != new_ssl_conf.verify {
                    return true;
                }

                match (&old_ssl_conf.ca_cert, &new_ssl_conf.ca_cert) {
                    (Some(old_ca_cert), Some(new_ca_cert)) => {
                        if old_ca_cert != new_ca_cert {
                            return true;
                        }
                    }
                    (None, Some(_)) => return true,
                    (Some(_), None) => return true,
                    _ => {}
                }

                match (&old_ssl_conf.client_cert, &new_ssl_conf.client_cert) {
                    (Some(old_client_cert), Some(new_client_cert)) => {
                        if old_client_cert != new_client_cert {
                            return true;
                        }
                    }
                    (None, Some(_)) => return true,
                    (Some(_), None) => return true,
                    _ => {}
                }

                match (&old_ssl_conf.client_key, &new_ssl_conf.client_key) {
                    (Some(old_client_key), Some(new_client_key)) => {
                        if old_client_key != new_client_key {
                            return true;
                        }
                    }
                    (None, Some(_)) => return true,
                    (Some(_), None) => return true,
                    _ => {}
                }

                return false;
            }
            (None, Some(_)) => true,
            (Some(_), None) => true,
            _ => false,
        }
    }

    pub async fn update(&mut self, req: GroupUpdateReq) -> Result<()> {
        self.check_stopped()?;

        let need_update_client = Self::check_update_client(&self.conf, &req);

        let client_group_conf = Arc::new(ClientGroupConf {
            port: req.port,
            ssl_conf: req.ssl_conf.clone(),
        });

        match self.conf.client_count.cmp(&req.client_count) {
            std::cmp::Ordering::Less => {
                let diff = req.client_count - self.conf.client_count;
                if need_update_client {
                    for client in self.clients.write().await.iter_mut() {
                        client.update(client_group_conf.clone()).await;
                    }
                }

                let new_clients = Self::new_clients(
                    &self.id,
                    self.conf.client_count,
                    diff,
                    &self.broker_info,
                    &self.conf,
                    client_group_conf,
                    &self.client_metrics,
                    &self.packet_metrics,
                );
                self.clients.write().await.extend(new_clients);
            }
            std::cmp::Ordering::Equal => {
                // TODO client_id 变更问题
                if need_update_client {
                    for client in self.clients.write().await.iter_mut() {
                        client.update(client_group_conf.clone()).await;
                    }
                }
            }
            std::cmp::Ordering::Greater => {
                let diff = self.conf.client_count - req.client_count;
                let mut client_guards = self.clients.write().await;
                for _ in 0..diff {
                    client_guards.pop().unwrap().stop().await;
                }
            }
        }

        self.conf.name = req.name;
        self.conf.client_count = req.client_count;
        self.conf.port = req.port;
        self.conf.ssl_conf = req.ssl_conf.clone();

        Ok(())
    }

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
        self.check_stopped()?;
        let req2 = req.clone();
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
        for client in self.clients.write().await.iter_mut() {
            client.update_publish(&publish_id, conf.clone());
        }
        self.publishes
            .iter_mut()
            .find(|(id, _)| **id == publish_id)
            .unwrap()
            .1 = req2;
        Ok(())
    }

    pub async fn delete_publish(&mut self, publish_id: String) -> Result<()> {
        self.check_stopped()?;
        for client in self.clients.write().await.iter_mut() {
            client.delete_publish(&publish_id);
        }
        self.subscribes.retain(|(id, _)| **id != publish_id);
        Ok(())
    }

    pub async fn create_subscribe(&mut self, req: SubscribeCreateUpdateReq) -> Result<()> {
        self.check_stopped()?;
        let id = Arc::new(generate_id());
        let conf = Arc::new(req);
        for client in self.clients.write().await.iter_mut() {
            client.create_subscribe(id.clone(), conf.clone()).await;
        }

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
        self.check_stopped()?;
        let conf = Arc::new(req);
        for client in self.clients.write().await.iter_mut() {
            client.update_subscribe(&subscribe_id, conf.clone()).await;
        }
        Ok(())
    }

    pub async fn delete_subscribe(&mut self, subscribe_id: String) -> Result<()> {
        self.check_stopped()?;
        for client in self.clients.write().await.iter_mut() {
            client.delete_subscribe(&subscribe_id).await;
        }
        self.subscribes.retain(|(id, _)| **id != subscribe_id);
        Ok(())
    }

    pub async fn list_clients(&self, query: ClientsQueryParams) -> ClientsListResp {
        let mut list = vec![];
        let offset = (query.p - 1) * query.s;
        let mut i = 0;
        for client in self.clients.read().await.iter().skip(offset) {
            i += 1;
            if i >= query.s {
                break;
            }
            list.push(client.read().await);
        }
        ClientsListResp {
            count: self.clients.read().await.len(),
            list,
        }
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

        for client in self.clients.write().await.iter_mut() {
            client.update_status(client_status);
        }
    }

    fn check_stopped(&self) -> Result<()> {
        match self.status {
            Status::Stopped => Ok(()),
            _ => bail!("请先停止组后再进行操作！"),
        }
    }
}

enum ClientIdTemplate {
    None,
    Index,
    GroupId,
    Uuid,
    IndexGroupId,
    IndexUuid,
    UuidGroupId,
    IndexGroupIdUuid,
}

fn parse_id(id: &str) -> ClientIdTemplate {
    let mut has_index = false;
    if id.contains("${index}") {
        has_index = true;
    }

    let mut has_group_id = false;
    if id.contains("${group_id}") {
        has_group_id = true;
    }

    let mut has_uuid = false;
    if id.contains("${uuid}") {
        has_uuid = true;
    }

    match (has_index, has_group_id, has_uuid) {
        (true, true, true) => ClientIdTemplate::IndexGroupIdUuid,
        (true, true, false) => ClientIdTemplate::IndexGroupId,
        (true, false, true) => ClientIdTemplate::IndexUuid,
        (true, false, false) => ClientIdTemplate::Index,
        (false, true, true) => ClientIdTemplate::UuidGroupId,
        (false, true, false) => ClientIdTemplate::GroupId,
        (false, false, true) => ClientIdTemplate::Uuid,
        (false, false, false) => ClientIdTemplate::None,
    }
}
