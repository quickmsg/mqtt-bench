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
    BrokerUpdateReq, ClientsListResp, ClientsQueryParams, GroupCreateReq, GroupMetrics,
    GroupUpdateReq, ListPublishResp, ListPublishRespItem, ListSubscribeResp, ListSubscribeRespItem,
    PublishCreateUpdateReq, ReadGroupResp, SslConf, SubscribeCreateUpdateReq,
};
use uuid::Uuid;

use crate::{
    client::{self, Client},
    generate_id, AtomicMetrics,
};

pub struct Group {
    pub id: String,
    pub conf: GroupCreateReq,
    running: bool,

    clients: Arc<RwLock<Vec<Box<dyn Client>>>>,
    client_connected_count: Arc<AtomicUsize>,

    status: Option<BiLock<Vec<GroupMetrics>>>,
    broker_info: Arc<BrokerUpdateReq>,
    stop_signal_tx: tokio::sync::broadcast::Sender<()>,

    publishes: Vec<(Arc<String>, Arc<PublishCreateUpdateReq>)>,
    subscribes: Vec<(Arc<String>, Arc<SubscribeCreateUpdateReq>)>,

    metrics: Arc<AtomicMetrics>,
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

        let metrics = Arc::new(AtomicMetrics::default());

        let clients = Self::new_clients(
            &id,
            0,
            group_conf.client_count,
            &broker_info,
            &group_conf,
            client_group_conf,
            &metrics,
        );

        let (stop_signal_tx, _) = tokio::sync::broadcast::channel(1);
        Self {
            id,
            conf: req,
            running: false,
            clients: Arc::new(RwLock::new(clients)),
            status: None,
            broker_info,
            client_connected_count: Arc::new(AtomicUsize::new(0)),
            stop_signal_tx,
            publishes: vec![],
            subscribes: vec![],
            metrics,
        }
    }

    fn new_clients(
        group_id: &String,
        offset: usize,
        client_count: usize,
        broker_info: &Arc<BrokerUpdateReq>,
        group_conf: &GroupCreateReq,
        client_group_conf: Arc<ClientGroupConf>,
        metrics: &Arc<AtomicMetrics>,
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
            let local_ip = match &group_conf.local_ips {
                Some(ips) => Some(ips[index % ips.len()].clone()),
                None => None,
            };
            let client_conf = client::ClientConf {
                index,
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
                        metrics.clone(),
                    ));
                }
                (types::Protocol::Mqtt, types::ProtocolVersion::V50) => {
                    clients.push(client::mqtt_v50::new(
                        client_conf,
                        client_group_conf.clone(),
                    ));
                }
                (types::Protocol::Websocket, types::ProtocolVersion::V311) => {
                    clients.push(client::websocket_v311::new(
                        client_conf,
                        client_group_conf.clone(),
                    ));
                }
                (types::Protocol::Websocket, types::ProtocolVersion::V50) => {
                    clients.push(client::websocket_v50::new(
                        client_conf,
                        client_group_conf.clone(),
                    ));
                }
                (types::Protocol::Http, _) => {
                    todo!()
                }
            }
        }
        clients
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
        // let clients = self.clients.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        break;
                    }

                    _ = status_interval.tick() => {
                        Self::collect_status(&metrics, &status).await;
                    }
                }
            }
        });
    }

    async fn collect_status(
        metrics: &Arc<AtomicMetrics>,
        group_status: &BiLock<Vec<GroupMetrics>>,
    ) {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // let mut succeed = 0;
        // let mut failed = 0;
        // let mut packet_metrics = PacketMetrics::default();
        // {
        //     let clients_guard = clients.read().await;
        //     clients_guard.iter().for_each(|client| {
        //         let client_metrics = client.get_metrics();
        //         if client_metrics.success {
        //             succeed += 1;
        //         } else {
        //             failed += 1;
        //         }
        //         packet_metrics.conn_ack += client_metrics.usize_metrics.conn_ack;
        //         packet_metrics.pub_ack += client_metrics.usize_metrics.pub_ack;
        //         packet_metrics.unsub_ack += client_metrics.usize_metrics.unsub_ack;
        //         packet_metrics.ping_req += client_metrics.usize_metrics.ping_req;
        //         packet_metrics.ping_resp += client_metrics.usize_metrics.ping_resp;
        //         packet_metrics.publish += client_metrics.usize_metrics.incoming_publish;
        //         packet_metrics.subscribe += client_metrics.usize_metrics.subscribe;
        //         packet_metrics.unsubscribe += client_metrics.usize_metrics.unsubscribe;
        //         packet_metrics.disconnect += client_metrics.usize_metrics.disconnect;
        //     });
        // }

        let usize_metrics = metrics.take_metrics();
        group_status.lock().await.push(GroupMetrics {
            ts,
            succeed: 0,
            failed: 0,
            usize_metrics,
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
            conf: self.conf.clone(),
            status: self.status.as_ref().unwrap().lock().await.clone(),
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

    pub async fn update(&mut self, req: GroupUpdateReq) {
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
                    &self.metrics,
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
    }

    pub async fn create_publish(&mut self, req: PublishCreateUpdateReq) {
        let id = Arc::new(generate_id());
        let conf = Arc::new(req);
        self.clients.write().await.iter_mut().for_each(|client| {
            client.create_publish(id.clone(), conf.clone());
        });

        self.publishes.push((id, conf));
    }

    pub async fn list_publishes(&self) -> ListPublishResp {
        let mut list = Vec::with_capacity(self.publishes.len());
        for (id, conf) in self.publishes.iter() {
            list.push(ListPublishRespItem {
                id: (**id).clone(),
                conf: (**conf).clone(),
            });
        }

        ListPublishResp { list }
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

    pub async fn list_clients(&self, query: ClientsQueryParams) -> ClientsListResp {
        let mut list = vec![];
        let offset = (query.p - 1) * query.l;
        let mut i = 0;
        for client in self.clients.read().await.iter().skip(offset) {
            i += 1;
            if i >= query.l {
                break;
            }
            list.push(client.read().await);
        }
        ClientsListResp {
            count: self.clients.read().await.len(),
            list,
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
