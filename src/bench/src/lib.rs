use std::{
    collections::VecDeque,
    sync::{atomic::AtomicUsize, Arc, LazyLock},
};

use anyhow::{bail, Result};
use futures::lock::BiLock;
use group::Group;
use tokio::sync::{mpsc, RwLock};
use tracing::debug;
use types::{
    BrokerUpdateReq, ClientUsizeMetrics, ClientsListResp, ClientsQueryParams, GroupCreateReq,
    GroupListResp, GroupListRespItem, GroupUpdateReq, ListPublishResp, ListSubscribeResp,
    MetricsListResp, MetricsQueryParams, PacketUsizeMetrics, ReadGroupResp, Status,
    SubscribeCreateUpdateReq,
};
use uuid::Uuid;

mod client;
mod group;

static RUNTIME_INSTANCE: LazyLock<RuntimeInstance> = LazyLock::new(|| Default::default());
static TASK_QUEUE: LazyLock<TaskQueue> = LazyLock::new(|| TaskQueue::new());

struct TaskQueue {
    get_task_signal_tx: mpsc::UnboundedSender<()>,
    queue: BiLock<VecDeque<String>>,
}

impl TaskQueue {
    pub fn new() -> Self {
        let (queue1, queue2) = BiLock::new(VecDeque::new());
        let (get_task_signal_tx, get_task_signal_rx) = mpsc::unbounded_channel();
        Self::start(queue1, get_task_signal_rx);
        Self {
            get_task_signal_tx,
            queue: queue2,
        }
    }

    pub async fn put_task(&self, group_id: String) {
        RUNTIME_INSTANCE
            .groups
            .write()
            .await
            .iter_mut()
            .find(|g| g.id == group_id)
            .unwrap()
            .update_status(types::Status::Waiting)
            .await;
        self.queue.lock().await.push_back(group_id);
        self.get_task_signal_tx.send(()).unwrap();
    }

    pub fn start(
        queue: BiLock<VecDeque<String>>,
        mut get_task_signal_rx: mpsc::UnboundedReceiver<()>,
    ) {
        let (job_finished_signal_tx, mut job_finished_signal_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                get_task_signal_rx.recv().await;
                if let Some(group_id) = queue.lock().await.pop_front() {
                    RUNTIME_INSTANCE
                        .groups
                        .write()
                        .await
                        .iter_mut()
                        .find(|g| g.id == group_id)
                        .unwrap()
                        .start(job_finished_signal_tx.clone())
                        .await;
                    debug!("group {} starting", group_id);
                    job_finished_signal_rx.recv().await;
                    debug!("group  finisned",);
                    RUNTIME_INSTANCE
                        .groups
                        .write()
                        .await
                        .iter_mut()
                        .find(|g| g.id == group_id)
                        .unwrap()
                        .update_status(types::Status::Running)
                        .await;
                }
            }
        });
    }
}

fn generate_id() -> String {
    Uuid::new_v4().to_string()
}

pub struct RuntimeInstance {
    pub broker_info: RwLock<Arc<BrokerUpdateReq>>,
    pub groups: RwLock<Vec<Group>>,
}

impl Default for RuntimeInstance {
    fn default() -> Self {
        Self {
            broker_info: RwLock::new(Arc::new(BrokerUpdateReq {
                hosts: vec!["127.0.0.1".into()],
                username: None,
                password: None,
                client_id: None,
                connect_interval: 1,
                statistics_interval: 1,
                local_ips: None,
            })),
            groups: RwLock::new(vec![]),
        }
    }
}

pub async fn read_broker() -> BrokerUpdateReq {
    let broker_info = RUNTIME_INSTANCE.broker_info.read().await.clone();
    (*broker_info).clone()
}

pub async fn update_broker(info: BrokerUpdateReq) -> Result<()> {
    if RUNTIME_INSTANCE
        .groups
        .read()
        .await
        .iter()
        .any(|group| group.status != Status::Stopped)
    {
        bail!("请停止所有的组后再进行修改！");
    }

    *RUNTIME_INSTANCE.broker_info.write().await = Arc::new(info);
    Ok(())
}

pub async fn create_group(req: GroupCreateReq) {
    let group = Group::new(
        generate_id(),
        RUNTIME_INSTANCE.broker_info.read().await.clone(),
        req,
    );
    RUNTIME_INSTANCE.groups.write().await.push(group);
}

pub async fn list_groups() -> GroupListResp {
    let list: Vec<_> = RUNTIME_INSTANCE
        .groups
        .read()
        .await
        .iter()
        .rev()
        .map(|group| GroupListRespItem {
            id: group.id.clone(),
            status: group.status,
            conf: group.conf.clone(),
        })
        .collect();
    GroupListResp { list }
}

pub async fn read_group(group_id: String) -> ReadGroupResp {
    RUNTIME_INSTANCE
        .groups
        .read()
        .await
        .iter()
        .find(|group| group.id == group_id)
        .unwrap()
        .read()
        .await
}

pub async fn update_group(group_id: String, req: GroupUpdateReq) -> Result<()> {
    RUNTIME_INSTANCE
        .groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
        .unwrap()
        .update(req)
        .await
}

pub async fn delete_group(group_id: String) {
    RUNTIME_INSTANCE
        .groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
        .unwrap()
        .stop()
        .await;

    RUNTIME_INSTANCE
        .groups
        .write()
        .await
        .retain(|group| group.id != group_id);
}

pub async fn start_group(group_id: String) {
    debug!("start group: {}", group_id);
    TASK_QUEUE.put_task(group_id.clone()).await;
}

pub async fn stop_group(group_id: String) {
    RUNTIME_INSTANCE
        .groups
        .write()
        .await
        .iter_mut()
        .find(|g| g.id == group_id)
        .unwrap()
        .stop()
        .await;
}

pub async fn create_publish(group_id: String, req: types::PublishCreateUpdateReq) -> Result<()> {
    RUNTIME_INSTANCE
        .groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
        .unwrap()
        .create_publish(req)
        .await
}

pub async fn list_publishes(group_id: String) -> Result<ListPublishResp> {
    match RUNTIME_INSTANCE
        .groups
        .read()
        .await
        .iter()
        .find(|group| group.id == group_id)
    {
        Some(group) => Ok(group.list_publishes().await),
        None => bail!("group not found"),
    }
}

pub async fn update_publish(
    group_id: String,
    publish_id: String,
    req: types::PublishCreateUpdateReq,
) -> Result<()> {
    RUNTIME_INSTANCE
        .groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
        .unwrap()
        .update_publish(publish_id, req)
        .await
}

pub async fn delete_publish(group_id: String, publish_id: String) -> Result<()> {
    RUNTIME_INSTANCE
        .groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
        .unwrap()
        .delete_publish(publish_id)
        .await
}

pub async fn create_subscribe(group_id: String, req: SubscribeCreateUpdateReq) -> Result<()> {
    RUNTIME_INSTANCE
        .groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
        .unwrap()
        .create_subscribe(req)
        .await
}

pub async fn list_subscribes(group_id: String) -> ListSubscribeResp {
    RUNTIME_INSTANCE
        .groups
        .read()
        .await
        .iter()
        .find(|group| group.id == group_id)
        .unwrap()
        .list_subscribes()
        .await
}

pub async fn update_subscribe(
    group_id: String,
    subscribe_id: String,
    req: SubscribeCreateUpdateReq,
) -> Result<()> {
    RUNTIME_INSTANCE
        .groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
        .unwrap()
        .update_subscribe(subscribe_id, req)
        .await
}

pub async fn delete_subscribe(group_id: String, subscribe_id: String) -> Result<()> {
    RUNTIME_INSTANCE
        .groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
        .unwrap()
        .delete_subscribe(subscribe_id)
        .await
}

pub async fn list_clients(group_id: String, query: ClientsQueryParams) -> ClientsListResp {
    RUNTIME_INSTANCE
        .groups
        .read()
        .await
        .iter()
        .find(|group| group.id == group_id)
        .unwrap()
        .list_clients(query)
        .await
}

pub async fn read_metrics(group_id: String, query: MetricsQueryParams) -> MetricsListResp {
    RUNTIME_INSTANCE
        .groups
        .read()
        .await
        .iter()
        .find(|group| group.id == group_id)
        .unwrap()
        .read_metrics(query)
        .await
}

#[derive(Default)]
pub struct ClientAtomicMetrics {
    pub running_cnt: AtomicUsize,
    pub waiting_cnt: AtomicUsize,
    pub error_cnt: AtomicUsize,
    pub stopped_cnt: AtomicUsize,
}

impl ClientAtomicMetrics {
    pub fn take_metrics(&self) -> ClientUsizeMetrics {
        ClientUsizeMetrics {
            running_cnt: self
                .running_cnt
                .swap(0, std::sync::atomic::Ordering::SeqCst),
            waiting_cnt: self
                .waiting_cnt
                .swap(0, std::sync::atomic::Ordering::SeqCst),
            error_cnt: self.error_cnt.swap(0, std::sync::atomic::Ordering::SeqCst),
            stopped_cnt: self
                .stopped_cnt
                .swap(0, std::sync::atomic::Ordering::SeqCst),
        }
    }
}

#[derive(Default)]
pub struct PacketAtomicMetrics {
    // 连接确认
    pub conn_ack: AtomicUsize,
    // 发布确认
    pub pub_ack: AtomicUsize,
    // 取消订阅确认
    pub unsub_ack: AtomicUsize,
    // ping请求
    pub ping_req: AtomicUsize,
    // ping响应
    pub ping_resp: AtomicUsize,
    // 发布
    pub outgoing_publish: AtomicUsize,

    pub incoming_publish: AtomicUsize,

    pub pub_rel: AtomicUsize,
    pub pub_rec: AtomicUsize,
    pub pub_comp: AtomicUsize,
    // 订阅
    pub subscribe: AtomicUsize,
    // 订阅确认
    pub sub_ack: AtomicUsize,
    pub await_ack: AtomicUsize,
    // 取消订阅
    pub unsubscribe: AtomicUsize,
    // 连接断开
    pub disconnect: AtomicUsize,
}

impl PacketAtomicMetrics {
    pub fn handle_v311_event(&self, event: rumqttc::Event) {
        match event {
            rumqttc::Event::Incoming(packet) => match packet {
                rumqttc::Packet::ConnAck(_) => {
                    self.conn_ack
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Packet::PubAck(_) => {
                    self.pub_ack
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Packet::UnsubAck(_) => {
                    self.unsub_ack
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Packet::PingReq => todo!(),
                rumqttc::Packet::PingResp => {
                    self.ping_resp
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Packet::Publish(_) => {
                    self.incoming_publish
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Packet::PubRec(_) => {
                    self.pub_rec
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Packet::PubRel(_) => {
                    self.pub_rel
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Packet::PubComp(_) => {
                    self.pub_comp
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Packet::Subscribe(_) => {
                    todo!()
                }
                rumqttc::Packet::SubAck(_) => {
                    self.sub_ack
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Packet::Unsubscribe(_) => todo!(),
                rumqttc::Packet::Disconnect => todo!(),
                _ => {}
            },
            rumqttc::Event::Outgoing(outgoing) => match outgoing {
                rumqttc::Outgoing::Publish(_) => {
                    self.outgoing_publish
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::Subscribe(_) => {
                    self.subscribe
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::Unsubscribe(_) => {
                    self.unsubscribe
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::PubAck(_) => {
                    self.pub_ack
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::PubRec(_) => {
                    self.pub_rec
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::PubRel(_) => {
                    self.pub_rel
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::PubComp(_) => {
                    self.pub_comp
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::PingReq => {
                    self.ping_req
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::Disconnect => {
                    self.disconnect
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::AwaitAck(_) => {
                    self.await_ack
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                _ => unreachable!(),
            },
        }
    }

    pub fn handle_v50_event(&self, event: rumqttc::v5::Event) {
        match event {
            rumqttc::v5::Event::Incoming(packet) => match packet {
                rumqttc::v5::mqttbytes::v5::Packet::ConnAck(_) => {
                    self.conn_ack
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::v5::mqttbytes::v5::Packet::PubAck(_) => {
                    self.pub_ack
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                // rumqttc::v5::mqttbytes::v5::Packet::PingReq(ping_req) => todo!(),
                rumqttc::v5::mqttbytes::v5::Packet::PingResp(_) => {
                    self.ping_resp
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                // rumqttc::v5::mqttbytes::v5::Packet::Subscribe(subscribe) => todo!(),
                // rumqttc::v5::mqttbytes::v5::Packet::SubAck(sub_ack) => todo!(),
                // rumqttc::v5::mqttbytes::v5::Packet::PubRec(pub_rec) => todo!(),
                // rumqttc::v5::mqttbytes::v5::Packet::PubRel(pub_rel) => todo!(),
                // rumqttc::v5::mqttbytes::v5::Packet::PubComp(pub_comp) => todo!(),
                // rumqttc::v5::mqttbytes::v5::Packet::Unsubscribe(unsubscribe) => todo!(),
                rumqttc::v5::mqttbytes::v5::Packet::UnsubAck(_) => {
                    self.unsub_ack
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                _ => unreachable!(),
            },
            rumqttc::v5::Event::Outgoing(outgoing) => match outgoing {
                rumqttc::Outgoing::Publish(_) => {
                    self.outgoing_publish
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::Subscribe(_) => {
                    self.subscribe
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::Unsubscribe(_) => {
                    self.unsubscribe
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::PubAck(_) => todo!(),
                rumqttc::Outgoing::PubRec(_) => todo!(),
                rumqttc::Outgoing::PubRel(_) => todo!(),
                rumqttc::Outgoing::PubComp(_) => todo!(),
                rumqttc::Outgoing::PingReq => {
                    self.ping_req
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::PingResp => todo!(),
                rumqttc::Outgoing::Disconnect => {
                    self.disconnect
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::AwaitAck(_) => todo!(),
            },
        }
    }

    pub fn take_metrics(&self) -> PacketUsizeMetrics {
        PacketUsizeMetrics {
            conn_ack: self.conn_ack.swap(0, std::sync::atomic::Ordering::SeqCst),
            pub_ack: self.pub_ack.swap(0, std::sync::atomic::Ordering::SeqCst),
            unsub_ack: self.unsub_ack.swap(0, std::sync::atomic::Ordering::SeqCst),
            ping_req: self.ping_req.swap(0, std::sync::atomic::Ordering::SeqCst),
            ping_resp: self.ping_resp.swap(0, std::sync::atomic::Ordering::SeqCst),
            outgoing_publish: self
                .outgoing_publish
                .swap(0, std::sync::atomic::Ordering::SeqCst),
            incoming_publish: self
                .incoming_publish
                .swap(0, std::sync::atomic::Ordering::SeqCst),
            pub_rel: self
                .incoming_publish
                .swap(0, std::sync::atomic::Ordering::SeqCst),
            pub_rec: self.pub_rec.swap(0, std::sync::atomic::Ordering::SeqCst),
            pub_comp: self.pub_comp.swap(0, std::sync::atomic::Ordering::SeqCst),
            subscribe: self.subscribe.swap(0, std::sync::atomic::Ordering::SeqCst),
            sub_ack: self.sub_ack.swap(0, std::sync::atomic::Ordering::SeqCst),
            unsubscribe: self
                .unsubscribe
                .swap(0, std::sync::atomic::Ordering::SeqCst),
            disconnect: self.disconnect.swap(0, std::sync::atomic::Ordering::SeqCst),
        }
    }
}

struct ErrorManager {
    task_err: Option<String>,
    client_err: BiLock<Option<String>>,
}

impl ErrorManager {
    fn new(client_err: BiLock<Option<String>>) -> Self {
        Self {
            task_err: None,
            client_err,
        }
    }

    async fn put_ok(&mut self) {
        if self.task_err.is_none() {
            return;
        }
        self.task_err = None;
        *self.client_err.lock().await = None;
    }

    async fn put_err(&mut self, err: String) {
        if let Some(task_err) = &self.task_err {
            if *task_err == err {
                return;
            }
        }

        self.task_err = Some(err.clone());
        *self.client_err.lock().await = Some(err);
    }
}
