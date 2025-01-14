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
    todo!()
    // RUNTIME_INSTANCE
    //     .groups
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|group| group.id == group_id)
    //     .unwrap()
    //     .update(req)
    //     .await
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

