use std::{
    collections::VecDeque,
    sync::{Arc, LazyLock},
};

use anyhow::{bail, Result};
use group::Group;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    RwLock,
};
use tracing::debug;
use types::{
    BrokerUpdateReq, ClientsListResp, ClientsQueryParams, GroupCreateReq, GroupListResp,
    GroupListRespItem, GroupUpdateReq, ListPublishResp, ListSubscribeResp, MetricsListResp,
    MetricsQueryParams, ReadGroupResp, Status, SubscribeCreateUpdateReq,
};
use uuid::Uuid;

mod client;
mod group;

static RUNTIME_INSTANCE: LazyLock<RuntimeInstance> = LazyLock::new(|| Default::default());
static TASK_QUEUE: LazyLock<TaskQueue> = LazyLock::new(|| TaskQueue::new());

struct TaskQueue {
    cmd_tx: UnboundedSender<Command>,
}

impl TaskQueue {
    pub fn new() -> Self {
        let (cmd_tx, cmd_rx) = unbounded_channel();
        TaskQueueLoop::new(cmd_rx).run();
        Self { cmd_tx }
    }

    pub fn start_group(&self, group_id: String) {
        self.cmd_tx.send(Command::Start(group_id)).unwrap();
    }
}

enum Command {
    Start(String),
}

struct TaskQueueLoop {
    cmd_rx: UnboundedReceiver<Command>,
    done_tx: UnboundedSender<()>,
    done_rx: UnboundedReceiver<()>,
    staring: bool,
    wating_starts: VecDeque<String>,
}

impl TaskQueueLoop {
    pub fn new(cmd_rx: UnboundedReceiver<Command>) -> Self {
        let (done_tx, done_rx) = unbounded_channel();
        Self {
            cmd_rx,
            done_tx,
            done_rx,
            staring: false,
            wating_starts: VecDeque::new(),
        }
    }

    pub fn run(mut self) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    cmd = self.cmd_rx.recv() => {
                        self.process_cmd(cmd).await;
                    }
                    _ = self.done_rx.recv() => {
                        self.process_done().await;
                    }
                }
            }
        });
    }

    async fn process_cmd(&mut self, cmd: Option<Command>) {
        let cmd = cmd.unwrap();
        match cmd {
            Command::Start(group_id) => {
                if self.staring {
                    self.wating_starts.push_back(group_id);
                } else {
                    self.staring = true;
                    RUNTIME_INSTANCE
                        .groups
                        .write()
                        .await
                        .iter_mut()
                        .find(|g| g.id == group_id)
                        .unwrap()
                        .start(self.done_tx.clone());
                }
            }
        }
    }

    async fn process_done(&mut self) {
        if let Some(group_id) = self.wating_starts.pop_front() {
            RUNTIME_INSTANCE
                .groups
                .write()
                .await
                .iter_mut()
                .find(|g| g.id == group_id)
                .unwrap()
                .start(self.done_tx.clone());
        } else {
            self.staring = false;
        }
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
                broker_hosts: vec!["127.0.0.1".into()],
                connect_interval: 1,
                keep_alive: 60,
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
    if RUNTIME_INSTANCE.groups.read().await.iter().any(|group| {
        Status::from(group.status.load(std::sync::atomic::Ordering::SeqCst)) != Status::Stopped
    }) {
        Err(anyhow::anyhow!("请停止所有的组后再进行修改！"))
    } else {
        *RUNTIME_INSTANCE.broker_info.write().await = Arc::new(info);
        Ok(())
    }
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
            status: Status::from(group.status.load(std::sync::atomic::Ordering::SeqCst)),
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
    TASK_QUEUE.start_group(group_id);
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
    debug!("bench create subscribe");
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
