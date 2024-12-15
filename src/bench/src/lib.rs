use std::sync::{atomic::AtomicUsize, Arc, LazyLock};

use group::Group;
use tokio::sync::RwLock;
use types::{
    BrokerUpdateReq, GroupCreateUpdateReq, ListGroupResp, ListGroupRespItem, ReadGroupResp,
    SubscribeCreateUpdateReq,
};
use uuid::Uuid;

mod client;
mod group;
mod mqtt;

static RUNTIME_INSTANCE: LazyLock<RuntimeInstance> = LazyLock::new(|| Default::default());

fn generate_id() -> String {
    Uuid::new_v4().simple().to_string()
}

pub struct RuntimeInstance {
    pub broker_info: RwLock<Arc<BrokerUpdateReq>>,
    pub groups: RwLock<Vec<Group>>,
}

impl Default for RuntimeInstance {
    fn default() -> Self {
        Self {
            broker_info: RwLock::new(Arc::new(BrokerUpdateReq {
                addrs: vec![("127.0.0.1".into(), 1883)],
                username: None,
                password: None,
                client_id: None,
                protocol_version: types::ProtocolVersion::V311,
                connect_interval: 1,
            })),
            groups: RwLock::new(vec![]),
        }
    }
}

pub async fn read_broker() -> BrokerUpdateReq {
    let broker_info = RUNTIME_INSTANCE.broker_info.read().await.clone();
    (*broker_info).clone()
}

pub async fn update_broker(info: BrokerUpdateReq) {
    *RUNTIME_INSTANCE.broker_info.write().await = Arc::new(info);
}

pub async fn create_group(req: GroupCreateUpdateReq) {
    let group = Group::new(
        generate_id(),
        RUNTIME_INSTANCE.broker_info.read().await.clone(),
        req,
    );
    RUNTIME_INSTANCE.groups.write().await.push(group);
}

pub async fn list_groups() -> ListGroupResp {
    let groups = RUNTIME_INSTANCE.groups.read().await;
    let list: Vec<_> = groups
        .iter()
        .rev()
        .map(|group| ListGroupRespItem {
            id: group.id.clone(),
            name: group.conf.name.clone(),
            client_count: group.conf.client_count,
        })
        .collect();
    ListGroupResp { list }
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

pub async fn start_group(group_id: String) {
    RUNTIME_INSTANCE
        .groups
        .write()
        .await
        .iter_mut()
        .find(|g| g.id == group_id)
        .unwrap()
        .start()
        .await;
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

pub async fn create_publish(group_id: String, req: types::PublishCreateUpdateReq) {
    RUNTIME_INSTANCE
        .groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
        .unwrap()
        .create_publish(req)
        .await;
}

pub async fn create_subscribe(group_id: String, req: SubscribeCreateUpdateReq) {
    RUNTIME_INSTANCE
        .groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
        .unwrap()
        .create_subscribe(req)
        .await;
}

#[derive(Default)]
pub struct Status {
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
    pub publish: AtomicUsize,

    // 订阅
    pub subscribe: AtomicUsize,
    // 取消订阅
    pub unsubscribe: AtomicUsize,
    // 连接断开
    pub disconnect: AtomicUsize,
}
