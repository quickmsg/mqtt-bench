use std::sync::{Arc, LazyLock};

use group::Group;
use tokio::sync::RwLock;
use types::{BrokerInfo, GroupConf};

mod client;
mod group;
mod mqtt;

static RUNTIME_INSTANCE: LazyLock<RuntimeInstance> = LazyLock::new(|| Default::default());

pub struct RuntimeInstance {
    pub broker_info: RwLock<Arc<BrokerInfo>>,
    // TODO remove dashmap
    pub groups: RwLock<Vec<Group>>,
}

impl Default for RuntimeInstance {
    fn default() -> Self {
        Self {
            broker_info: RwLock::new(Arc::new(BrokerInfo {
                addrs: vec![("127.0.0.1".into(), 1883)],
                username: None,
                password: None,
                client_id: None,
                protocol_version: types::ProtocolVersion::V311,
                connect_rate: 1000,
            })),
            groups: RwLock::new(vec![]),
        }
    }
}

pub async fn read_broker() -> BrokerInfo {
    let broker_info = RUNTIME_INSTANCE.broker_info.read().await.clone();
    (*broker_info).clone()
}

pub async fn update_broker(info: BrokerInfo) {
    *RUNTIME_INSTANCE.broker_info.write().await = Arc::new(info);
}

pub async fn create_group(conf: GroupConf) {
    let group = Group::new(RUNTIME_INSTANCE.broker_info.read().await.clone(), conf);
    RUNTIME_INSTANCE.groups.write().await.push(group);
}

pub async fn list_groups() -> Vec<GroupConf> {
    let groups = RUNTIME_INSTANCE.groups.read().await;
    groups.iter().map(|g| (*g.conf).clone()).collect()
}

pub async fn start_group(id: String) {
    RUNTIME_INSTANCE.groups.write().await[0].start().await;
}
