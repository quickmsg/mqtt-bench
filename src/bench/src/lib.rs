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
                hosts: vec!["127.0.0.1".into()],
                username: None,
                password: None,
                client_id: None,
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
            conf: (*group.conf).clone(),
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
    // 订阅确认
    pub sub_ack: AtomicUsize,
    // 取消订阅
    pub unsubscribe: AtomicUsize,
    // 连接断开
    pub disconnect: AtomicUsize,
}

impl Status {
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
                // rumqttc::Packet::PubRec(pub_rec) => todo!(),
                // rumqttc::Packet::PubRel(pub_rel) => todo!(),
                // rumqttc::Packet::PubComp(pub_comp) => todo!(),
                // rumqttc::Packet::Subscribe(subscribe) => todo!(),
                // rumqttc::Packet::SubAck(sub_ack) => todo!(),
                rumqttc::Packet::UnsubAck(_) => {
                    self.unsub_ack
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Packet::PingReq => todo!(),
                rumqttc::Packet::PingResp => {
                    self.ping_resp
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Packet::Connect(connect) => todo!(),
                rumqttc::Packet::Publish(publish) => todo!(),
                rumqttc::Packet::PubRec(pub_rec) => todo!(),
                rumqttc::Packet::PubRel(pub_rel) => todo!(),
                rumqttc::Packet::PubComp(pub_comp) => todo!(),
                rumqttc::Packet::Subscribe(subscribe) => todo!(),
                rumqttc::Packet::SubAck(sub_ack) => {
                    self.sub_ack
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Packet::Unsubscribe(unsubscribe) => todo!(),
                rumqttc::Packet::Disconnect => todo!(),
            },
            rumqttc::Event::Outgoing(outgoing) => match outgoing {
                rumqttc::Outgoing::Publish(_) => {
                    self.publish
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
                rumqttc::Outgoing::Disconnect => {
                    self.disconnect
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rumqttc::Outgoing::AwaitAck(_) => todo!(),
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
                    self.publish
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
}
