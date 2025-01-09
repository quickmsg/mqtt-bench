use std::sync::atomic::AtomicUsize;

use crate::PacketUsizeMetrics;

#[derive(Default, Debug)]
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
