use std::sync::atomic::AtomicUsize;

use crate::{ClientUsizeMetrics, PacketUsizeMetrics};

#[derive(Default, Debug)]
pub struct PacketAtomicMetrics {
    pub connect: AtomicUsize,
    // 连接确认
    pub conn_ack: AtomicUsize,

    // 取消订阅确认
    pub unsub_ack: AtomicUsize,
    // ping请求
    pub ping_req: AtomicUsize,
    // ping响应
    pub ping_resp: AtomicUsize,

    pub in_publish: AtomicUsize,
    pub out_publish: AtomicUsize,

    pub in_pub_ack: AtomicUsize,
    pub out_pub_ack: AtomicUsize,

    pub in_pub_rel: AtomicUsize,
    pub out_pub_rel: AtomicUsize,

    pub in_pub_rec: AtomicUsize,
    pub out_pub_rec: AtomicUsize,

    pub in_pub_comp: AtomicUsize,
    pub out_pub_comp: AtomicUsize,

    // 订阅
    pub subscribe: AtomicUsize,
    // 订阅确认
    pub sub_ack: AtomicUsize,
    // 取消订阅
    pub unsubscribe: AtomicUsize,
    // 连接断开
    pub disconnect: AtomicUsize,
}

impl PacketAtomicMetrics {
    pub fn take_metrics(&self) -> PacketUsizeMetrics {
        PacketUsizeMetrics {
            conn_ack: self.conn_ack.swap(0, std::sync::atomic::Ordering::SeqCst),
            unsub_ack: self.unsub_ack.swap(0, std::sync::atomic::Ordering::SeqCst),
            ping_req: self.ping_req.swap(0, std::sync::atomic::Ordering::SeqCst),
            ping_resp: self.ping_resp.swap(0, std::sync::atomic::Ordering::SeqCst),
            in_publish: self.in_publish.swap(0, std::sync::atomic::Ordering::SeqCst),
            out_publish: self
                .out_publish
                .swap(0, std::sync::atomic::Ordering::SeqCst),
            in_pub_ack: self.in_pub_ack.swap(0, std::sync::atomic::Ordering::SeqCst),
            out_pub_ack: self
                .out_pub_ack
                .swap(0, std::sync::atomic::Ordering::SeqCst),
            in_pub_rec: self.in_pub_rec.swap(0, std::sync::atomic::Ordering::SeqCst),
            out_pub_rec: self
                .out_pub_rec
                .swap(0, std::sync::atomic::Ordering::SeqCst),
            in_pub_rel: self.in_pub_rel.swap(0, std::sync::atomic::Ordering::SeqCst),
            out_pub_rel: self
                .out_pub_rel
                .swap(0, std::sync::atomic::Ordering::SeqCst),
            in_pub_comp: self
                .in_pub_comp
                .swap(0, std::sync::atomic::Ordering::SeqCst),
            out_pub_comp: self
                .out_pub_comp
                .swap(0, std::sync::atomic::Ordering::SeqCst),
            subscribe: self.subscribe.swap(0, std::sync::atomic::Ordering::SeqCst),
            sub_ack: self.sub_ack.swap(0, std::sync::atomic::Ordering::SeqCst),
            unsubscribe: self
                .unsubscribe
                .swap(0, std::sync::atomic::Ordering::SeqCst),
            disconnect: self.disconnect.swap(0, std::sync::atomic::Ordering::SeqCst),
        }
    }
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
