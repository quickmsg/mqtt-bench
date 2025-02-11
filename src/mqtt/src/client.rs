use std::sync::Arc;

use tokio::sync::mpsc;
use types::group::{ClientAtomicMetrics, PacketAtomicMetrics};

use crate::{protocol::v3_mini::v4::Packet, EventLoop, MqttOptions};

#[derive(Clone, Debug)]
pub struct AsyncClient {
    request_tx: mpsc::Sender<Packet>,
}

impl AsyncClient {
    pub async fn new(
        client_metrics: Arc<ClientAtomicMetrics>,
        options: MqttOptions,
        packet_metrics: Arc<PacketAtomicMetrics>,
    ) -> AsyncClient {
        let request_tx = EventLoop::start(client_metrics, options, packet_metrics).await;
        let client = AsyncClient { request_tx };
        client
    }

    pub async fn send(&self, packet: Packet) {
        let _ = self.request_tx.send(packet).await;
    }
}
