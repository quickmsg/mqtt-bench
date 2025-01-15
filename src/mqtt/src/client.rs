//! This module offers a high level synchronous and asynchronous abstraction to
//! async eventloop.

use std::sync::Arc;

use flume::Sender;
use tracing::{debug, warn};
use types::group::{ClientAtomicMetrics, PacketAtomicMetrics};

use crate::{protocol::v3_mini::v4::Packet, ConnectionError, EventLoop, MqttOptions, Request};

/// Client Error
// #[derive(Debug, thiserror::Error)]
// pub enum ClientError {
//     #[error("Failed to send mqtt requests to eventloop")]
//     Request(Request),
// }

/// An asynchronous client, communicates with MQTT `EventLoop`.
///
/// This is cloneable and can be used to asynchronously [`publish`](`AsyncClient::publish`),
/// [`subscribe`](`AsyncClient::subscribe`) through the `EventLoop`, which is to be polled parallelly.
///
/// **NOTE**: The `EventLoop` must be regularly polled in order to send, receive and process packets
/// from the broker, i.e. move ahead.
#[derive(Clone, Debug)]
pub struct AsyncClient {
    request_tx: Sender<Packet>,
}

impl AsyncClient {
    /// Create a new `AsyncClient`.
    ///
    /// `cap` specifies the capacity of the bounded async channel.
    pub async fn new(
        client_metrics: Arc<ClientAtomicMetrics>,
        options: MqttOptions,
        cap: usize,
        packet_metrics: Arc<PacketAtomicMetrics>,
    ) -> AsyncClient {
        let request_tx = EventLoop::start(client_metrics, options, cap, packet_metrics).await;
        let client = AsyncClient { request_tx };
        client
    }

    pub async fn publish(&self, payload: Packet) {
        match self.request_tx.send_async(payload).await {
            Ok(_) => debug!("send ok"),
            Err(e) => debug!("send err: {:?}", e),
        }
    }
}

/// Error type returned by [`Connection::recv`]
#[derive(Debug, Eq, PartialEq)]
pub struct RecvError;

/// Error type returned by [`Connection::try_recv`]
#[derive(Debug, Eq, PartialEq)]
pub enum TryRecvError {
    /// User has closed requests channel
    Disconnected,
    /// Did not resolve
    Empty,
}

/// Error type returned by [`Connection::recv_timeout`]
#[derive(Debug, Eq, PartialEq)]
pub enum RecvTimeoutError {
    /// User has closed requests channel
    Disconnected,
    /// Recv request timedout
    Timeout,
}
