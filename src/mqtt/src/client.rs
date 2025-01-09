//! This module offers a high level synchronous and asynchronous abstraction to
//! async eventloop.

use flume::Sender;
use tokio::runtime::Runtime;

use crate::{EventLoop, MqttOptions, Request};

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
    request_tx: Sender<Request>,
}

impl AsyncClient {
    /// Create a new `AsyncClient`.
    ///
    /// `cap` specifies the capacity of the bounded async channel.
    pub fn new(options: MqttOptions, cap: usize) -> (AsyncClient, EventLoop) {
        let eventloop = EventLoop::new(options, cap);
        let request_tx = eventloop.requests_tx.clone();

        let client = AsyncClient { request_tx };

        (client, eventloop)
    }

    pub async fn publish(&self, payload: Request) {
        self.request_tx.send_async(payload).await;
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

///  MQTT connection. Maintains all the necessary state
pub struct Connection {
    pub eventloop: EventLoop,
    runtime: Runtime,
}
impl Connection {
    fn new(eventloop: EventLoop, runtime: Runtime) -> Connection {
        Connection { eventloop, runtime }
    }
}
