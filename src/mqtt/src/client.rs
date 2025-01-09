//! This module offers a high level synchronous and asynchronous abstraction to
//! async eventloop.
use std::{sync::Arc, time::Duration};

use flume::Sender;
use tracing::trace;

use crate::{EventLoop, MqttOptions, Request};

/// Client Error
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Failed to send mqtt requests to eventloop")]
    Request(Request),
}

/// An asynchronous client, communicates with MQTT `EventLoop`.
///
/// This is cloneable and can be used to asynchronously [`publish`](`AsyncClient::publish`),
/// [`subscribe`](`AsyncClient::subscribe`) through the `EventLoop`, which is to be polled parallelly.
///
/// **NOTE**: The `EventLoop` must be regularly polled in order to send, receive and process packets
/// from the broker, i.e. move ahead.
#[derive(Clone, Debug)]
pub struct AsyncClient {
    request_tx: Sender<Arc<Vec<u8>>>,
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

    /// Create a new `AsyncClient` from a channel `Sender`.
    ///
    /// This is mostly useful for creating a test instance where you can
    /// listen on the corresponding receiver.
    pub fn from_senders(request_tx: Sender<Request>) -> AsyncClient {
        AsyncClient { request_tx }
    }

    /// Sends a MQTT Publish to the `EventLoop`.
    pub async fn publish(&self, payload: Arc<Vec<u8>>) -> Result<(), ClientError> {
        self.request_tx.send_async(payload).await?;
        Ok(())
    }

    /// Sends a MQTT Subscribe to the `EventLoop`
    // pub async fn subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<(), ClientError> {
    //     let subscribe = Subscribe::new(topic, qos);
    //     if !subscribe_has_valid_filters(&subscribe) {
    //         return Err(ClientError::Request(subscribe.into()));
    //     }

    //     self.request_tx.send_async(subscribe.into()).await?;
    //     Ok(())
    // }

    /// Sends a MQTT Subscribe for multiple topics to the `EventLoop`
    // pub async fn subscribe_many<T>(&self, topics: T) -> Result<(), ClientError>
    // where
    //     T: IntoIterator<Item = SubscribeFilter>,
    // {
    //     let subscribe = Subscribe::new_many(topics);
    //     if !subscribe_has_valid_filters(&subscribe) {
    //         return Err(ClientError::Request(subscribe.into()));
    //     }

    //     self.request_tx.send_async(subscribe.into()).await?;
    //     Ok(())
    // }

   
    /// Sends a MQTT disconnect to the `EventLoop`
    // pub async fn disconnect(&self) -> Result<(), ClientError> {
    //     let request = Request::Disconnect(Disconnect);
    //     self.request_tx.send_async(request).await?;
    //     Ok(())
    // }

    // /// Attempts to send a MQTT disconnect to the `EventLoop`
    // pub fn try_disconnect(&self) -> Result<(), ClientError> {
    //     let request = Request::Disconnect(Disconnect);
    //     self.request_tx.try_send(request)?;
    //     Ok(())
    // }
}

// fn get_ack_req(publish: &Publish) -> Option<Request> {
//     let ack = match publish.qos {
//         QoS::AtMostOnce => return None,
//         QoS::AtLeastOnce => Request::PubAck(PubAck::new(publish.pkid)),
//         QoS::ExactlyOnce => Request::PubRec(PubRec::new(publish.pkid)),
//     };
//     Some(ack)
// }

// #[must_use]
// fn subscribe_has_valid_filters(subscribe: &Subscribe) -> bool {
//     !subscribe.filters.is_empty()
//         && subscribe
//             .filters
//             .iter()
//             .all(|filter| valid_filter(&filter.path))
// }

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

    /// Returns an iterator over this connection. Iterating over this is all that's
    /// necessary to make connection progress and maintain a robust connection.
    /// Just continuing to loop will reconnect
    /// **NOTE** Don't block this while iterating
    // ideally this should be named iter_mut because it requires a mutable reference
    // Also we can implement IntoIter for this to make it easy to iterate over it
    #[must_use = "Connection should be iterated over a loop to make progress"]
    pub fn iter(&mut self) -> Iter<'_> {
        Iter { connection: self }
    }

    /// Attempt to fetch an incoming [`Event`] on the [`EvenLoop`], returning an error
    /// if all clients/users have closed requests channel.
    ///
    /// [`EvenLoop`]: super::EventLoop
    pub fn recv(&mut self) -> Result<Result<Event, ConnectionError>, RecvError> {
        let f = self.eventloop.poll();
        let event = self.runtime.block_on(f);

        resolve_event(event).ok_or(RecvError)
    }

    /// Attempt to fetch an incoming [`Event`] on the [`EvenLoop`], returning an error
    /// if none immediately present or all clients/users have closed requests channel.
    ///
    /// [`EvenLoop`]: super::EventLoop
    pub fn try_recv(&mut self) -> Result<Result<Event, ConnectionError>, TryRecvError> {
        let f = self.eventloop.poll();
        // Enters the runtime context so we can poll the future, as required by `now_or_never()`.
        // ref: https://docs.rs/tokio/latest/tokio/runtime/struct.Runtime.html#method.enter
        let _guard = self.runtime.enter();
        let event = f.now_or_never().ok_or(TryRecvError::Empty)?;

        resolve_event(event).ok_or(TryRecvError::Disconnected)
    }

    /// Attempt to fetch an incoming [`Event`] on the [`EvenLoop`], returning an error
    /// if all clients/users have closed requests channel or the timeout has expired.
    ///
    /// [`EvenLoop`]: super::EventLoop
    pub fn recv_timeout(
        &mut self,
        duration: Duration,
    ) -> Result<Result<Event, ConnectionError>, RecvTimeoutError> {
        let f = self.eventloop.poll();
        let event = self
            .runtime
            .block_on(async { timeout(duration, f).await })
            .map_err(|_| RecvTimeoutError::Timeout)?;

        resolve_event(event).ok_or(RecvTimeoutError::Disconnected)
    }
}

fn resolve_event(event: Result<Event, ConnectionError>) -> Option<Result<Event, ConnectionError>> {
    match event {
        Ok(v) => Some(Ok(v)),
        // closing of request channel should stop the iterator
        Err(ConnectionError::RequestsDone) => {
            trace!("Done with requests");
            None
        }
        Err(e) => Some(Err(e)),
    }
}

/// Iterator which polls the `EventLoop` for connection progress
pub struct Iter<'a> {
    connection: &'a mut Connection,
}

impl Iterator for Iter<'_> {
    type Item = Result<Event, ConnectionError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.connection.recv().ok()
    }
}