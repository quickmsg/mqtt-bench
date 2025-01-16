use std::sync::Arc;

use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;
use tracing::{debug, warn};
use types::group::PacketAtomicMetrics;

use crate::{
    protocol::{
        self,
        v3_mini::{
            self,
            v4::{Codec, Packet},
        },
    },
    state::{self, StateError},
    Incoming,
};

/// Network transforms packets <-> frames efficiently. It takes
/// advantage of pre-allocation, buffering and vectorization when
/// appropriate to achieve performance
pub struct Network {
    /// Frame MQTT packets from network connection
    pub framed: Framed<Box<dyn AsyncReadWrite>, Codec>,
}

impl Network {
    pub fn new(
        socket: impl AsyncReadWrite + 'static,
        max_incoming_size: usize,
        max_outgoing_size: usize,
    ) -> Network {
        let socket = Box::new(socket) as Box<dyn AsyncReadWrite>;
        let codec = Codec {
            max_incoming_size,
            max_outgoing_size,
        };
        let framed = Framed::new(socket, codec);

        Network { framed }
    }

    pub async fn handle_incoming_packet(
        &mut self,
        packet: Option<Result<Packet, v3_mini::Error>>,
        packet_metrics: &Arc<PacketAtomicMetrics>,
    ) -> bool {
        match packet {
            Some(packet) => match packet {
                Ok(packet) => match packet {
                    Packet::Connect(connect) => {
                        warn!("Received Connect packet, but not expected");
                        false
                    }
                    Packet::ConnAck(conn_ack) => {
                        packet_metrics
                            .conn_ack
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        true
                    }
                    Packet::Publish(publish) => {
                        debug!("Received Publish packet: {:?}", publish);
                        packet_metrics
                            .incoming_publish
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        match publish.qos {
                            v3_mini::QoS::AtMostOnce => true,
                            v3_mini::QoS::AtLeastOnce => {
                                let pub_ack = v3_mini::v4::PubAck::new(publish.pkid);
                                let packet = Packet::PubAck(pub_ack);
                                if let Err(e) = self.write(packet, packet_metrics).await {
                                    warn!("{:?}", e);
                                };

                                true
                            }
                            v3_mini::QoS::ExactlyOnce => todo!(),
                        }
                    }
                    Packet::PubAck(pub_ack) => {
                        packet_metrics
                            .pub_ack
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        true
                    }
                    Packet::PubRec(pub_rec) => todo!(),
                    Packet::PubRel(pub_rel) => todo!(),
                    Packet::PubComp(pub_comp) => todo!(),
                    Packet::Subscribe(subscribe) => todo!(),
                    Packet::SubAck(_) => {
                        packet_metrics
                            .sub_ack
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        true
                    }
                    Packet::Unsubscribe(unsubscribe) => todo!(),
                    Packet::UnsubAck(unsub_ack) => todo!(),
                    Packet::PingReq => todo!(),
                    Packet::PingResp => todo!(),
                    Packet::Disconnect => todo!(),
                },
                Err(e) => {
                    warn!("Error deserializing packet: {:?}", e);
                    false
                }
            },
            None => false,
        }
    }

    /// Reads and returns a single packet from network
    pub async fn read(&mut self) -> Result<Incoming, StateError> {
        match self.framed.next().await {
            Some(Ok(packet)) => Ok(packet),
            Some(Err(protocol::v3_mini::Error::InsufficientBytes(_))) => unreachable!(),
            Some(Err(e)) => Err(StateError::Deserialization(e)),
            None => Err(StateError::ConnectionAborted),
        }
    }

    /// Read packets in bulk. This allow replies to be in bulk. This method is used
    /// after the connection is established to read a bunch of incoming packets
    pub async fn readb(
        &mut self,
        packet_metrics: &Arc<PacketAtomicMetrics>,
    ) -> Result<(), StateError> {
        // wait for the first read
        let mut res = self.framed.next().await;
        loop {
            match res {
                Some(Ok(packet)) => {
                    if let Some(outgoing) = state::handle_incoming_packet(packet, &packet_metrics)?
                    {
                        self.write(outgoing, &packet_metrics).await?;
                    }
                }
                Some(Err(protocol::v3_mini::Error::InsufficientBytes(_))) => unreachable!(),
                Some(Err(e)) => return Err(StateError::Deserialization(e)),
                // None => return Err(StateError::ConnectionAborted),
                None => break,
            }
            match self.framed.next().now_or_never() {
                Some(r) => res = r,
                _ => break,
            };
        }

        Ok(())
    }

    pub async fn write(
        &mut self,
        packet: Packet,
        packet_metrics: &Arc<PacketAtomicMetrics>,
    ) -> Result<(), StateError> {
        match &packet {
            Packet::Connect(connect) => {}
            Packet::ConnAck(conn_ack) => todo!(),
            Packet::Publish(_) => {
                packet_metrics
                    .outgoing_publish
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Packet::PubAck(_) => {
                packet_metrics
                    .pub_ack
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Packet::PubRec(pub_rec) => todo!(),
            Packet::PubRel(pub_rel) => todo!(),
            Packet::PubComp(pub_comp) => todo!(),
            Packet::Subscribe(_) => {
                packet_metrics
                    .subscribe
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Packet::SubAck(sub_ack) => {
                packet_metrics
                    .sub_ack
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Packet::Unsubscribe(unsubscribe) => todo!(),
            Packet::UnsubAck(unsub_ack) => todo!(),
            Packet::PingReq => todo!(),
            Packet::PingResp => todo!(),
            Packet::Disconnect => todo!(),
        }
        self.framed
            .feed(packet)
            .await
            .map_err(StateError::Deserialization);
        self.flush().await
    }

    pub async fn flush(&mut self) -> Result<(), crate::state::StateError> {
        self.framed
            .flush()
            .await
            .map_err(StateError::Deserialization)
    }
}

pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Send + Unpin {}
