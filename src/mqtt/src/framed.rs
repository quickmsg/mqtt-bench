use std::sync::Arc;

use futures_util::SinkExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;
use tracing::warn;
use types::group::PacketAtomicMetrics;

use crate::{
    protocol::v3_mini::{
        self,
        v4::{Codec, Packet},
    },
    ConnectionError,
};

pub struct Network {
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
                    Packet::Publish(publish) => {
                        packet_metrics
                            .in_publish
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
                            v3_mini::QoS::ExactlyOnce => {
                                let pub_rec = v3_mini::v4::PubRec::new(publish.pkid);
                                let packet = Packet::PubRec(pub_rec);
                                if let Err(e) = self.write(packet, packet_metrics).await {
                                    warn!("{:?}", e);
                                };
                                true
                            }
                        }
                    }
                    Packet::PubAck(_) => {
                        packet_metrics
                            .in_pub_ack
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        true
                    }
                    Packet::PubRec(_) => {
                        packet_metrics
                            .in_pub_rec
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        true
                    }
                    Packet::PubRel(pub_rel) => {
                        let pub_comp = v3_mini::v4::PubComp::new(pub_rel.pkid);
                        let packet = Packet::PubComp(pub_comp);
                        if let Err(e) = self.write(packet, packet_metrics).await {
                            warn!("{:?}", e);
                        };
                        true
                    }
                    Packet::PubComp(_) => {
                        packet_metrics
                            .in_pub_comp
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        true
                    }
                    Packet::SubAck(_) => {
                        packet_metrics
                            .sub_ack
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        true
                    }
                    Packet::UnsubAck(_) => {
                        packet_metrics
                            .unsub_ack
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        true
                    }
                    Packet::PingResp => todo!(),
                    Packet::Disconnect => todo!(),
                    _ => {
                        todo!()
                    }
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
    // pub async fn read(&mut self) -> Result<Incoming, StateError> {
    //     match self.framed.next().await {
    //         Some(Ok(packet)) => Ok(packet),
    //         Some(Err(protocol::v3_mini::Error::InsufficientBytes(_))) => unreachable!(),
    //         Some(Err(e)) => Err(StateError::Deserialization(e)),
    //         None => Err(StateError::ConnectionAborted),
    //     }
    // }

    /// Read packets in bulk. This allow replies to be in bulk. This method is used
    /// after the connection is established to read a bunch of incoming packets
    // pub async fn readb(
    //     &mut self,
    //     packet_metrics: &Arc<PacketAtomicMetrics>,
    // ) -> Result<(), StateError> {
    //     // wait for the first read
    //     let mut res = self.framed.next().await;
    //     loop {
    //         match res {
    //             Some(Ok(packet)) => {
    //                 if let Some(outgoing) = state::handle_incoming_packet(packet, &packet_metrics)?
    //                 {
    //                     self.write(outgoing, &packet_metrics).await?;
    //                 }
    //             }
    //             Some(Err(protocol::v3_mini::Error::InsufficientBytes(_))) => unreachable!(),
    //             Some(Err(e)) => return Err(StateError::Deserialization(e)),
    //             // None => return Err(StateError::ConnectionAborted),
    //             None => break,
    //         }
    //         match self.framed.next().now_or_never() {
    //             Some(r) => res = r,
    //             _ => break,
    //         };
    //     }

    //     Ok(())
    // }

    pub async fn write(
        &mut self,
        packet: Packet,
        packet_metrics: &Arc<PacketAtomicMetrics>,
    ) -> Result<(), ConnectionError> {
        match &packet {
            Packet::Connect(_) => {
                packet_metrics
                    .connect
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Packet::Publish(_) => {
                packet_metrics
                    .out_publish
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Packet::PubAck(_) => {
                packet_metrics
                    .out_pub_ack
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Packet::PubRec(_) => {
                packet_metrics
                    .out_pub_rec
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Packet::PubRel(_) => {
                packet_metrics
                    .out_pub_rel
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Packet::PubComp(_) => {
                packet_metrics
                    .out_pub_comp
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
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
            Packet::Unsubscribe(_) => {
                packet_metrics
                    .unsubscribe
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Packet::UnsubAck(_) => {
                packet_metrics
                    .unsub_ack
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Packet::PingReq => todo!(),
            Packet::PingResp => todo!(),
            Packet::Disconnect => todo!(),
            _ => unreachable!(),
        }
        self.framed.send(packet).await.unwrap();
        Ok(())
    }
}

pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Send + Unpin {}
