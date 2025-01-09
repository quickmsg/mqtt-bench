use tracing::error;

use crate::{
    protocol::{
        self,
        v3_mini::{
            v4::{Packet, PubAck, PubComp, PubRec, PubRel, Publish},
            QoS,
        },
    },
    Incoming,
};

use std::io;

/// Errors during state handling
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Io Error while state is passed to network
    #[error("Io error: {0:?}")]
    Io(#[from] io::Error),
    /// Invalid state for a given operation
    #[error("Invalid state for a given operation")]
    InvalidState,
    /// Received a packet (ack) which isn't asked for
    #[error("Received unsolicited ack pkid: {0}")]
    Unsolicited(u16),
    /// Last pingreq isn't acked
    #[error("Last pingreq isn't acked")]
    AwaitPingResp,
    /// Received a wrong packet while waiting for another packet
    #[error("Received a wrong packet while waiting for another packet")]
    WrongPacket,
    #[error("Timeout while waiting to resolve collision")]
    CollisionTimeout,
    #[error("A Subscribe packet must contain atleast one filter")]
    EmptySubscription,
    #[error("Mqtt serialization/deserialization error: {0}")]
    Deserialization(#[from] protocol::v3_mini::Error),
    #[error("Connection closed by peer abruptly")]
    ConnectionAborted,
}

/// Consolidates handling of all incoming mqtt packets. Returns a `Notification` which for the
/// user to consume and `Packet` which for the eventloop to put on the network
/// E.g For incoming QoS1 publish packet, this method returns (Publish, Puback). Publish packet will
/// be forwarded to user and Pubck packet will be written to network
pub fn handle_incoming_packet(packet: Incoming) -> Result<Option<Packet>, StateError> {
    let outgoing = match &packet {
        Incoming::PingResp => handle_incoming_pingresp()?,
        Incoming::Publish(publish) => handle_incoming_publish(publish)?,
        Incoming::SubAck(_suback) => handle_incoming_suback()?,
        Incoming::UnsubAck(_unsuback) => handle_incoming_unsuback()?,
        Incoming::PubAck(puback) => handle_incoming_puback(puback)?,
        Incoming::PubRec(pubrec) => handle_incoming_pubrec(pubrec)?,
        Incoming::PubRel(pubrel) => handle_incoming_pubrel(pubrel)?,
        Incoming::PubComp(pubcomp) => handle_incoming_pubcomp(pubcomp)?,
        _ => {
            error!("Invalid incoming packet = {:?}", packet);
            return Err(StateError::WrongPacket);
        }
    };
    Ok(outgoing)
}

fn handle_incoming_suback() -> Result<Option<Packet>, StateError> {
    Ok(None)
}

fn handle_incoming_unsuback() -> Result<Option<Packet>, StateError> {
    Ok(None)
}

/// Results in a publish notification in all the QoS cases. Replys with an ack
/// in case of QoS1 and Replys rec in case of QoS while also storing the message
fn handle_incoming_publish(publish: &Publish) -> Result<Option<Packet>, StateError> {
    let qos = publish.qos;

    match qos {
        QoS::AtMostOnce => Ok(None),
        QoS::AtLeastOnce => {
            let puback = PubAck::new(publish.pkid);
            return Ok(Some(Packet::PubAck(puback)));
        }
        QoS::ExactlyOnce => {
            let pubrec = PubRec::new(publish.pkid);
            return Ok(Some(Packet::PubRec(pubrec)));
        }
    }
}

fn handle_incoming_puback(_puback: &PubAck) -> Result<Option<Packet>, StateError> {
    Ok(None)
}

fn handle_incoming_pubrec(pubrec: &PubRec) -> Result<Option<Packet>, StateError> {
    let pubrel = PubRel { pkid: pubrec.pkid };
    Ok(Some(Packet::PubRel(pubrel)))
}

fn handle_incoming_pubrel(pubrel: &PubRel) -> Result<Option<Packet>, StateError> {
    let pubcomp = PubComp { pkid: pubrel.pkid };
    Ok(Some(Packet::PubComp(pubcomp)))
}

fn handle_incoming_pubcomp(_pubcomp: &PubComp) -> Result<Option<Packet>, StateError> {
    Ok(None)
}

fn handle_incoming_pingresp() -> Result<Option<Packet>, StateError> {
    Ok(None)
}
