use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{Future, Stream};
use futures::sync::mpsc::Receiver;
use tokio::timer::Interval;

use ::log::error;
use beacon::types::SignedBeaconBlock;
use primitives::types::{Gossip, UID, AuthorityStake};
use transaction::ChainPayload;

use crate::message::Message;
use crate::protocol::Protocol;
use crate::service::{NetworkEvent, Severity};

pub mod error;
pub mod message;
pub mod protocol;
pub mod service;
pub mod test_utils;

const TICK_TIMEOUT: Duration = Duration::from_millis(1000);

pub fn spawn_network_tasks(
    protocol_: Protocol,
    block_receiver: Receiver<SignedBeaconBlock>,
    authority_receiver: Receiver<HashMap<UID, AuthorityStake>>,
    gossip_rx: Receiver<Gossip<ChainPayload>>,
    event_rx: Receiver<NetworkEvent>,
) {
    let protocol = Arc::new(protocol_);
    // Interval for performing maintenance on the protocol handler.
    let timer = Interval::new_interval(TICK_TIMEOUT)
        .for_each({
            let protocol1 = protocol.clone();
            move |_| {
                for peer in protocol1.maintain_peers() {
                    error!("Dropping timeouted node {:?}.", peer);
                    protocol1.report_peer(peer, Severity::Timeout);
                }
                Ok(())
            }
        }).map_err(|e| error!("Error in the propagation timer: {}", e));

    let event_task = event_rx.for_each({
        let protocol = protocol.clone();
        move |event| {
            match event {
                NetworkEvent::PeerConnected { peer_id, .. } => {
                    protocol.on_peer_connected(peer_id);
                }
                NetworkEvent::Message { peer_id, data } => {
                    if let Err((peer, severity)) = protocol.on_message(peer_id, &data) {
                        protocol.report_peer(peer, severity);
                    }
                }
                NetworkEvent::PeerClosed { peer_id } => {
                    protocol.on_peer_disconnected(peer_id);
                }
            }
            Ok(())
        }
    });

    tokio::spawn(timer);
    tokio::spawn(event_task);

    let protocol1 = protocol.clone();
    let block_announce_handler = block_receiver.for_each(move |block| {
        protocol1.on_outgoing_block(&block);
        Ok(())
    });

    let protocol2 = protocol.clone();
    tokio::spawn(block_announce_handler);
    tokio::spawn(authority_receiver.for_each(move |map| {
        protocol2.set_authority_map(map);
        Ok(())
    }));

    let protocol3 = protocol.clone();
    let gossip_sender = gossip_rx.for_each(move |g| {
        if let Some(peer) = protocol3.get_peer_id_by_uid(g.receiver_uid) {
            let m = Message::Gossip(Box::new(g));
            protocol3.send_message(peer, m);
        } else {
            error!("Peer id not found for UID: {}", g.receiver_uid);
        }
        Ok(())
    });
    tokio::spawn(gossip_sender);
}