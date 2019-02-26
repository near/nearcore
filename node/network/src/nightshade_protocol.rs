use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use futures::future;
use futures::future::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc::{channel, Receiver, Sender};
use log::{warn, info, error};

use chain::ChainPayload;
use nightshade::nightshade::AuthorityId;
use nightshade::nightshade_task::Gossip;
use primitives::hash::CryptoHash;
use primitives::hash::hash_struct;
use primitives::network::PeerInfo;
use primitives::serialize::{Decode, Encode};
use primitives::types::AccountId;

use crate::peer::PeerMessage;
use crate::peer_manager::PeerManager;
use std::sync::Arc;
use configs::NetworkConfig;

/// Spawn network task that process incoming and outgoing gossips for nightshade consensus
///
/// Args:
pub fn spawn_consensus_network(
    account_id: Option<AccountId>,
    network_cfg: NetworkConfig,
    inc_gossip_tx: Sender<Gossip<ChainPayload>>,
    out_gossip_rx: Receiver<Gossip<ChainPayload>>,
    authority_map: HashMap<AuthorityId, AccountId>,
) {
    let (inc_msg_tx, inc_msg_rx) = channel(1024);
    let (_out_msg_tx, out_msg_rx) = channel(1024);

    let peer_manager = Arc::new(PeerManager::new(
        network_cfg.reconnect_delay,
        network_cfg.gossip_interval,
        network_cfg.gossip_sample_size,
        PeerInfo {
            id: network_cfg.peer_id,
            addr: network_cfg.listen_addr,
            account_id,
        },
        &network_cfg.boot_nodes,
        inc_msg_tx,
        out_msg_rx,
    ));

    // Spawn a task that decodes incoming messages and places them in the corresponding channels.
    let task = inc_msg_rx.for_each(move |(_, data)| {
        match Decode::decode(&data) {
            Ok(gossip) => forward_msg(inc_gossip_tx.clone(), gossip),
            Err(e) => warn!(target: "network", "Error decoding gossip: {}", e)
        };
        future::ok(())
    });
    tokio::spawn(task);

    // Spawn a task that encodes and route outgoing gossips to receiver.
    let task = out_gossip_rx.for_each(move |g| {
        info!("Sending gossip: {} -> {}", g.sender_id, g.receiver_id);
        let receiver_channel = authority_map.get(&g.receiver_id).and_then(|acc_id| {
            peer_manager.get_account_channel(acc_id.clone())
        });

        if let Some(ch) = receiver_channel {
            let data = Encode::encode(&g).unwrap();
            forward_msg(ch, PeerMessage::Message(data));
        } else {
            error!("Channel not found!");
        }
        future::ok(())
    });
    tokio::spawn(task);
}

fn forward_msg<T>(ch: Sender<T>, el: T)
    where T: Send + 'static,
{
    let task =
        ch.send(el).map(|_| ()).map_err(|e| warn!(target: "network", "Error forwarding {}", e));
    tokio::spawn(task);
}

// TODO: Document this function
pub fn start_peer(
    authority: usize,
    num_authorities: usize,
    account_id: Option<AccountId>,
    boot_nodes: Vec<PeerInfo>,
)
    -> (PeerManager, Sender<(CryptoHash, Vec<u8>)>, Receiver<(CryptoHash, Vec<u8>)>)
{
    let (out_msg_tx, out_msg_rx) = channel(1024);
    let (inc_msg_tx, inc_msg_rx) = channel(1024);

    let pm = PeerManager::new(
        Duration::from_millis(50),
        Duration::from_millis(100),
        if authority == 0 { num_authorities - 1 } else { 1 },
        PeerInfo {
            id: hash_struct(&authority),
            addr: SocketAddr::new("127.0.0.1".parse().unwrap(), 3000 + authority as u16),
            account_id,
        },
        &boot_nodes,
        inc_msg_tx,
        out_msg_rx,
    );

    (pm, out_msg_tx, inc_msg_rx)
}
