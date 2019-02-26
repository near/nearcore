use futures::future;
use futures::future::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc::{channel, Receiver, Sender};
use log::{error, info, warn};

use nightshade::nightshade_task::Gossip;
use primitives::chain::ChainPayload;
use primitives::network::PeerInfo;
use primitives::serialize::{Decode, Encode};
use primitives::types::AccountId;

use crate::peer::PeerMessage;
use crate::peer_manager::PeerManager;
use client::Client;
use configs::NetworkConfig;
use std::sync::Arc;

/// Spawn network task that process incoming and outgoing gossips for nightshade consensus
///
/// Args:
pub fn spawn_consensus_network(
    account_id: Option<AccountId>,
    network_cfg: NetworkConfig,
    client: Arc<Client>,
    inc_gossip_tx: Sender<Gossip<ChainPayload>>,
    out_gossip_rx: Receiver<Gossip<ChainPayload>>,
) {
    let (inc_msg_tx, inc_msg_rx) = channel(1024);
    let (_out_msg_tx, out_msg_rx) = channel(1024);

    let peer_manager = Arc::new(PeerManager::new(
        network_cfg.reconnect_delay,
        network_cfg.gossip_interval,
        network_cfg.gossip_sample_size,
        PeerInfo { id: network_cfg.peer_id, addr: network_cfg.listen_addr, account_id },
        &network_cfg.boot_nodes,
        inc_msg_tx,
        out_msg_rx,
    ));

    // Spawn a task that decodes incoming messages and places them in the corresponding channels.
    let task = inc_msg_rx.for_each(move |(_, data)| {
        match Decode::decode(&data) {
            Ok(gossip) => forward_msg(inc_gossip_tx.clone(), gossip),
            Err(e) => warn!(target: "network", "Error decoding gossip: {}", e),
        };
        future::ok(())
    });
    tokio::spawn(task);

    // Spawn a task that encodes and route outgoing gossips to receiver.
    let client1 = client.clone();
    let task = out_gossip_rx.for_each(move |g| {
        let authority_map = client1.get_recent_uid_to_authority_map();
        info!("Sending gossip: {} -> {}", g.sender_id, g.receiver_id);
        let receiver_channel = authority_map
            .get(&(g.receiver_id as u64))
            .and_then(|acc_id| peer_manager.get_account_channel(acc_id.account_id.clone()));

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
where
    T: Send + 'static,
{
    let task =
        ch.send(el).map(|_| ()).map_err(|e| warn!(target: "network", "Error forwarding {}", e));
    tokio::spawn(task);
}
