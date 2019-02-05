use crate::message::Message;
use crate::peer::PeerMessage;
use crate::peer_manager::PeerManager;
use beacon::types::SignedBeaconBlock;
use chain::ChainPayload;
use chain::SignedShardBlock;
use client::Client;
use configs::NetworkConfig;
use futures::future;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc::channel;
use futures::sync::mpsc::Receiver;
use futures::sync::mpsc::Sender;
use futures::Future;
use log::warn;
use primitives::network::PeerInfo;
use primitives::serialize::{Decode, Encode};
use primitives::types::AccountId;
use primitives::types::Gossip;
use std::sync::Arc;

/// Spawn network tasks that process incoming and outgoing messages of various kind.
/// Args:
/// * `account_id`: Optional account id of the node;
/// * `network_cfg`: `NetworkConfig` object;
/// * `client`: Shared Client object which we use to get the list of authorities, and use for
///   exporting, importing blocks;
/// * `inc_gossip_tx`: Channel where protocol places incoming TxFlow gossip;
/// * `out_gossip_rx`: Channel where from protocol reads gossip that should be sent to other peers;
/// * `inc_block_tx`: Channel where protocol places incoming blocks;
/// * `out_blocks_rx`: Channel where from protocol reads blocks that should be sent for
///   announcements.
pub fn spawn_network(
    account_id: Option<AccountId>,
    network_cfg: NetworkConfig,
    client: Arc<Client>,
    inc_gossip_tx: Sender<Gossip<ChainPayload>>,
    out_gossip_rx: Receiver<Gossip<ChainPayload>>,
    inc_block_tx: Sender<(SignedBeaconBlock, SignedShardBlock)>,
    out_block_rx: Receiver<(SignedBeaconBlock, SignedShardBlock)>,
) {
    let (inc_msg_tx, inc_msg_rx) = channel(1024);
    let (_, out_msg_rx) = channel(1024);

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
            Ok(m) => match m {
                Message::Gossip(gossip) => forward_msg(inc_gossip_tx.clone(), *gossip),
                Message::BlockAnnounce(block) => {
                    let unboxed = *block;
                    forward_msg(inc_block_tx.clone(), (unboxed.0, unboxed.1));
                }
                _ => (),
            },
            Err(e) => warn!(target: "network", "{}", e),
        };
        future::ok(())
    });
    tokio::spawn(task);

    // Spawn a task that encodes and sends outgoing gossips.
    let client1 = client.clone();
    let peer_manager1 = peer_manager.clone();
    let task = out_gossip_rx.for_each(move |g| {
        let auth_map = client1.get_recent_uid_to_authority_map();
        let out_channel = auth_map
            .get(&g.receiver_uid)
            .map(|auth| auth.account_id.clone())
            .and_then(|account_id| peer_manager1.get_account_channel(account_id));
        if let Some(ch) = out_channel {
            let data = Encode::encode(&Message::Gossip(Box::new(g))).unwrap();
            forward_msg(ch, PeerMessage::Message(data));
        }
        future::ok(())
    });
    tokio::spawn(task);

    // Spawn a task that encodes and sends outgoing block announcements.
    let task = out_block_rx.for_each(move |b| {
        let data = Encode::encode(&Message::BlockAnnounce(Box::new((
            b.0.clone(),
            b.1.clone(),
        ))))
        .unwrap();
        for ch in peer_manager.get_ready_channels() {
            forward_msg(ch, PeerMessage::Message(data.to_vec()));
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
