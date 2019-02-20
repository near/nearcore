use std::collections::HashSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;
use std::time::Duration;

use futures::future;
use futures::future::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc;
use futures::sync::mpsc::channel;
use futures::sync::mpsc::Receiver;
use futures::sync::mpsc::Sender;
use log::{error, warn};

use client::Client;
use configs::NetworkConfig;
use nightshade::nightshade_task::Control;
use nightshade::nightshade_task::Gossip;
use primitives::hash::CryptoHash;
use primitives::hash::hash_struct;
use primitives::network::PeerInfo;
use primitives::serialize::{Decode, Encode};
use primitives::types::AccountId;

use crate::message::Message;
use crate::peer::PeerMessage;
use crate::peer_manager::PeerManager;
use crate::testing_utils::wait_all_peers_connected;

/// Spawn network tasks that process incoming and outgoing gossips for nightshade consensus
///
/// Args:
/// * `account_id`: Optional account id of the node;
/// * `network_cfg`: `NetworkConfig` object;
/// * `inc_gossip_tx`: Channel where protocol places incoming gossip;
/// * `out_gossip_rx`: Channel where from protocol reads gossip that should be sent to other peers;
//pub fn spawn_consensus_network<P>(
//    account_id: Option<AccountId>,
//    network_cfg: NetworkConfig,
//    inc_gossip_tx: mpsc::Sender<Gossip<P>>,
//    out_gossip_rx: mpsc::Receiver<Gossip<P>>,
//) {
//    let (inc_msg_tx, inc_msg_rx) = mpsc::channel(1024);
//    let (out_msg_tx, out_msg_rx) = mpsc::channel(1024);
//
//    let peer_manager = Arc::new(PeerManager::new(
//        network_cfg.reconnect_delay,
//        network_cfg.gossip_interval,
//        network_cfg.gossip_sample_size,
//        PeerInfo {
//            id: network_cfg.peer_id,
//            addr: network_cfg.listen_addr,
//            account_id,
//        },
//        &network_cfg.boot_nodes,
//        inc_msg_tx,
//        out_msg_rx,
//    ));
//
//    // Spawn a task that decodes incoming messages and places them in the corresponding channels.
//    let task = inc_msg_rx.for_each(move |(_, data)| {
//        match Decode::decode(&data) {
//            Ok(gossip) => forward_msg(inc_gossip_tx.clone(), gossip),
//            Err(e) => error!(target: "network", "Error deserialising data {:?}", e),
//        }
//    });
//    tokio::spawn(task);
//
//    // Spawn a task that encodes and sends outgoing gossips.
//    let peer_manager1 = peer_manager.clone();
//    let task = out_gossip_rx.for_each(move |g| {
//        let auth_map = client1.get_recent_uid_to_authority_map();
//        let out_channel = auth_map
//            .get(&g.receiver_uid)
//            .map(|auth| auth.account_id.clone())
//            .and_then(|account_id| peer_manager1.get_account_channel(account_id));
//        if let Some(ch) = out_channel {
//            let data = Encode::encode(&Message::Gossip(Box::new(g))).unwrap();
//            forward_msg(ch, PeerMessage::Message(data));
//        }
//        future::ok(())
//    });
//    tokio::spawn(task);
//}

fn forward_msg<T>(ch: Sender<T>, el: T)
    where T: Send + 'static,
{
    let task =
        ch.send(el).map(|_| ()).map_err(|e| warn!(target: "network", "Error forwarding {}", e));
    tokio::spawn(task);
}

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Helper function for alphanet. Will be removed.
/// Launch `num_authorities` nodes and connect them.
pub fn start_peers(num_authorities: usize) -> (Vec<Sender<(CryptoHash, Vec<u8>)>>,
                                               Vec<Receiver<(CryptoHash, Vec<u8>)>>,
                                               Arc<RwLock<Vec<PeerManager>>>) {
    let mut out_msg_tx_vec = vec![];
    let mut inc_msg_rx_vec = vec![];
    let all_pms = Arc::new(RwLock::new(vec![]));

    // Launch all authorities booting in star
    for a in 0..num_authorities {
        let (out_msg_tx, out_msg_rx) = channel(1024);
        let (inc_msg_tx, inc_msg_rx) = channel(1024);

        out_msg_tx_vec.push(out_msg_tx);
        inc_msg_rx_vec.push(inc_msg_rx);

        let all_pms1 = all_pms.clone();

        let task = futures::lazy(move || {
            // All nodes boot from node zero
            let mut boot_nodes = vec![];

            if a != 0 {
                boot_nodes.push(PeerInfo {
                    id: hash_struct(&(0 as usize)),
                    addr: SocketAddr::from_str("127.0.0.1:3000").unwrap(),
                    account_id: None,
                });
            }

            let pm = PeerManager::new(
                Duration::from_millis(50),
                Duration::from_millis(100),
                if a == 0 { num_authorities - 1 } else { 1 },
                PeerInfo {
                    id: hash_struct(&a),
                    addr: SocketAddr::new("127.0.0.1".parse().unwrap(), 3000 + a as u16),
                    account_id: None,
                },
                &boot_nodes,
                inc_msg_tx,
                out_msg_rx,
            );

            all_pms1.write().expect(POISONED_LOCK_ERR).push(pm);
            Ok(())
        });

        thread::spawn(move || tokio::run(task));
    }

    wait_all_peers_connected(50, 10000, &all_pms, num_authorities);

    (out_msg_tx_vec, inc_msg_rx_vec, all_pms)
}