//! Structure that encapsulates communication, gossip, and discovery with the peers.

use crate::all_peers::AllPeers;
use crate::peer::Peer;
use crate::peer::PeerMessage;
use futures::future;
use futures::future::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc::channel;
use futures::sync::mpsc::Receiver;
use futures::sync::mpsc::Sender;
use log::warn;
use parking_lot::RwLock;
use primitives::types::AccountId;
use primitives::types::PeerId;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::timer::Interval;

/// Frequency of gossiping the peers info.
const GOSSIP_INTERVAL_MS: u64 = 5000;
/// How many peers should we gossip info to.
const GOSSIP_SAMPLE_SIZE: usize = 5;

pub struct PeerManager {
    /// `PeerId` of the current node.
    node_id: PeerId,
    /// `AccountId` of the current node.
    node_account_id: Option<AccountId>,
    all_peers: Arc<RwLock<AllPeers>>,
    /// Channel from which we receive incoming messages.
    inc_msg_tx: Sender<(PeerId, Vec<u8>)>,
}

impl PeerManager {
    pub fn new(
        node_id: PeerId,
        node_account_id: Option<AccountId>,
        boot_nodes: &Vec<SocketAddr>,
        inc_msg_tx: Sender<(PeerId, Vec<u8>)>,
    ) -> Self {
        let (check_new_peers_tx, check_new_peers_rx) = channel(1024);
        let res = Self {
            node_id,
            node_account_id,
            all_peers: Arc::new(RwLock::new(AllPeers::new(boot_nodes, check_new_peers_tx))),
            inc_msg_tx,
        };
        res.spawn(check_new_peers_rx);
        res
    }

    /// Spawns tasks for managing the peers.
    pub fn spawn(&self, check_new_peers_rx: Receiver<()>) {
        // Spawn the task that initializes peers.
        let all_peers = self.all_peers.clone();
        let node_id = self.node_id;
        let node_account_id = self.node_account_id.clone();
        let inc_msg_tx = self.inc_msg_tx.clone();
        let task = check_new_peers_rx
            .for_each(move |_| {
                for info in all_peers.write().lock_new_peers() {
                    let peer = Peer::from_known(
                        info.clone(),
                        node_id,
                        node_account_id.clone(),
                        all_peers.clone(),
                    );
                    tokio::spawn(
                        peer
                            .map_err(|e| warn!(target: "network", "Error receiving message: {}", e))
                            .forward(inc_msg_tx.clone().sink_map_err(
                                |e| warn!(target: "network", "Error forwarding incoming messages: {}", e),
                            ))
                            .map(|_| ()),
                    );
                }
                future::ok(())
            })
            .map(|_| ())
            .map_err(|_| warn!(target: "network", "Error checking for peers"));
        tokio::spawn(task);

        // Spawn the task that gossips.
        let all_peers = self.all_peers.clone();
        let task = Interval::new_interval(Duration::from_millis(GOSSIP_INTERVAL_MS))
            .for_each(|_| {
                future::ok(())
            });
    }
}
