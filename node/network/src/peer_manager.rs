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
use tokio::net::TcpListener;
use tokio::timer::Interval;

/// Frequency of gossiping the peers info.
const GOSSIP_INTERVAL_MS: u64 = 5000;
/// How many peers should we gossip info to.
const GOSSIP_SAMPLE_SIZE: usize = 5;

pub struct PeerManager {
    listen_addr: SocketAddr,
    node_id: PeerId,
    node_account_id: Option<AccountId>,
    all_peers: Arc<RwLock<AllPeers>>,
    inc_msg_tx: Sender<(PeerId, Vec<u8>)>,
}

impl PeerManager {
    /// Args:
    /// * `listen_addr`: `SocketAddr` on which we listen for incoming connections;
    /// * `node_id`: `PeerId` of the current node;
    /// * `node_account_id`: Optional `AccountId` of the current node;
    /// * `boot_nodes`: list of addresses from which we join the network;
    /// * `inc_msg_tx`: channel from which we receive incoming messages;
    /// * `out_msg_tx`: channel into which we send outgoing messages.
    pub fn new(
        listen_addr: SocketAddr,
        node_id: PeerId,
        node_account_id: Option<AccountId>,
        boot_nodes: &Vec<SocketAddr>,
        inc_msg_tx: Sender<(PeerId, Vec<u8>)>,
        out_msg_rx: Receiver<(PeerId, Vec<u8>)>,
    ) -> Self {
        assert!(!boot_nodes.is_empty(), "Network needs at least one boot node.");
        let (check_new_peers_tx, check_new_peers_rx) = channel(1024);
        let res = Self {
            listen_addr,
            node_id,
            node_account_id,
            all_peers: Arc::new(RwLock::new(AllPeers::new(boot_nodes, check_new_peers_tx))),
            inc_msg_tx,
        };
        res.spawn(check_new_peers_rx, out_msg_rx);
        res
    }

    /// Spawns tasks for managing the peers.
    pub fn spawn(&self, check_new_peers_rx: Receiver<()>, out_msg_rx: Receiver<(PeerId, Vec<u8>)>) {
        // Spawn the task that initializes new peers.
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
            .for_each(move |_| {
                let guard = all_peers.read();
                let peers_info = guard.peers_info();
                for ch in guard.sample_ready_peers(GOSSIP_SAMPLE_SIZE) {
                    tokio::spawn(
                        ch.send(PeerMessage::InfoGossip(peers_info.clone()))
                            .map(|_| ())
                            .map_err(|_| warn!(target: "network", "Error gossiping peers info.")),
                    );
                }
                future::ok(())
            })
            .map(|_| ())
            .map_err(|e| warn!(target: "network", "Error gossiping peers info {}", e));
        tokio::spawn(task);

        // Spawn the task that forwards outgoing messages to the appropriate peers.
        let all_peers = self.all_peers.clone();
        let task = out_msg_rx
            .filter_map(move |(id, data)| {
                all_peers.read().get_ready_peer_by_id(&id).map(|ch| (ch, data))
            })
            .for_each(|(ch, data)| {
                ch.send(PeerMessage::Message(data))
                    .map(|_| ())
                    .map_err(|_| warn!(target: "network", "Error sending message to the peer"))
            })
            .map(|_| ());
        tokio::spawn(task);

        // Spawn the task that listens to incoming connections.
        let node_id = self.node_id;
        let node_account_id = self.node_account_id.clone();
        let all_peers = self.all_peers.clone();
        let inc_msg_tx = self.inc_msg_tx.clone();
        let task = TcpListener::bind(&self.listen_addr).expect("Cannot listen to the adress")
            .incoming().for_each(move |socket| {
            match Peer::from_incoming_conn(node_id, node_account_id.clone(), socket, all_peers.clone()) {
                Ok(peer) => {
                    tokio::spawn(
                        peer
                            .map_err(|e| warn!(target: "network", "Error receiving message: {}", e))
                            .forward(inc_msg_tx.clone().sink_map_err(
                                |e| warn!(target: "network", "Error forwarding incoming messages: {}", e),
                            ))
                            .map(|_| ()),
                    );
                },
                Err(e) => {warn!(target: "network", "Rejecting incoming connection {}", e); },
            }
            future::ok(())
        }).map(|_| ())
            .map_err(|e| warn!(target: "network", "Error processing incomming connection {}", e));
        tokio::spawn(task);
    }
}
