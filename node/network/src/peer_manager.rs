//! Structure that encapsulates communication, gossip, and discovery with the peers.

use crate::all_peers::AllPeers;
use crate::peer::Peer;
use crate::peer::PeerInfo;
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
const GOSSIP_INTERVAL_MS: u64 = 50;
/// How many peers should we gossip info to.
const GOSSIP_SAMPLE_SIZE: usize = 5;
/// Only happens if we made a mistake in our code and allowed certain optional fields to be None
/// during the states that they are not supposed to be None.
const STATE_ERR: &str = "Some fields are expected to be not None at the given state";

pub struct PeerManager {
    node_info: PeerInfo,
    all_peers: Arc<RwLock<AllPeers>>,
    inc_msg_tx: Sender<(PeerInfo, Vec<u8>)>,
}

impl PeerManager {
    /// Args:
    /// * `node_info`: Information about the current node;
    /// * `boot_nodes`: list of verified info about boot nodes from which we can join the network;
    /// * `inc_msg_tx`: channel from which we receive incoming messages;
    /// * `out_msg_tx`: channel into which we send outgoing messages.
    pub fn new(
        node_info: PeerInfo,
        boot_nodes: &Vec<PeerInfo>,
        inc_msg_tx: Sender<(PeerInfo, Vec<u8>)>,
        out_msg_rx: Receiver<(PeerInfo, Vec<u8>)>,
    ) -> Self {
        let (check_new_peers_tx, check_new_peers_rx) = channel(1024);
        let res = Self {
            node_info,
            all_peers: Arc::new(RwLock::new(AllPeers::new(boot_nodes, check_new_peers_tx))),
            inc_msg_tx,
        };
        res.spawn(check_new_peers_rx, out_msg_rx);
        res
    }

    /// Spawns tasks for managing the peers.
    pub fn spawn(
        &self,
        check_new_peers_rx: Receiver<()>,
        out_msg_rx: Receiver<(PeerInfo, Vec<u8>)>,
    ) {
        // Spawn the task that initializes new peers.
        let all_peers = self.all_peers.clone();
        let node_info = self.node_info.clone();
        let inc_msg_tx = self.inc_msg_tx.clone();
        let task = check_new_peers_rx
            .for_each(move |_| {
                for info in all_peers.write().lock_new_peers() {
                    println!("{} Locked new peer {}", &node_info, &info);
                    let peer = Peer::from_known(
                        node_info.clone(),
                        info,
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
            .filter_map(move |(peer_info, data)| {
                all_peers.read().get_ready_channel(&peer_info).map(|ch| (ch, data))
            })
            .for_each(|(ch, data)| {
                ch.send(PeerMessage::Message(data))
                    .map(|_| ())
                    .map_err(|_| warn!(target: "network", "Error sending message to the peer"))
            })
            .map(|_| ());
        tokio::spawn(task);

        // Spawn the task that listens to incoming connections.
        let node_info = self.node_info.clone();
        let all_peers = self.all_peers.clone();
        let inc_msg_tx = self.inc_msg_tx.clone();
        let task = TcpListener::bind(&node_info.addr).expect("Cannot listen to the address")
            .incoming().for_each(move |socket| {
            println!("{} got incoming {}", &node_info, socket.peer_addr().unwrap());
            match Peer::from_incoming_conn(node_info.clone(),
                                           socket, all_peers.clone()) {
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

#[cfg(test)]
mod tests {
    use crate::peer::PeerInfo;
    use crate::peer_manager::PeerManager;
    use futures::future;
    use futures::future::Future;
    use futures::sink::Sink;
    use futures::stream::{iter_ok, Stream};
    use futures::sync::mpsc::channel;
    use primitives::hash::hash_struct;
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;
    use tokio::timer::Delay;
    use tokio::util::StreamExt;

    #[test]
    fn test_two_peers_boot() {
        // Spawn the first manager.
        let (out_msg_tx1, out_msg_rx1) = channel(1024);
        let (inc_msg_tx1, inc_msg_rx1) = channel(1024);
        let task = futures::lazy(move || {
            let manager1 = PeerManager::new(
                PeerInfo {
                    id: hash_struct(&0),
                    addr: SocketAddr::from_str("127.0.0.1:3000").unwrap(),
                    account_id: None,
                },
                &vec![],
                inc_msg_tx1,
                out_msg_rx1,
            );
            Ok(())
        });
        thread::spawn(move || tokio::run(task));

        // Spawn the second manager and boot it from the first one.
        let (out_msg_tx2, out_msg_rx2) = channel(1024);
        let (inc_msg_tx2, inc_msg_rx2) = channel(1024);
        let task = futures::lazy(move || {
            let manager2 = PeerManager::new(
                PeerInfo {
                    id: hash_struct(&1),
                    addr: SocketAddr::from_str("127.0.0.1:3001").unwrap(),
                    account_id: None,
                },
                &vec![PeerInfo {
                    id: hash_struct(&0),
                    addr: SocketAddr::from_str("127.0.0.1:3000").unwrap(),
                    account_id: None,
                }],
                inc_msg_tx2,
                out_msg_rx2,
            );
            Ok(())
        });
        thread::spawn(move || tokio::run(task));

        // Create task that waits for the message to be transmitted.
        let task = Delay::new(Instant::now() + Duration::from_millis(10))
            .then(move |_| {
                // Send message from `manager1` to `manager2`.
                out_msg_tx1
                    .send((
                        PeerInfo {
                            id: hash_struct(&1),
                            addr: SocketAddr::from_str("127.0.0.1:3001").unwrap(),
                            account_id: None,
                        },
                        b"hello".to_vec(),
                    ))
                    .map(|_| ())
                    .map_err(|e| panic!("Should not panic {}", e))
            })
            .then(move |_| {
                // Wait for 1 sec to receive it on `manager2`.
                inc_msg_rx2
                    .timeout(Duration::from_millis(1000))
                    .into_future()
                    .map(|(el, _)| println!("Received! {:?}", el))
                    .map_err(|_| panic!("Message was not received."))
            })
            .map(|_| ())
            .map_err(|_| panic!("Error verifying messages."));
        tokio::run(task);
    }

    //    #[test]
    //    fn test_ten_ten() {
    //        // Spawn ten managers send ten messages from each manager to another manager.
    //        // Manager i sends to manager j message 10*i + j.
    //
    //        const NUM_TASKS: usize = 3;
    //
    //        let (mut v_out_msg_tx, mut v_inc_msg_rx) = (vec![], vec![]);
    //
    //        for i in 0..NUM_TASKS {
    //            let (out_msg_tx, out_msg_rx) = channel(1024);
    //            let (inc_msg_tx, inc_msg_rx) = channel(1024);
    //            v_out_msg_tx.push(out_msg_tx);
    //            v_inc_msg_rx.push(inc_msg_rx);
    //            let task = futures::lazy(move || {
    //                let mut boot_nodes = vec![];
    //                if i != 0 {
    //                    boot_nodes.push(SocketAddr::from_str("127.0.0.1:3000").unwrap());
    //                }
    //                let manager = PeerManager::new(
    //                    SocketAddr::new("127.0.0.1".parse().unwrap(), 3000 + i as u16),
    //                    hash_struct(&i),
    //                    None,
    //                    &boot_nodes,
    //                    inc_msg_tx,
    //                    out_msg_rx,
    //                );
    //                Ok(())
    //            });
    //            thread::spawn(move || tokio::run(task));
    //        }
    //
    //        // Send 10 messages from each peer to each other peer and check the receival.
    //        let task = Delay::new(Instant::now() + Duration::from_millis(5000))
    //            .map(move |_| {
    //                for i in 0..NUM_TASKS {
    //                    let mut messages = vec![];
    //                    for j in (i + 1)..NUM_TASKS {
    //                        messages.push((hash_struct(&j), vec![(NUM_TASKS * i + j) as u8]));
    //                    }
    //                    let task = v_out_msg_tx[i]
    //                        .clone()
    //                        .send_all(iter_ok(messages))
    //                        .map(|_| ())
    //                        .map_err(|_| panic!("Error sending messages"));
    //                    tokio::spawn(task);
    //
    //                    // Create task that waits for 1 sec to receive message.
    //                    let inc_msg_rx = v_inc_msg_rx.remove(0);
    //                    let task = inc_msg_rx
    //                        .timeout(Duration::from_millis(1000))
    //                        .fold(HashSet::new(), move |mut acc, msg| {
    //                            let (id, data) = msg;
    //                            let data = data[0] as usize;
    //                            println!("Message received {}", data);
    //                            let sender = data / NUM_TASKS;
    //                            let receiver = data % NUM_TASKS;
    //                            assert_eq!(hash_struct(&sender), id, "Sender mismatch");
    //                            assert_eq!(receiver, i, "Receiver mismatch");
    //                            acc.insert(sender);
    //                            future::ok(acc)
    //                        })
    //                        .and_then(|acc| {
    //                            let mut expected = HashSet::new();
    //                            expected.extend(0..NUM_TASKS);
    //                            assert_eq!(acc, expected, "Did not receive expected messages");
    //                            future::ok(())
    //                        })
    //                        .map(|_| ())
    //                        .map_err(|_| panic!("Error verifying messages."));
    //                    tokio::spawn(task);
    //                }
    //            })
    //            .map(|_| ())
    //            .map_err(|_| panic!("Error verifying messages"));
    //        tokio::run(task);
    //    }
}
