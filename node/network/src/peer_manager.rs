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
use primitives::types::PeerId;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::timer::Interval;

pub struct PeerManager {
    gossip_interval_ms: u64,
    gossip_sample_size: usize,
    node_info: PeerInfo,
    all_peers: Arc<RwLock<AllPeers>>,
    inc_msg_tx: Sender<(PeerId, Vec<u8>)>,
}

impl PeerManager {
    /// Args:
    /// * `gossip_interval_ms`: Frequency of gossiping the peers info;
    /// * `gossip_sample_size`: How many peers should we gossip info to;
    /// * `node_info`: Information about the current node;
    /// * `boot_nodes`: list of verified info about boot nodes from which we can join the network;
    /// * `inc_msg_tx`: channel from which we receive incoming messages;
    /// * `out_msg_tx`: channel into which we send outgoing messages.
    pub fn new(
        gossip_interval_ms: u64,
        gossip_sample_size: usize,
        node_info: PeerInfo,
        boot_nodes: &Vec<PeerInfo>,
        inc_msg_tx: Sender<(PeerId, Vec<u8>)>,
        out_msg_rx: Receiver<(PeerId, Vec<u8>)>,
    ) -> Self {
        let (check_new_peers_tx, check_new_peers_rx) = channel(1024);
        let node_info1 = node_info.clone();
        let res = Self {
            gossip_interval_ms,
            gossip_sample_size,
            node_info,
            all_peers: Arc::new(RwLock::new(AllPeers::new(
                node_info1,
                boot_nodes,
                check_new_peers_tx,
            ))),
            inc_msg_tx,
        };
        res.spawn(check_new_peers_rx, out_msg_rx);
        res
    }

    /// Spawns tasks for managing the peers.
    pub fn spawn(&self, check_new_peers_rx: Receiver<()>, out_msg_rx: Receiver<(PeerId, Vec<u8>)>) {
        // Spawn the task that initializes new peers.
        let all_peers = self.all_peers.clone();
        let node_info = self.node_info.clone();
        let inc_msg_tx = self.inc_msg_tx.clone();
        let task = check_new_peers_rx
            .for_each(move |_| {
                for info in all_peers.write().lock_new_peers() {
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

        let gossip_interval_ms = self.gossip_interval_ms;
        let gossip_sample_size = self.gossip_sample_size;
        // Spawn the task that gossips.
        let all_peers = self.all_peers.clone();
        let task = Interval::new_interval(Duration::from_millis(gossip_interval_ms))
            .for_each(move |_| {
                let guard = all_peers.read();
                let peers_info = guard.peers_info();
                for ch in guard.sample_ready_peers(gossip_sample_size) {
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
                all_peers.read().get_ready_channel(&id).map(|ch| (ch, data))
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
            let peer =  Peer::from_incoming_conn(node_info.clone(),
                                           socket, all_peers.clone());
            tokio::spawn(
                        peer
                            .map_err(|e| warn!(target: "network", "Error receiving message: {}", e))
                            .forward(inc_msg_tx.clone().sink_map_err(
                                |e| warn!(target: "network", "Error forwarding incoming messages: {}", e),
                            ))
                            .map(|_| ()),
                    );
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
    use parking_lot::RwLock;
    use primitives::hash::hash_struct;
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;
    use tokio::timer::Delay;
    use tokio::util::StreamExt;

    #[test]
    fn test_two_peers_boot() {
        // Spawn the first manager.
        let (out_msg_tx1, out_msg_rx1) = channel(1024);
        let (inc_msg_tx1, _) = channel(1024);
        let task = futures::lazy(move || {
            PeerManager::new(
                5000,
                1,
                PeerInfo {
                    id: hash_struct(&0),
                    addr: SocketAddr::from_str("127.0.0.1:4000").unwrap(),
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
        let (_, out_msg_rx2) = channel(1024);
        let (inc_msg_tx2, inc_msg_rx2) = channel(1024);
        let task = futures::lazy(move || {
            PeerManager::new(
                5000,
                1,
                PeerInfo {
                    id: hash_struct(&1),
                    addr: SocketAddr::from_str("127.0.0.1:4001").unwrap(),
                    account_id: None,
                },
                &vec![PeerInfo {
                    id: hash_struct(&0),
                    addr: SocketAddr::from_str("127.0.0.1:4000").unwrap(),
                    account_id: None,
                }],
                inc_msg_tx2,
                out_msg_rx2,
            );
            Ok(())
        });
        thread::spawn(move || tokio::run(task));

        // Create task that sends the message and then places it into `acc`.
        let acc = Arc::new(RwLock::new(None));
        let acc1 = acc.clone();
        let task = Delay::new(Instant::now() + Duration::from_millis(100))
            .then(move |_| {
                // Send message from `manager1` to `manager2`.
                out_msg_tx1
                    .send((hash_struct(&1), b"hello".to_vec()))
                    .map(|_| ())
                    .map_err(|e| panic!("Should not panic {}", e))
            })
            .then(move |_| {
                // Wait for 1 sec to receive it on `manager2`.
                inc_msg_rx2
                    .timeout(Duration::from_millis(1000))
                    .into_future()
                    .map(move |(el, _)| {
                        *acc.write() = Some(el);
                    })
                    .map_err(|_| panic!("Message was not received."))
            })
            .map(|_| ())
            .map_err(|_| panic!("Error verifying messages."));
        thread::spawn(move || tokio::run(task));

        wait(move || acc1.read().is_some(), 50, 1000);
    }

    #[test]
    fn test_five_five() {
        // Spawn five managers send five messages from each manager to another manager.
        // Manager i sends to manager j message five*i + j.

        const NUM_TASKS: usize = 5;

        let (mut v_out_msg_tx, mut v_inc_msg_rx) = (vec![], vec![]);

        for i in 0..NUM_TASKS {
            let (out_msg_tx, out_msg_rx) = channel(1024);
            let (inc_msg_tx, inc_msg_rx) = channel(1024);
            v_out_msg_tx.push(out_msg_tx);
            v_inc_msg_rx.push(inc_msg_rx);
            let task = futures::lazy(move || {
                let mut boot_nodes = vec![];
                if i != 0 {
                    boot_nodes.push(PeerInfo {
                        id: hash_struct(&(0 as usize)),
                        addr: SocketAddr::from_str("127.0.0.1:3000").unwrap(),
                        account_id: None,
                    });
                }
                PeerManager::new(
                    if i == 0 { 50 } else { 5000 },
                    if i == 0 { NUM_TASKS - 1 } else { 1 },
                    PeerInfo {
                        id: hash_struct(&i),
                        addr: SocketAddr::new("127.0.0.1".parse().unwrap(), 3000 + i as u16),
                        account_id: None,
                    },
                    &boot_nodes,
                    inc_msg_tx,
                    out_msg_rx,
                );
                Ok(())
            });
            thread::spawn(move || tokio::run(task));
        }

        let acc = Arc::new(RwLock::new(HashSet::new()));
        let acc1 = acc.clone();

        // Send 10 messages from each peer to each other peer and check the receival.
        let task = Delay::new(Instant::now() + Duration::from_millis(100))
            .map(move |_| {
                for i in 0..NUM_TASKS {
                    let mut messages = vec![];
                    for j in 0..NUM_TASKS {
                        if j != i {
                            messages.push((hash_struct(&j), vec![(NUM_TASKS * i + j) as u8]));
                        }
                    }
                    let task = v_out_msg_tx[i]
                        .clone()
                        .send_all(iter_ok(messages))
                        .map(|_| ())
                        .map_err(|_| panic!("Error sending messages"));
                    tokio::spawn(task);

                    // Create task that waits for 1 sec to receive message.
                    let inc_msg_rx = v_inc_msg_rx.remove(0);
                    let acc = acc.clone();
                    let task = inc_msg_rx
                        .for_each(move |msg| {
                            let (id, data) = msg;
                            let data = data[0] as usize;
                            let sender = data / NUM_TASKS;
                            let receiver = data % NUM_TASKS;
                            if hash_struct(&sender) == id && receiver == i {
                                acc.write().insert(data);
                            }
                            future::ok(())
                        })
                        .map(|_| ())
                        .map_err(|_| ());
                    tokio::spawn(task);
                }
            })
            .map(|_| ())
            .map_err(|_| ());
        thread::spawn(move || tokio::run(task));

        wait(move || acc1.read().len() == NUM_TASKS * (NUM_TASKS - 1), 50, 1000);
    }

    fn wait<F>(f: F, check_interval_ms: u64, max_wait_ms: u64)
    where
        F: Fn() -> bool,
    {
        let mut ms_slept = 0;
        while !f() {
            thread::sleep(Duration::from_millis(check_interval_ms));
            ms_slept += check_interval_ms;
            if ms_slept > max_wait_ms {
                panic!("Timed out waiting for the condition");
            }
        }
    }
}
