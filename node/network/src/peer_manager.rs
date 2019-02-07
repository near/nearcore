//! Structure that encapsulates communication, gossip, and discovery with the peers.

use crate::peer::Peer;
use crate::peer::PeerState;
use crate::peer::{AllPeerStates, PeerMessage};
use futures::future;
use futures::future::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc::Receiver;
use futures::sync::mpsc::Sender;
use log::warn;
use primitives::network::PeerInfo;
use primitives::types::AccountId;
use primitives::types::PeerId;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::timer::Interval;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub struct PeerManager {
    all_peer_states: AllPeerStates,
}

impl PeerManager {
    /// Args:
    /// * `reconnect_delay`: How long should we wait before connecting a newly discovered peer or
    ///   reconnecting the old one;
    /// * `gossip_interval`: Frequency of gossiping the peers info;
    /// * `gossip_sample_size`: How many peers should we gossip info to;
    /// * `node_info`: Information about the current node;
    /// * `boot_nodes`: list of verified info about boot nodes from which we can join the network;
    /// * `inc_msg_tx`: where `PeerManager` should be sending incoming messages;
    /// * `out_msg_rx`: where from `PeerManager` should be getting outgoing messages.
    pub fn new(
        reconnect_delay: Duration,
        gossip_interval: Duration,
        gossip_sample_size: usize,
        node_info: PeerInfo,
        boot_nodes: &Vec<PeerInfo>,
        inc_msg_tx: Sender<(PeerId, Vec<u8>)>,
        out_msg_rx: Receiver<(PeerId, Vec<u8>)>,
    ) -> Self {
        let all_peer_states = Arc::new(RwLock::new(HashMap::new()));
        // Spawn peers that represent boot nodes.
        Peer::spawn_from_known(
            node_info.clone(),
            boot_nodes.to_vec(),
            all_peer_states.clone(),
            &mut all_peer_states.write().expect(POISONED_LOCK_ERR),
            inc_msg_tx.clone(),
            reconnect_delay,
            // Connect to the boot nodes immediately.
            Instant::now(),
        );

        // Spawn the task that forwards outgoing messages to the appropriate peers.
        let all_peer_states1 = all_peer_states.clone();
        let task = out_msg_rx
            .filter_map(move |(id, data)| {
                let states_guard = all_peer_states1.read().expect(POISONED_LOCK_ERR);
                states_guard.get(&id).and_then(|locked_peer| {
                    let locked_peer_guard = locked_peer.read().expect(POISONED_LOCK_ERR);
                    match locked_peer_guard.deref() {
                        // We only use peers that are ready to communicate.
                        PeerState::Ready { out_msg_tx, .. } => Some((out_msg_tx.clone(), data)),
                        _ => None,
                    }
                })
            })
            .for_each(|(ch, data)| {
                ch.send(PeerMessage::Message(data))
                    .map(|_| ())
                    .map_err(|_| warn!(target: "network", "Error sending message to the peer"))
            })
            .map(|_| ());
        tokio::spawn(task);

        // Spawn the task that gossips.
        let all_peer_states2 = all_peer_states.clone();
        let task = Interval::new_interval(gossip_interval)
            .for_each(move |_| {
                let guard = all_peer_states2.read().expect(POISONED_LOCK_ERR);
                let peers_info: Vec<_> = guard.keys().cloned().collect();
                let mut rng = thread_rng();
                let sampled_peers = guard
                    .iter()
                    .filter_map(|(_, state)| {
                        if let PeerState::Ready { out_msg_tx, .. } =
                            state.read().expect(POISONED_LOCK_ERR).deref()
                        {
                            Some(out_msg_tx.clone())
                        } else {
                            None
                        }
                    })
                    .choose_multiple(&mut rng, gossip_sample_size);
                for ch in sampled_peers {
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

        // Spawn the task that listens to incoming connections.
        let all_peer_states3 = all_peer_states.clone();
        let task = TcpListener::bind(&node_info.addr)
            .expect("Cannot listen to the address")
            .incoming()
            .for_each(move |socket| {
                Peer::spawn_incoming_conn(
                    node_info.clone(),
                    socket,
                    all_peer_states3.clone(),
                    inc_msg_tx.clone(),
                    reconnect_delay,
                );
                future::ok(())
            })
            .map(|_| ())
            .map_err(|e| warn!(target: "network", "Error processing incomming connection {}", e));
        tokio::spawn(task);

        Self { all_peer_states }
    }

    /// Get channel for the given `account_id`, if the corrresponding peer is `Ready`.
    pub fn get_account_channel(&self, account_id: AccountId) -> Option<Sender<PeerMessage>> {
        self.all_peer_states.read().expect(POISONED_LOCK_ERR).iter().find_map(|(info, state)| {
            if info.account_id.as_ref() == Some(&account_id) {
                match state.read().expect(POISONED_LOCK_ERR).deref() {
                    PeerState::Ready { out_msg_tx, .. } => Some(out_msg_tx.clone()),
                    _ => {
                        None
                    },
                }
            } else {
                None
            }
        })
    }

    /// Get channels of all peers that are `Ready`.
    pub fn get_ready_channels(&self) -> Vec<Sender<PeerMessage>> {
        self.all_peer_states
            .read()
            .expect(POISONED_LOCK_ERR)
            .values()
            .filter_map(|state| match state.read().expect(POISONED_LOCK_ERR).deref() {
                PeerState::Ready { out_msg_tx, .. } => Some(out_msg_tx.clone()),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::peer::PeerState;
    use crate::peer_manager::{PeerManager, POISONED_LOCK_ERR};
    use futures::future;
    use futures::future::Future;
    use futures::sink::Sink;
    use futures::stream::{iter_ok, Stream};
    use futures::sync::mpsc::channel;
    use primitives::hash::hash_struct;
    use primitives::network::PeerInfo;
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::Duration;
    use tokio::util::StreamExt;
    use std::ops::Deref;

    impl PeerManager {
        fn count_ready_channels(&self) -> usize {
            self.all_peer_states
                .read()
                .expect(POISONED_LOCK_ERR)
                .values()
                .filter_map(|state| match state.read().expect(POISONED_LOCK_ERR).deref() {
                    PeerState::Ready { .. } => Some(1 as usize),
                    _ => None,
                })
                .sum()
        }
    }

    fn wait_all_peers_connected(
        check_interval_ms: u64,
        max_wait_ms: u64,
        peer_managers: &Arc<RwLock<Vec<PeerManager>>>,
        expected_num_managers: usize,
    ) {
        wait(
            || {
                let guard = peer_managers.read().expect(POISONED_LOCK_ERR);
                guard.len() == expected_num_managers
                    && guard.iter().all(|pm| pm.count_ready_channels() == expected_num_managers - 1)
            },
            check_interval_ms,
            max_wait_ms,
        );
    }

    #[test]
    fn test_two_peers_boot() {
        let all_pms = Arc::new(RwLock::new(vec![]));
        // Spawn the first manager.
        let (out_msg_tx1, out_msg_rx1) = channel(1024);
        let (inc_msg_tx1, _) = channel(1024);
        let all_pms1 = all_pms.clone();
        let task = futures::lazy(move || {
            let pm = PeerManager::new(
                Duration::from_millis(5000),
                Duration::from_millis(5000),
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
            all_pms1.write().expect(POISONED_LOCK_ERR).push(pm);
            future::ok(())
        });
        thread::spawn(move || tokio::run(task));

        // Spawn the second manager and boot it from the first one.
        let (_, out_msg_rx2) = channel(1024);
        let (inc_msg_tx2, inc_msg_rx2) = channel(1024);
        let all_pms2 = all_pms.clone();
        let task = futures::lazy(move || {
            let pm = PeerManager::new(
                Duration::from_millis(5000),
                Duration::from_millis(5000),
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
            all_pms2.write().expect(POISONED_LOCK_ERR).push(pm);
            future::ok(())
        });
        thread::spawn(move || tokio::run(task));

        wait_all_peers_connected(50, 10000, &all_pms, 2);
        // Create task that sends the message and then places it into `acc`.
        let acc = Arc::new(RwLock::new(None));
        let acc1 = acc.clone();
        // Send message from `manager1` to `manager2`.
        let task = out_msg_tx1
            .send((hash_struct(&1), b"hello".to_vec()))
            .map(|_| ())
            .map_err(|e| panic!("Should not panic {}", e))
            .then(move |_| {
                // Wait for 1 sec to receive it on `manager2`.
                inc_msg_rx2
                    .timeout(Duration::from_millis(1000))
                    .into_future()
                    .map(move |(el, _)| {
                        *acc.write().expect(POISONED_LOCK_ERR) = Some(el);
                    })
                    .map_err(|_| panic!("Message was not received."))
            })
            .map(|_| ())
            .map_err(|_| panic!("Error verifying messages."));
        thread::spawn(move || tokio::run(task));

        wait(move || acc1.read().expect(POISONED_LOCK_ERR).is_some(), 50, 10000);
    }

    #[test]
    /// Connect many nodes to a single boot node, i.e. star. Check that all nodes manage to discover
    /// each other by sending a message from every node to every other node.
    fn test_many_star() {
        const NUM_TASKS: usize = 20;

        let (mut v_out_msg_tx, mut v_inc_msg_rx) = (vec![], vec![]);
        let all_pms = Arc::new(RwLock::new(vec![]));

        for i in 0..NUM_TASKS {
            let (out_msg_tx, out_msg_rx) = channel(1024);
            let (inc_msg_tx, inc_msg_rx) = channel(1024);
            v_out_msg_tx.push(out_msg_tx);
            v_inc_msg_rx.push(inc_msg_rx);
            let all_pms1 = all_pms.clone();
            let task = futures::lazy(move || {
                let mut boot_nodes = vec![];
                if i != 0 {
                    boot_nodes.push(PeerInfo {
                        id: hash_struct(&(0 as usize)),
                        addr: SocketAddr::from_str("127.0.0.1:3000").unwrap(),
                        account_id: None,
                    });
                }
                let pm = PeerManager::new(
                    Duration::from_millis(50),
                    Duration::from_millis(if i == 0 { 50 } else { 500000 }),
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
                all_pms1.write().expect(POISONED_LOCK_ERR).push(pm);
                Ok(())
            });
            thread::spawn(move || tokio::run(task));
        }

        let acc = Arc::new(RwLock::new(HashSet::new()));
        let acc1 = acc.clone();

        // Send messages from each peer to each other peer and check the receival.
        wait_all_peers_connected(50, 10000, &all_pms, NUM_TASKS);
        let task = futures::lazy(move || {
            for i in 0..NUM_TASKS {
                let mut messages = vec![];
                for j in 0..NUM_TASKS {
                    if j != i {
                        messages.push((hash_struct(&j), vec![i as u8, j as u8]));
                    }
                }
                let task = v_out_msg_tx[i]
                    .clone()
                    .send_all(iter_ok(messages.to_vec()))
                    .map(move |_| ())
                    .map_err(|_| panic!("Error sending messages"));
                tokio::spawn(task);

                let inc_msg_rx = v_inc_msg_rx.remove(0);
                let acc = acc.clone();
                let task = inc_msg_rx
                    .for_each(move |msg| {
                        let (id, data) = msg;
                        let sender = data[0] as usize;
                        let receiver = data[1] as usize;
                        if hash_struct(&sender) == id && receiver == i {
                            acc.write().expect(POISONED_LOCK_ERR).insert(data);
                        } else {
                            panic!("Should not happen");
                        }
                        future::ok(())
                    })
                    .map(|_| ())
                    .map_err(|_| ());
                tokio::spawn(task);
            }
            Ok(())
        });
        thread::spawn(move || tokio::run(task));

        wait(
            move || acc1.read().expect(POISONED_LOCK_ERR).len() == NUM_TASKS * (NUM_TASKS - 1),
            50,
            10000,
        );
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
