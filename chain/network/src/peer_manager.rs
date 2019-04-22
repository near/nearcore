//! Structure that encapsulates communication, gossip, and discovery with the peers.

use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use futures::future;
use futures::future::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc::Receiver;
use futures::sync::mpsc::Sender;
use log::warn;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use tokio::net::TcpListener;
use tokio::timer::Interval;

use primitives::network::{PeerAddr, PeerInfo, PeerMessage};
use primitives::types::{AccountId, PeerId};

use crate::peer::{
    get_delay, get_peer_info, AllPeerStates, ChainStateRetriever, Peer, PeerState, CONNECT_TIMEOUT,
};

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub struct PeerManager<T> {
    pub all_peer_states: AllPeerStates,
    pub node_info: PeerInfo,
    phantom: PhantomData<T>,
}

impl<T: ChainStateRetriever> PeerManager<T> {
    /// Args:
    /// * `reconnect_delay`: How long should we wait before connecting a newly discovered peer or
    ///   reconnecting the old one;
    ///    TODO: Use smarter strategy to gossip peer info. More frequent while not synced, and decaying with time
    /// * `gossip_interval`: Frequency of gossiping the peers info;
    /// * `gossip_sample_size`: How many peers should we gossip info to;
    /// * `node_info`: Information about the current node;
    /// * `boot_nodes`: list of verified info about boot nodes from which we can join the network;
    /// * `inc_msg_tx`: where `PeerManager` should be sending incoming messages;
    /// * `out_msg_rx`: where from `PeerManager` should be getting outgoing messages.
    /// * `chain_state_retriever`: adapter to retrieve `ChainState`.
    pub fn new(
        reconnect_delay: Duration,
        gossip_interval: Duration,
        gossip_sample_size: usize,
        node_info: PeerInfo,
        boot_nodes: &[PeerAddr],
        inc_msg_tx: Sender<(PeerId, Vec<u8>)>,
        out_msg_rx: Receiver<(PeerId, Vec<u8>)>,
        chain_state_retriever: T,
    ) -> Self {
        let all_peer_states = Arc::new(RwLock::new(HashMap::new()));
        // Spawn peers that represent boot nodes.
        Peer::spawn_from_known(
            node_info.clone(),
            boot_nodes.iter().cloned().map(std::convert::Into::into).collect(),
            all_peer_states.clone(),
            &mut all_peer_states.write().expect(POISONED_LOCK_ERR),
            inc_msg_tx.clone(),
            reconnect_delay,
            // Connect to the boot nodes immediately.
            Instant::now(),
            chain_state_retriever.clone(),
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
        tokio_utils::spawn(task);

        // Spawn the task that gossips.
        let all_peer_states2 = all_peer_states.clone();
        let task = Interval::new_interval(gossip_interval)
            .for_each(move |_| {
                let guard = all_peer_states2.read().expect(POISONED_LOCK_ERR);
                let peers_info: Vec<_> = guard
                    .values()
                    .filter_map(|state| {
                        get_peer_info(state.write().expect(POISONED_LOCK_ERR).deref_mut()).cloned()
                    })
                    .collect();
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
                    tokio_utils::spawn(
                        ch.send(PeerMessage::InfoGossip(peers_info.clone()))
                            .map(|_| ())
                            .map_err(|_| warn!(target: "network", "Error gossiping peers info.")),
                    );
                }
                future::ok(())
            })
            .map(|_| ())
            .map_err(|e| warn!(target: "network", "Error gossiping peers info {}", e));
        tokio_utils::spawn(task);

        // Spawn the task that listens to incoming connections if address is specified.
        if node_info.addr.is_some() {
            let node_info1 = node_info.clone();
            let all_peer_states3 = all_peer_states.clone();
            let task = TcpListener::bind(&node_info.addr.unwrap())
                .expect("Cannot listen to the address")
                .incoming()
                .for_each(move |socket| {
                    Peer::spawn_incoming_conn(
                        node_info1.clone(),
                        socket,
                        all_peer_states3.clone(),
                        inc_msg_tx.clone(),
                        reconnect_delay,
                        chain_state_retriever.clone(),
                    );
                    future::ok(())
                })
                .map(|_| ())
                .map_err(
                    |e| warn!(target: "network", "Error processing incoming connection {}", e),
                );
            tokio_utils::spawn(task);
        }

        Self { all_peer_states, node_info, phantom: PhantomData }
    }

    /// Ban this peer due to malicious behavior.
    /// # Panics
    /// Panics if the peer is in `IncomingConnection` state.
    pub fn ban_peer(&self, peer_id: &PeerId, duration: Duration) {
        if let Some(state) = self.all_peer_states.write().expect(POISONED_LOCK_ERR).get_mut(peer_id)
        {
            let mut guard = state.write().expect(POISONED_LOCK_ERR);
            match guard.deref() {
                PeerState::Connected { info, .. }
                | PeerState::Connecting { info, .. }
                | PeerState::Ready { info, .. }
                | PeerState::Unconnected { info, .. } => {
                    *guard = PeerState::Unconnected {
                        info: info.clone(),
                        connect_timer: get_delay(CONNECT_TIMEOUT),
                        banned_until: Some(Instant::now() + duration),
                        evicted: false,
                    };
                }
                // Incoming connection is either already handled or shouldn't be banned
                // because the connection is not yet established.
                PeerState::IncomingConnection { .. } => {
                    unreachable!("ban_peer cannot be called on incoming connection")
                }
            }
        }
    }

    /// This should be called if peer has done something wrong.
    pub fn suspect_malicious(&self, _peer_id: &PeerId) {
        // TODO(??): add counter + banning (disconnect and don't connect again) flag here for given peers.
    }

    /// Get `peer_id` peer information.
    pub fn get_peer_info(&self, peer_id: &PeerId) -> Option<PeerInfo> {
        if let Some(state) = self.all_peer_states.read().expect(POISONED_LOCK_ERR).get(peer_id) {
            return get_peer_info(&state.read().expect(POISONED_LOCK_ERR)).cloned();
        }
        None
    }

    /// Get channel for the given `peer_id`, if the corresponding peer is `Ready`.
    pub fn get_peer_channel(&self, peer_id: &PeerId) -> Option<Sender<PeerMessage>> {
        if let Some(state) = self.all_peer_states.read().expect(POISONED_LOCK_ERR).get(peer_id) {
            if let PeerState::Ready { out_msg_tx, .. } =
                state.read().expect(POISONED_LOCK_ERR).deref()
            {
                return Some(out_msg_tx.clone());
            }
        }
        None
    }

    /// Get channel for the given `account_id`, if the corresponding peer is `Ready`.
    pub fn get_account_channel(&self, account_id: AccountId) -> Option<Sender<PeerMessage>> {
        self.all_peer_states.read().expect(POISONED_LOCK_ERR).iter().find_map(|(_, state)| {
            let state_guard = state.read().expect(POISONED_LOCK_ERR);
            if let Some(info) = get_peer_info(&state_guard) {
                if info.account_id.as_ref() == Some(&account_id) {
                    if let PeerState::Ready { out_msg_tx, .. } = state_guard.deref() {
                        return Some(out_msg_tx.clone());
                    }
                }
            }
            None
        })
    }

    /// Get channels of all peers that are `Ready`.
    #[allow(dead_code)]
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

    pub fn get_peer_stats(&self) -> (usize, usize) {
        let guard = self.all_peer_states.read().expect(POISONED_LOCK_ERR);
        let active_peers = guard
            .iter()
            .map(|(_, state)| {
                let state_guard = state.read().expect(POISONED_LOCK_ERR);
                if let PeerState::Ready { .. } = state_guard.deref() {
                    1
                } else {
                    0
                }
            })
            .count();
        let total_peers = guard.len();
        (active_peers, total_peers)
    }
}

impl<T> fmt::Debug for PeerManager<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let peers: Vec<AccountId> = self
            .all_peer_states
            .read()
            .expect(POISONED_LOCK_ERR)
            .iter()
            .filter_map(|(_, state)| {
                let state_guard = state.read().expect(POISONED_LOCK_ERR);
                if let Some(info) = get_peer_info(&state_guard) {
                    if let PeerState::Ready { .. } = state_guard.deref() {
                        return info.account_id.clone();
                    }
                }
                None
            })
            .collect();
        write!(f, "{:?}", peers)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::Duration;

    use futures::future;
    use futures::future::Future;
    use futures::sink::Sink;
    use futures::stream::{iter_ok, Stream};
    use futures::sync::mpsc::channel;
    use tokio::util::StreamExt;

    use primitives::hash::hash_struct;
    use primitives::network::{PeerAddr, PeerInfo};

    use crate::peer::PeerState;
    use crate::peer_manager::{PeerManager, POISONED_LOCK_ERR};
    use crate::protocol::PEER_BAN_PERIOD;
    use crate::testing_utils::{wait, wait_all_peers_connected, MockChainStateRetriever};

    #[test]
    fn test_two_peers_boot() {
        let all_pms = Arc::new(RwLock::new(vec![]));
        // Spawn the first manager.
        let (out_msg_tx1, out_msg_rx1) = channel(1024);
        let (inc_msg_tx1, _) = channel(1024);
        let all_pms1 = all_pms.clone();
        let chain_state_retriever = MockChainStateRetriever {};
        let task = futures::lazy(move || {
            let pm = PeerManager::new(
                Duration::from_millis(5000),
                Duration::from_millis(5000),
                1,
                PeerInfo {
                    id: hash_struct(&0),
                    addr: Some(SocketAddr::from_str("127.0.0.1:4000").unwrap()),
                    account_id: None,
                },
                &vec![],
                inc_msg_tx1,
                out_msg_rx1,
                chain_state_retriever,
            );
            all_pms1.write().expect(POISONED_LOCK_ERR).push(pm);
            future::ok(())
        });
        thread::spawn(move || tokio::run(task));

        // Spawn the second manager and boot it from the first one.
        let (_, out_msg_rx2) = channel(1024);
        let (inc_msg_tx2, inc_msg_rx2) = channel(1024);
        let all_pms2 = all_pms.clone();
        let chain_state_retriever = MockChainStateRetriever {};
        let task = futures::lazy(move || {
            let pm = PeerManager::new(
                Duration::from_millis(5000),
                Duration::from_millis(5000),
                1,
                PeerInfo {
                    id: hash_struct(&1),
                    addr: Some(SocketAddr::from_str("127.0.0.1:4001").unwrap()),
                    account_id: None,
                },
                &vec![PeerAddr {
                    id: hash_struct(&0),
                    addr: SocketAddr::from_str("127.0.0.1:4000").unwrap(),
                }],
                inc_msg_tx2,
                out_msg_rx2,
                chain_state_retriever,
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
            let chain_state_retriever = MockChainStateRetriever {};
            let task = futures::lazy(move || {
                let mut boot_nodes = vec![];
                if i != 0 {
                    boot_nodes.push(PeerAddr {
                        id: hash_struct(&(0 as usize)),
                        addr: SocketAddr::from_str("127.0.0.1:3000").unwrap(),
                    });
                }
                let pm = PeerManager::new(
                    Duration::from_millis(50),
                    Duration::from_millis(if i == 0 { 50 } else { 500000 }),
                    if i == 0 { NUM_TASKS - 1 } else { 1 },
                    PeerInfo {
                        id: hash_struct(&i),
                        addr: Some(SocketAddr::new("127.0.0.1".parse().unwrap(), 3000 + i as u16)),
                        account_id: None,
                    },
                    &boot_nodes,
                    inc_msg_tx,
                    out_msg_rx,
                    chain_state_retriever,
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
                tokio_utils::spawn(task);

                let inc_msg_rx = v_inc_msg_rx.remove(0);
                let acc = acc.clone();
                let task = inc_msg_rx
                    .for_each(move |msg| {
                        let (id, data) = msg;
                        // Skip peer connected messages.
                        if data.len() == 2 {
                            let sender = data[0] as usize;
                            let receiver = data[1] as usize;
                            if hash_struct(&sender) == id && receiver == i {
                                acc.write().expect(POISONED_LOCK_ERR).insert(data);
                            } else {
                                panic!("Should not happen");
                            }
                        }
                        future::ok(())
                    })
                    .map(|_| ())
                    .map_err(|_| ());
                tokio_utils::spawn(task);
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

    #[test]
    /// Two nodes connect and one bans the other. Check that the peer states are properly transitioned in
    /// both nodes. Then try to reconnect the banned peer to the boot node. Check that the peer is not connected.
    fn test_ban_peer() {
        let all_pms = Arc::new(RwLock::new(vec![]));
        // Spawn the first manager.
        let (_, out_msg_rx1) = channel(1024);
        let (inc_msg_tx1, _) = channel(1024);
        let all_pms1 = all_pms.clone();
        let chain_state_retriever = MockChainStateRetriever {};
        let task = futures::lazy(move || {
            let pm = PeerManager::new(
                Duration::from_millis(5000),
                Duration::from_millis(5000),
                1,
                PeerInfo {
                    id: hash_struct(&0),
                    addr: Some(SocketAddr::from_str("127.0.0.1:5000").unwrap()),
                    account_id: None,
                },
                &vec![],
                inc_msg_tx1,
                out_msg_rx1,
                chain_state_retriever,
            );
            all_pms1.write().expect(POISONED_LOCK_ERR).push(pm);
            future::ok(())
        });
        thread::spawn(move || tokio::run(task));

        let all_pms1 = all_pms.clone();
        wait(move || all_pms1.read().expect(POISONED_LOCK_ERR).len() == 1, 50, 60000);

        // Spawn the second manager and boot it from the first one.
        let (_, out_msg_rx2) = channel(1024);
        let (inc_msg_tx2, _) = channel(1024);
        let all_pms1 = all_pms.clone();
        let chain_state_retriever = MockChainStateRetriever {};
        let task = futures::lazy(move || {
            let pm = PeerManager::new(
                Duration::from_millis(5000),
                Duration::from_millis(5000),
                1,
                PeerInfo {
                    id: hash_struct(&1),
                    addr: Some(SocketAddr::from_str("127.0.0.1:5001").unwrap()),
                    account_id: None,
                },
                &vec![PeerAddr {
                    id: hash_struct(&0),
                    addr: SocketAddr::from_str("127.0.0.1:5000").unwrap(),
                }],
                inc_msg_tx2,
                out_msg_rx2,
                chain_state_retriever,
            );
            all_pms1.write().expect(POISONED_LOCK_ERR).push(pm);
            future::ok(())
        });
        thread::spawn(move || tokio::run(task));

        let all_pms1 = all_pms.clone();
        wait(move || all_pms1.read().expect(POISONED_LOCK_ERR).len() == 2, 50, 60000);

        wait_all_peers_connected(50, 60000, &all_pms, 2);
        let peer1_id = all_pms.read().expect(POISONED_LOCK_ERR)[0].node_info.id;
        let peer2_id = all_pms.read().expect(POISONED_LOCK_ERR)[1].node_info.id;
        all_pms.read().expect(POISONED_LOCK_ERR)[0].ban_peer(&peer2_id, PEER_BAN_PERIOD);
        let all_pms1 = all_pms.clone();
        wait(
            move || {
                if let Some(state) = all_pms1.read().expect(POISONED_LOCK_ERR)[0]
                    .all_peer_states
                    .read()
                    .expect(POISONED_LOCK_ERR)
                    .get(&peer2_id)
                {
                    match *state.read().expect(POISONED_LOCK_ERR) {
                        PeerState::Unconnected { banned_until, .. } => banned_until.is_some(),
                        _ => false,
                    }
                } else {
                    false
                }
            },
            50,
            60000,
        );

        let all_pms1 = all_pms.clone();
        wait(
            move || {
                if let Some(state) = all_pms1.read().expect(POISONED_LOCK_ERR)[1]
                    .all_peer_states
                    .read()
                    .expect(POISONED_LOCK_ERR)
                    .get(&peer1_id)
                {
                    match *state.read().expect(POISONED_LOCK_ERR) {
                        PeerState::Unconnected { .. } => true,
                        _ => false,
                    }
                } else {
                    false
                }
            },
            50,
            60000,
        );

        // Spawn the third peer manager with the same peer id as the second peer manager
        // to check that the banned peer cannot reconnect.
        let all_pms1 = all_pms.clone();
        let (_, out_msg_rx3) = channel(1024);
        let (inc_msg_tx3, _) = channel(1024);
        let task = futures::lazy(move || {
            let pm = PeerManager::new(
                Duration::from_millis(5000),
                Duration::from_millis(5000),
                1,
                PeerInfo {
                    id: hash_struct(&1),
                    addr: Some(SocketAddr::from_str("127.0.0.1:5002").unwrap()),
                    account_id: None,
                },
                &vec![PeerAddr {
                    id: hash_struct(&0),
                    addr: SocketAddr::from_str("127.0.0.1:5000").unwrap(),
                }],
                inc_msg_tx3,
                out_msg_rx3,
                chain_state_retriever,
            );
            all_pms1.write().expect(POISONED_LOCK_ERR).push(pm);
            future::ok(())
        });
        thread::spawn(move || tokio::run(task));

        // Wait until the peer manager is spawned.
        let all_pms1 = all_pms.clone();
        wait(move || all_pms1.read().expect(POISONED_LOCK_ERR).len() == 3, 50, 60000);

        let all_pms1 = all_pms.clone();
        wait(
            move || {
                if let Some(state) = all_pms1.read().expect(POISONED_LOCK_ERR)[2]
                    .all_peer_states
                    .read()
                    .expect(POISONED_LOCK_ERR)
                    .get(&peer1_id)
                {
                    match *state.read().expect(POISONED_LOCK_ERR) {
                        PeerState::Unconnected { .. } => true,
                        _ => false,
                    }
                } else {
                    false
                }
            },
            50,
            60000,
        );
    }
}
