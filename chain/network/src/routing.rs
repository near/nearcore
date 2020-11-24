use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::ops::Sub;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;

use borsh::{BorshDeserialize, BorshSerialize};
use byteorder::{LittleEndian, WriteBytesExt};
use cached::{Cached, SizedCache};
use chrono;
use log::{info, trace, warn};

use near_crypto::{SecretKey, Signature};
use near_metrics;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::AccountId;
use near_primitives::utils::index_to_bytes;
use near_store::{
    ColAccountAnnouncements, ColComponentEdges, ColLastComponentNonce, ColPeerComponent, Store,
    StoreUpdate,
};

use crate::metrics;
use crate::{
    cache::RouteBackCache,
    types::{PeerIdOrHash, Ping, Pong},
    utils::cache_to_hashmap,
};
#[cfg(feature = "delay_detector")]
use delay_detector::DelayDetector;

const ANNOUNCE_ACCOUNT_CACHE_SIZE: usize = 10_000;
const ROUTE_BACK_CACHE_SIZE: u64 = 100_000;
const ROUTE_BACK_CACHE_EVICT_TIMEOUT: u64 = 120_000; // 120 seconds
const ROUTE_BACK_CACHE_REMOVE_BATCH: u64 = 100;
const PING_PONG_CACHE_SIZE: usize = 1_000;
const ROUND_ROBIN_MAX_NONCE_DIFFERENCE_ALLOWED: usize = 10;
const ROUND_ROBIN_NONCE_CACHE_SIZE: usize = 10_000;
/// Routing table will clean edges if there is at least one node that is not reachable
/// since `SAVE_PEERS_MAX_TIME` seconds. All peers disconnected since `SAVE_PEERS_AFTER_TIME`
/// seconds will be removed from cache and persisted in disk.
pub const SAVE_PEERS_MAX_TIME: u64 = 7_200;
pub const SAVE_PEERS_AFTER_TIME: u64 = 3_600;

/// Information that will be ultimately used to create a new edge.
/// It contains nonce proposed for the edge with signature from peer.
#[derive(Clone, BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Debug, Default)]
pub struct EdgeInfo {
    pub nonce: u64,
    pub signature: Signature,
}

impl EdgeInfo {
    pub fn new(peer0: PeerId, peer1: PeerId, nonce: u64, secret_key: &SecretKey) -> Self {
        let (peer0, peer1) = Edge::key(peer0, peer1);
        let data = Edge::build_hash(&peer0, &peer1, nonce);
        let signature = secret_key.sign(data.as_ref());
        Self { nonce, signature }
    }
}

/// Status of the edge
#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, PartialEq, Eq, Debug, Hash)]
pub enum EdgeType {
    Added,
    Removed,
}

/// Edge object. Contains information relative to a new edge that is being added or removed
/// from the network. This is the information that is required
#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct Edge {
    /// Since edges are not directed `peer0 < peer1` should hold.
    pub peer0: PeerId,
    pub peer1: PeerId,
    /// Nonce to keep tracking of the last update on this edge.
    /// It must be even
    pub nonce: u64,
    /// Signature from parties validating the edge. These are signature of the added edge.
    signature0: Signature,
    signature1: Signature,
    /// Info necessary to declare an edge as removed.
    /// The bool says which party is removing the edge: false for Peer0, true for Peer1
    /// The signature from the party removing the edge.
    removal_info: Option<(bool, Signature)>,
}

impl Edge {
    /// Create an addition edge.
    pub fn new(
        peer0: PeerId,
        peer1: PeerId,
        nonce: u64,
        signature0: Signature,
        signature1: Signature,
    ) -> Self {
        let (peer0, signature0, peer1, signature1) = if peer0 < peer1 {
            (peer0, signature0, peer1, signature1)
        } else {
            (peer1, signature1, peer0, signature0)
        };

        Self { peer0, peer1, nonce, signature0, signature1, removal_info: None }
    }

    /// Build a new edge with given information from the other party.
    pub fn build_with_secret_key(
        peer0: PeerId,
        peer1: PeerId,
        nonce: u64,
        secret_key: &SecretKey,
        signature1: Signature,
    ) -> Self {
        let hash = if peer0 < peer1 {
            Edge::build_hash(&peer0, &peer1, nonce)
        } else {
            Edge::build_hash(&peer1, &peer0, nonce)
        };
        let signature0 = secret_key.sign(hash.as_ref());
        Edge::new(peer0, peer1, nonce, signature0, signature1)
    }

    /// Create the remove edge change from an added edge change.
    pub fn remove_edge(&self, me: PeerId, sk: &SecretKey) -> Self {
        assert_eq!(self.edge_type(), EdgeType::Added);
        let mut edge = self.clone();
        edge.nonce += 1;
        let me = edge.peer0 == me;
        let hash = edge.hash();
        let signature = sk.sign(hash.as_ref());
        edge.removal_info = Some((me, signature));
        edge
    }

    /// Build the hash of the edge given its content.
    /// It is important that peer0 < peer1 at this point.
    fn build_hash(peer0: &PeerId, peer1: &PeerId, nonce: u64) -> CryptoHash {
        let mut buffer = Vec::<u8>::new();
        let peer0: Vec<u8> = peer0.clone().into();
        buffer.extend_from_slice(peer0.as_slice());
        let peer1: Vec<u8> = peer1.clone().into();
        buffer.extend_from_slice(peer1.as_slice());
        buffer.write_u64::<LittleEndian>(nonce).unwrap();
        hash(buffer.as_slice())
    }

    fn hash(&self) -> CryptoHash {
        Edge::build_hash(&self.peer0, &self.peer1, self.nonce)
    }

    fn prev_hash(&self) -> CryptoHash {
        Edge::build_hash(&self.peer0, &self.peer1, self.nonce - 1)
    }

    pub fn verify(&self) -> bool {
        if self.peer0 > self.peer1 {
            return false;
        }

        match self.edge_type() {
            EdgeType::Added => {
                let data = self.hash();

                self.removal_info.is_none()
                    && self.signature0.verify(data.as_ref(), &self.peer0.public_key())
                    && self.signature1.verify(data.as_ref(), &self.peer1.public_key())
            }
            EdgeType::Removed => {
                // nonce should be an even positive number
                if self.nonce == 0 {
                    return false;
                }

                // Check referring added edge is valid.
                let add_hash = self.prev_hash();
                if !self.signature0.verify(add_hash.as_ref(), &self.peer0.public_key())
                    || !self.signature1.verify(add_hash.as_ref(), &self.peer1.public_key())
                {
                    return false;
                }

                if let Some((party, signature)) = &self.removal_info {
                    let peer = if *party { &self.peer0 } else { &self.peer1 };
                    let del_hash = self.hash();
                    signature.verify(del_hash.as_ref(), &peer.public_key())
                } else {
                    false
                }
            }
        }
    }

    pub fn key(peer0: PeerId, peer1: PeerId) -> (PeerId, PeerId) {
        if peer0 < peer1 {
            (peer0, peer1)
        } else {
            (peer1, peer0)
        }
    }

    /// Helper function when adding a new edge and we receive information from new potential peer
    /// to verify the signature.
    pub fn partial_verify(peer0: PeerId, peer1: PeerId, edge_info: &EdgeInfo) -> bool {
        let pk = peer1.public_key();
        let (peer0, peer1) = Edge::key(peer0, peer1);
        let data = Edge::build_hash(&peer0, &peer1, edge_info.nonce);
        edge_info.signature.verify(data.as_ref(), &pk)
    }

    fn get_pair(&self) -> (PeerId, PeerId) {
        (self.peer0.clone(), self.peer1.clone())
    }

    /// It will be considered as a new edge if the nonce is odd, otherwise it is canceling the
    /// previous edge.
    pub fn edge_type(&self) -> EdgeType {
        if self.nonce % 2 == 1 {
            EdgeType::Added
        } else {
            EdgeType::Removed
        }
    }

    /// Next nonce of valid addition edge.
    pub fn next_nonce(nonce: u64) -> u64 {
        if nonce % 2 == 1 {
            nonce + 2
        } else {
            nonce + 1
        }
    }

    /// Next nonce of valid addition edge.
    pub fn next(&self) -> u64 {
        Edge::next_nonce(self.nonce)
    }

    pub fn contains_peer(&self, peer_id: &PeerId) -> bool {
        self.peer0 == *peer_id || self.peer1 == *peer_id
    }

    /// Find a peer id in this edge different from `me`.
    pub fn other(&self, me: &PeerId) -> Option<PeerId> {
        if self.peer0 == *me {
            Some(self.peer1.clone())
        } else if self.peer1 == *me {
            Some(self.peer0.clone())
        } else {
            None
        }
    }
}

pub struct RoutingTable {
    /// PeerId associated for every known account id.
    account_peers: SizedCache<AccountId, AnnounceAccount>,
    /// Active PeerId that are part of the shortest path to each PeerId.
    pub peer_forwarding: HashMap<PeerId, HashSet<PeerId>>,
    /// Store last update for known edges.
    pub edges_info: HashMap<(PeerId, PeerId), Edge>,
    /// Hash of messages that requires routing back to respective previous hop.
    pub route_back: RouteBackCache,
    /// Last time a peer with reachable through active edges.
    pub peer_last_time_reachable: HashMap<PeerId, chrono::DateTime<chrono::Utc>>,
    /// Access to store on disk
    store: Arc<Store>,
    /// Current view of the network. Nodes are Peers and edges are active connections.
    pub raw_graph: Graph,
    /// Number of times each active connection was used to route a message.
    /// If there are several options use route with minimum nonce.
    /// New routes are added with minimum nonce.
    route_nonce: SizedCache<PeerId, usize>,
    /// Flag to know if there is state recalculation pending.
    recalculation_scheduled: Option<Instant>,
    /// Ping received by nonce.
    ping_info: SizedCache<usize, Ping>,
    /// Ping received by nonce.
    pong_info: SizedCache<usize, Pong>,
    /// List of pings sent for which we haven't received any pong yet.
    waiting_pong: SizedCache<PeerId, SizedCache<usize, Instant>>,
    /// Last nonce sent to each peer through pings.
    last_ping_nonce: SizedCache<PeerId, usize>,
    /// Last nonce used to store edges on disk.
    pub component_nonce: u64,
}

#[derive(Debug)]
pub enum FindRouteError {
    Disconnected,
    PeerNotFound,
    AccountNotFound,
    RouteBackNotFound,
}

impl RoutingTable {
    pub fn new(peer_id: PeerId, store: Arc<Store>) -> Self {
        // Find greater nonce on disk and set `component_nonce` to this value.
        let component_nonce = store
            .get_ser::<u64>(ColLastComponentNonce, &[])
            .unwrap_or(None)
            .map_or(0, |nonce| nonce + 1);

        Self {
            account_peers: SizedCache::with_size(ANNOUNCE_ACCOUNT_CACHE_SIZE),
            peer_forwarding: HashMap::new(),
            edges_info: HashMap::new(),
            route_back: RouteBackCache::new(
                ROUTE_BACK_CACHE_SIZE,
                ROUTE_BACK_CACHE_EVICT_TIMEOUT,
                ROUTE_BACK_CACHE_REMOVE_BATCH,
            ),
            peer_last_time_reachable: HashMap::new(),
            store,
            raw_graph: Graph::new(peer_id),
            route_nonce: SizedCache::with_size(ROUND_ROBIN_NONCE_CACHE_SIZE),
            recalculation_scheduled: None,
            ping_info: SizedCache::with_size(PING_PONG_CACHE_SIZE),
            pong_info: SizedCache::with_size(PING_PONG_CACHE_SIZE),
            waiting_pong: SizedCache::with_size(PING_PONG_CACHE_SIZE),
            last_ping_nonce: SizedCache::with_size(PING_PONG_CACHE_SIZE),
            component_nonce,
        }
    }

    fn peer_id(&self) -> &PeerId {
        &self.raw_graph.source
    }

    pub fn reachable_peers(&self) -> impl Iterator<Item = &PeerId> {
        self.peer_forwarding.keys()
    }

    /// Find peer that is connected to `source` and belong to the shortest path
    /// from `source` to `peer_id`.
    pub fn find_route_from_peer_id(&mut self, peer_id: &PeerId) -> Result<PeerId, FindRouteError> {
        if let Some(routes) = self.peer_forwarding.get(&peer_id).cloned() {
            if routes.is_empty() {
                return Err(FindRouteError::Disconnected);
            }

            // Strategy similar to Round Robin. Select node with least nonce and send it. Increase its
            // nonce by one. Additionally if the difference between the highest nonce and the lowest
            // nonce is greater than some threshold increase the lowest nonce to be at least
            // max nonce - threshold.
            let nonce_peer = routes
                .iter()
                .map(|peer_id| {
                    (self.route_nonce.cache_get(&peer_id).cloned().unwrap_or(0), peer_id)
                })
                .collect::<Vec<_>>();

            // Neighbor with minimum and maximum nonce respectively.
            let min_v = nonce_peer.iter().min().cloned().unwrap();
            let max_v = nonce_peer.into_iter().max().unwrap();

            if min_v.0 + ROUND_ROBIN_MAX_NONCE_DIFFERENCE_ALLOWED < max_v.0 {
                self.route_nonce
                    .cache_set(min_v.1.clone(), max_v.0 - ROUND_ROBIN_MAX_NONCE_DIFFERENCE_ALLOWED);
            }

            let next_hop = min_v.1;
            let nonce = self.route_nonce.cache_get(&next_hop).cloned();
            self.route_nonce.cache_set(next_hop.clone(), nonce.map_or(1, |nonce| nonce + 1));
            Ok(next_hop.clone())
        } else {
            Err(FindRouteError::PeerNotFound)
        }
    }

    pub fn find_route(&mut self, target: &PeerIdOrHash) -> Result<PeerId, FindRouteError> {
        match target {
            PeerIdOrHash::PeerId(peer_id) => self.find_route_from_peer_id(&peer_id),
            PeerIdOrHash::Hash(hash) => {
                self.fetch_route_back(hash.clone()).ok_or(FindRouteError::RouteBackNotFound)
            }
        }
    }

    /// Find peer that owns this AccountId.
    pub fn account_owner(&mut self, account_id: &AccountId) -> Result<PeerId, FindRouteError> {
        self.get_announce(account_id)
            .map(|announce_account| announce_account.peer_id)
            .ok_or_else(|| FindRouteError::AccountNotFound)
    }

    /// Add (account id, peer id) to routing table.
    /// Note: There is at most on peer id per account id.
    pub fn add_account(&mut self, announce_account: AnnounceAccount) {
        let account_id = announce_account.account_id.clone();
        self.account_peers.cache_set(account_id.clone(), announce_account.clone());

        // Add account to store
        let mut update = self.store.store_update();
        if let Err(e) = update
            .set_ser(ColAccountAnnouncements, account_id.as_bytes(), &announce_account)
            .and_then(|_| update.commit())
        {
            warn!(target: "network", "Error saving announce account to store: {:?}", e);
        }
    }

    // TODO(MarX, #1694): Allow one account id to be routed to several peer id.
    pub fn contains_account(&mut self, announce_account: &AnnounceAccount) -> bool {
        self.get_announce(&announce_account.account_id).map_or(false, |current_announce_account| {
            current_announce_account.epoch_id == announce_account.epoch_id
        })
    }

    /// Get the nonce of the component where the peer was stored
    fn component_nonce_from_peer(&mut self, peer_id: PeerId) -> Result<u64, ()> {
        match self.store.get_ser::<u64>(ColPeerComponent, Vec::from(peer_id).as_ref()) {
            Ok(Some(nonce)) => Ok(nonce),
            _ => Err(()),
        }
    }

    /// Get all edges in the component with `nonce`
    /// Remove those edges from the store.
    fn get_component_edges(
        &mut self,
        nonce: u64,
        update: &mut StoreUpdate,
    ) -> Result<Vec<Edge>, ()> {
        let enc_nonce = index_to_bytes(nonce);

        let result = match self.store.get_ser::<Vec<Edge>>(ColComponentEdges, enc_nonce.as_ref()) {
            Ok(Some(edges)) => Ok(edges),
            _ => Err(()),
        };

        update.delete(ColComponentEdges, enc_nonce.as_ref());

        result
    }

    /// If peer_id is not on memory check if it is on disk in bring it back on memory.
    fn touch(&mut self, peer_id: &PeerId) {
        if peer_id == self.peer_id() || self.peer_last_time_reachable.contains_key(peer_id) {
            return;
        }

        let me = self.peer_id().clone();

        if let Ok(nonce) = self.component_nonce_from_peer(peer_id.clone()) {
            let mut update = self.store.store_update();

            if let Ok(edges) = self.get_component_edges(nonce, &mut update) {
                for edge in edges {
                    for &peer_id in vec![&edge.peer0, &edge.peer1].iter() {
                        if peer_id == &me || self.peer_last_time_reachable.contains_key(peer_id) {
                            continue;
                        }

                        if let Ok(cur_nonce) = self.component_nonce_from_peer(peer_id.clone()) {
                            if cur_nonce == nonce {
                                self.peer_last_time_reachable.insert(
                                    peer_id.clone(),
                                    chrono::Utc::now()
                                        .sub(chrono::Duration::seconds(SAVE_PEERS_MAX_TIME as i64)),
                                );
                                update
                                    .delete(ColPeerComponent, Vec::from(peer_id.clone()).as_ref());
                            }
                        }
                    }
                    self.add_edge(edge);
                }
            }

            if let Err(e) = update.commit() {
                warn!(target: "network", "Error removing network component from store. {:?}", e);
            }
        } else {
            self.peer_last_time_reachable.insert(peer_id.clone(), chrono::Utc::now());
        }
    }

    fn add_edge(&mut self, edge: Edge) -> bool {
        let key = edge.get_pair();

        if self.find_nonce(&key) >= edge.nonce {
            // We already have a newer information about this edge. Discard this information.
            false
        } else {
            match edge.edge_type() {
                EdgeType::Added => {
                    self.raw_graph.add_edge(key.0.clone(), key.1.clone());
                }
                EdgeType::Removed => {
                    self.raw_graph.remove_edge(&key.0, &key.1);
                }
            }
            self.edges_info.insert(key, edge);
            true
        }
    }

    /// Add several edges to the current view of the network.
    /// These edges are assumed to be valid at this point.
    /// Return true if some of the edges contains new information to the network.
    pub fn process_edges(&mut self, edges: Vec<Edge>) -> ProcessEdgeResult {
        let mut new_edge = false;
        let total = edges.len();

        for edge in edges {
            let key = edge.get_pair();

            self.touch(&key.0);
            self.touch(&key.1);

            if self.add_edge(edge) {
                new_edge = true;
            }
        }

        let mut new_schedule = None;

        if new_edge {
            // Minimum between known routes and 1000
            let known_routes = std::cmp::min(self.peer_forwarding.len() as u64, 1000);

            new_schedule = self.recalculation_scheduled.map_or_else(
                move || Some(Duration::from_millis(known_routes)),
                |target| {
                    if Instant::now() > target {
                        Some(Duration::from_millis(known_routes))
                    } else {
                        None
                    }
                },
            );

            if let Some(duration) = new_schedule {
                self.recalculation_scheduled = Some(Instant::now() + duration);
            }
        }

        // Update metrics after edge update
        near_metrics::inc_counter_by(&metrics::EDGE_UPDATES, total as i64);
        near_metrics::set_gauge(&metrics::EDGE_ACTIVE, self.raw_graph.total_active_edges as i64);

        ProcessEdgeResult { new_edge, schedule_computation: new_schedule }
    }

    pub fn find_nonce(&self, edge: &(PeerId, PeerId)) -> u64 {
        self.edges_info.get(&edge).map_or(0, |x| x.nonce)
    }

    pub fn get_edge(&self, peer0: PeerId, peer1: PeerId) -> Option<Edge> {
        let key = Edge::key(peer0, peer1);
        self.edges_info.get(&key).cloned()
    }

    pub fn get_edges(&self) -> Vec<Edge> {
        self.edges_info.iter().map(|(_, edge)| edge.clone()).collect()
    }

    pub fn add_route_back(&mut self, hash: CryptoHash, peer_id: PeerId) {
        self.route_back.insert(hash, peer_id);
    }

    // Find route back with given hash and removes it from cache.
    fn fetch_route_back(&mut self, hash: CryptoHash) -> Option<PeerId> {
        self.route_back.remove(&hash)
    }

    pub fn compare_route_back(&mut self, hash: CryptoHash, peer_id: &PeerId) -> bool {
        self.route_back.get(&hash).map_or(false, |value| value == peer_id)
    }

    pub fn add_ping(&mut self, ping: Ping) {
        self.ping_info.cache_set(ping.nonce as usize, ping);
    }

    /// Return time of the round trip of ping + pong
    pub fn add_pong(&mut self, pong: Pong) -> Option<f64> {
        let mut res = None;

        if let Some(nonces) = self.waiting_pong.cache_get_mut(&pong.source) {
            res = nonces
                .cache_remove(&(pong.nonce as usize))
                .and_then(|sent| Some(Instant::now().duration_since(sent).as_secs_f64() * 1000f64));
        }

        self.pong_info.cache_set(pong.nonce as usize, pong);

        res
    }

    pub fn sending_ping(&mut self, nonce: usize, target: PeerId) {
        let entry = if let Some(entry) = self.waiting_pong.cache_get_mut(&target) {
            entry
        } else {
            self.waiting_pong.cache_set(target.clone(), SizedCache::with_size(10));
            self.waiting_pong.cache_get_mut(&target).unwrap()
        };

        entry.cache_set(nonce, Instant::now());
    }

    pub fn get_ping(&mut self, peer_id: PeerId) -> usize {
        if let Some(entry) = self.last_ping_nonce.cache_get_mut(&peer_id) {
            *entry += 1;
            *entry - 1
        } else {
            self.last_ping_nonce.cache_set(peer_id, 1);
            0
        }
    }

    pub fn fetch_ping_pong(&self) -> (HashMap<usize, Ping>, HashMap<usize, Pong>) {
        (cache_to_hashmap(&self.ping_info), cache_to_hashmap(&self.pong_info))
    }

    pub fn info(&mut self) -> RoutingTableInfo {
        let account_peers = self
            .get_announce_accounts()
            .into_iter()
            .map(|announce_account| (announce_account.account_id, announce_account.peer_id))
            .collect();
        RoutingTableInfo { account_peers, peer_forwarding: self.peer_forwarding.clone() }
    }

    fn try_save_edges(&mut self) {
        let now = chrono::Utc::now();
        let mut oldest_time = now;
        let to_save = self
            .peer_last_time_reachable
            .iter()
            .filter_map(|(peer_id, last_time)| {
                oldest_time = std::cmp::min(oldest_time, *last_time);
                if now.signed_duration_since(*last_time).num_seconds()
                    >= SAVE_PEERS_AFTER_TIME as i64
                {
                    Some(peer_id.clone())
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();

        // Save nodes on disk and remove from memory only if elapsed time from oldest peer
        // is greater than `SAVE_PEERS_MAX_TIME`
        if now.signed_duration_since(oldest_time).num_seconds() < SAVE_PEERS_MAX_TIME as i64 {
            return;
        }

        let component_nonce = self.component_nonce;
        self.component_nonce += 1;

        let mut update = self.store.store_update();
        let _ = update.set_ser(ColLastComponentNonce, &[], &component_nonce);

        for peer_id in to_save.iter() {
            let _ = update.set_ser(
                ColPeerComponent,
                Vec::from(peer_id.clone()).as_ref(),
                &component_nonce,
            );

            self.peer_last_time_reachable.remove(peer_id);
        }

        let component_nonce = index_to_bytes(component_nonce);
        let mut edges_in_component = vec![];

        self.edges_info.retain(|(peer0, peer1), edge| {
            if to_save.contains(peer0) || to_save.contains(peer1) {
                edges_in_component.push(edge.clone());
                false
            } else {
                true
            }
        });

        let _ = update.set_ser(ColComponentEdges, component_nonce.as_ref(), &edges_in_component);

        if let Err(e) = update.commit() {
            warn!(target: "network", "Error storing network component to store. {:?}", e);
        }
    }

    /// Recalculate routing table.
    pub fn update(&mut self) {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("routing table update".into());
        let _routing_table_recalculation =
            near_metrics::start_timer(&metrics::ROUTING_TABLE_RECALCULATION_HISTOGRAM);

        trace!(target: "network", "Update routing table.");
        self.recalculation_scheduled = None;

        self.peer_forwarding = self.raw_graph.calculate_distance();

        let now = chrono::Utc::now();
        for peer in self.peer_forwarding.keys() {
            self.peer_last_time_reachable.insert(peer.clone(), now);
        }

        self.try_save_edges();

        near_metrics::inc_counter_by(&metrics::ROUTING_TABLE_RECALCULATIONS, 1);
        near_metrics::set_gauge(&metrics::PEER_REACHABLE, self.peer_forwarding.len() as i64);
    }

    /// Public interface for `account_peers`
    ///
    /// Get keys currently on cache.
    pub fn get_accounts_keys(&mut self) -> Vec<AccountId> {
        self.account_peers.key_order().cloned().collect()
    }

    /// Get announce accounts on cache.
    pub fn get_announce_accounts(&mut self) -> Vec<AnnounceAccount> {
        self.account_peers.value_order().cloned().collect()
    }

    /// Get account announce from
    pub fn get_announce(&mut self, account_id: &AccountId) -> Option<AnnounceAccount> {
        if let Some(announce_account) = self.account_peers.cache_get(&account_id) {
            Some(announce_account.clone())
        } else {
            self.store
                .get_ser(ColAccountAnnouncements, account_id.as_bytes())
                .and_then(|res: Option<AnnounceAccount>| {
                    if let Some(announce_account) = res {
                        self.add_account(announce_account.clone());
                        Ok(Some(announce_account))
                    } else {
                        Ok(None)
                    }
                })
                .unwrap_or_else(|e| {
                    warn!(target: "network", "Error loading announce account from store: {:?}", e);
                    None
                })
        }
    }

    pub fn get_raw_graph(&self) -> &HashMap<PeerId, HashSet<PeerId>> {
        &self.raw_graph.adjacency
    }
}

pub struct ProcessEdgeResult {
    pub new_edge: bool,
    pub schedule_computation: Option<Duration>,
}

#[derive(Debug)]
pub struct RoutingTableInfo {
    pub account_peers: HashMap<AccountId, PeerId>,
    pub peer_forwarding: HashMap<PeerId, HashSet<PeerId>>,
}

#[derive(Clone)]
pub struct Graph {
    pub source: PeerId,
    adjacency: HashMap<PeerId, HashSet<PeerId>>,
    total_active_edges: u64,
}

impl Graph {
    pub fn new(source: PeerId) -> Self {
        Self { source, adjacency: HashMap::new(), total_active_edges: 0 }
    }

    pub fn log_diameter(&self) {
        let n = self.adjacency.len();

        if !self.adjacency.contains_key(&self.source) {
            info!(target: "routing", "Waiting to have reachable peers");
            return;
        }

        if n >= 5000 {
            info!(target: "routing", "Too many nodes {}", n);
            return;
        }

        let mut dist = vec![vec![n; n]; n];

        let order = self
            .adjacency
            .keys()
            .enumerate()
            .map(|(a, b)| (b.clone(), a))
            .collect::<HashMap<_, _>>();

        for i in 0..n {
            dist[i][i] = 0;
        }

        for (u, row) in self.adjacency.iter() {
            let u_ix = *order.get(u).unwrap();
            for v in row {
                if let Some(&v_ix) = order.get(v) {
                    dist[u_ix][v_ix] = 1;
                    dist[v_ix][u_ix] = 1;
                }
            }
        }

        for k in 0..n {
            for i in 0..n {
                for j in 0..n {
                    dist[i][j] = std::cmp::min(dist[i][j], dist[i][k] + dist[k][j]);
                }
            }
        }

        let source = *order.get(&self.source).unwrap();
        let mut diameter = 0;

        let mut reachable = 0;
        let mut all_dist = HashMap::new();
        let mut dist_from_me = HashMap::new();

        for i in 0..n {
            if dist[source][i] == n {
                continue;
            }

            reachable += 1;

            dist_from_me
                .entry(dist[source][i])
                .and_modify(|x| {
                    *x += 1;
                })
                .or_insert(1);

            for j in 0..i {
                if dist[source][j] == n {
                    continue;
                }

                diameter = std::cmp::max(diameter, dist[i][j]);
                all_dist
                    .entry(dist[i][j])
                    .and_modify(|x| {
                        *x += 1;
                    })
                    .or_insert(1);
            }
        }

        info!(target: "routing", "Diameter: {}", diameter);
        info!(target: "routing", "Reachable: {}", reachable);
        info!(target: "routing", "All pair of distances frequency: {:?}", all_dist);
        info!(target: "routing", "Distance from me frequency: {:?}", dist_from_me);
    }

    fn contains_edge(&mut self, peer0: &PeerId, peer1: &PeerId) -> bool {
        if let Some(adj) = self.adjacency.get(&peer0) {
            if adj.contains(&peer1) {
                return true;
            }
        }

        false
    }

    fn add_directed_edge(&mut self, peer0: PeerId, peer1: PeerId) {
        self.adjacency.entry(peer0).or_insert_with(HashSet::new).insert(peer1);
    }

    fn remove_directed_edge(&mut self, peer0: &PeerId, peer1: &PeerId) {
        self.adjacency.get_mut(&peer0).unwrap().remove(&peer1);
    }

    pub fn add_edge(&mut self, peer0: PeerId, peer1: PeerId) {
        if !self.contains_edge(&peer0, &peer1) {
            self.add_directed_edge(peer0.clone(), peer1.clone());
            self.add_directed_edge(peer1, peer0);
            self.total_active_edges += 1;
        }
    }

    pub fn remove_edge(&mut self, peer0: &PeerId, peer1: &PeerId) {
        if self.contains_edge(&peer0, &peer1) {
            self.remove_directed_edge(&peer0, &peer1);
            self.remove_directed_edge(&peer1, &peer0);
            self.total_active_edges -= 1;
        }
    }

    // TODO(MarX, #1363): This is too slow right now. (See benchmarks)
    /// Compute for every node `u` on the graph (other than `source`) which are the neighbors of
    /// `sources` which belong to the shortest path from `source` to `u`. Nodes that are
    /// not connected to `source` will not appear in the result.
    pub fn calculate_distance(&self) -> HashMap<PeerId, HashSet<PeerId>> {
        let mut queue = vec![];
        let mut distance = HashMap::new();
        // TODO(MarX, #1363): Represent routes more efficiently at least while calculating distances
        let mut routes: HashMap<PeerId, HashSet<PeerId>> = HashMap::new();

        distance.insert(&self.source, 0);

        // Add active connections
        if let Some(neighbors) = self.adjacency.get(&self.source) {
            for neighbor in neighbors {
                queue.push(neighbor);
                distance.insert(neighbor, 1);
                routes.insert(neighbor.clone(), vec![neighbor.clone()].drain(..).collect());
            }
        }

        let mut head = 0;

        while head < queue.len() {
            let cur_peer = queue[head];
            let cur_distance = *distance.get(cur_peer).unwrap();
            head += 1;

            if let Some(neighbors) = self.adjacency.get(&cur_peer) {
                for neighbor in neighbors {
                    if let Entry::Vacant(entry) = distance.entry(neighbor) {
                        queue.push(entry.key());
                        entry.insert(cur_distance + 1);
                        routes.insert(neighbor.clone(), HashSet::new());
                    }

                    // If this edge belong to a shortest path, all paths to
                    // the closer nodes are also valid for the current node.
                    if *distance.get(neighbor).unwrap() == cur_distance + 1 {
                        let adding_routes = routes.get(cur_peer).unwrap().clone();
                        let target_routes = routes.get_mut(neighbor).unwrap();

                        for route in adding_routes {
                            target_routes.insert(route.clone());
                        }
                    }
                }
            }
        }

        routes.into_iter().filter(|(_, hops)| !hops.is_empty()).collect()
    }
}

#[cfg(test)]
mod test {
    use crate::routing::Graph;
    use crate::test_utils::{expected_routing_tables, random_peer_id};

    #[test]
    fn graph_contains_edge() {
        let source = random_peer_id();

        let node0 = random_peer_id();
        let node1 = random_peer_id();

        let mut graph = Graph::new(source.clone());

        assert_eq!(graph.contains_edge(&source, &node0), false);
        assert_eq!(graph.contains_edge(&source, &node1), false);
        assert_eq!(graph.contains_edge(&node0, &node1), false);
        assert_eq!(graph.contains_edge(&node1, &node0), false);

        graph.add_edge(node0.clone(), node1.clone());

        assert_eq!(graph.contains_edge(&source, &node0), false);
        assert_eq!(graph.contains_edge(&source, &node1), false);
        assert_eq!(graph.contains_edge(&node0, &node1), true);
        assert_eq!(graph.contains_edge(&node1, &node0), true);

        graph.remove_edge(&node1, &node0);

        assert_eq!(graph.contains_edge(&node0, &node1), false);
        assert_eq!(graph.contains_edge(&node1, &node0), false);
    }

    #[test]
    fn graph_distance0() {
        let source = random_peer_id();
        let node0 = random_peer_id();

        let mut graph = Graph::new(source.clone());
        graph.add_edge(source.clone(), node0.clone());

        assert!(expected_routing_tables(
            graph.calculate_distance(),
            vec![(node0.clone(), vec![node0.clone()])],
        ));
    }

    #[test]
    fn graph_distance1() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..3).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        graph.add_edge(nodes[0].clone(), nodes[1].clone());
        graph.add_edge(nodes[2].clone(), nodes[1].clone());
        graph.add_edge(nodes[1].clone(), nodes[2].clone());

        assert!(expected_routing_tables(graph.calculate_distance(), vec![]));
    }

    #[test]
    fn graph_distance2() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..3).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        graph.add_edge(nodes[0].clone(), nodes[1].clone());
        graph.add_edge(nodes[2].clone(), nodes[1].clone());
        graph.add_edge(nodes[1].clone(), nodes[2].clone());
        graph.add_edge(source.clone(), nodes[0].clone());

        assert!(expected_routing_tables(
            graph.calculate_distance(),
            vec![
                (nodes[0].clone(), vec![nodes[0].clone()]),
                (nodes[1].clone(), vec![nodes[0].clone()]),
                (nodes[2].clone(), vec![nodes[0].clone()]),
            ],
        ));
    }

    #[test]
    fn graph_distance3() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..3).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        graph.add_edge(nodes[0].clone(), nodes[1].clone());
        graph.add_edge(nodes[2].clone(), nodes[1].clone());
        graph.add_edge(nodes[0].clone(), nodes[2].clone());
        graph.add_edge(source.clone(), nodes[0].clone());
        graph.add_edge(source.clone(), nodes[1].clone());

        assert!(expected_routing_tables(
            graph.calculate_distance(),
            vec![
                (nodes[0].clone(), vec![nodes[0].clone()]),
                (nodes[1].clone(), vec![nodes[1].clone()]),
                (nodes[2].clone(), vec![nodes[0].clone(), nodes[1].clone()]),
            ],
        ));
    }

    /// Test the following graph
    ///     0 - 3 - 6
    ///   /   x   x
    /// s - 1 - 4 - 7
    ///   \   x   x
    ///     2 - 5 - 8
    ///
    ///    9 - 10 (Dummy edge disconnected)
    ///
    /// There is a shortest path to nodes [3..9) going through 0, 1, and 2.
    #[test]
    fn graph_distance4() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..11).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        for i in 0..3 {
            graph.add_edge(source.clone(), nodes[i].clone());
        }

        for level in 0..2 {
            for i in 0..3 {
                for j in 0..3 {
                    graph.add_edge(nodes[level * 3 + i].clone(), nodes[level * 3 + 3 + j].clone());
                }
            }
        }

        // Dummy edge.
        graph.add_edge(nodes[9].clone(), nodes[10].clone());

        let mut next_hops: Vec<_> =
            (0..3).map(|i| (nodes[i].clone(), vec![nodes[i].clone()])).collect();
        let target: Vec<_> = (0..3).map(|i| nodes[i].clone()).collect();

        for i in 3..9 {
            next_hops.push((nodes[i].clone(), target.clone()));
        }

        assert!(expected_routing_tables(graph.calculate_distance(), next_hops));
    }
}
