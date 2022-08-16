use crate::routing;
use crate::routing::route_back_cache::RouteBackCache;
use crate::store;
use lru::LruCache;
use near_network_primitives::time;
use near_network_primitives::types::{Edge, PeerIdOrHash};
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::AccountId;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::Mutex;
use tracing::warn;

const ANNOUNCE_ACCOUNT_CACHE_SIZE: usize = 10_000;
const LAST_ROUTED_CACHE_SIZE: usize = 10_000;

pub(crate) struct RoutingTableView(Mutex<Inner>);

struct Inner {
    my_peer_id: PeerId,
    /// Store last update for known edges. This is limited to list of adjacent edges to `my_peer_id`.
    local_edges_info: HashMap<PeerId, Edge>,

    /// Maps an account_id to a peer owning it.
    account_peers: LruCache<AccountId, AnnounceAccount>,
    /// For each peer, the set of neighbors which are one hop closer to `my_peer_id`.
    /// Alternatively, if we look at the set of all shortest path from `my_peer_id` to peer,
    /// this will be the set of first nodes on all such paths.
    next_hops: Arc<routing::NextHopTable>,
    /// Hash of messages that requires routing back to respective previous hop.
    route_back: RouteBackCache,
    /// Access to store on disk
    store: store::Store,

    /// Counter of number of calls to find_route_by_peer_id.
    find_route_calls: u64,
    /// Last time the given peer was selected by find_route_by_peer_id.
    last_routed: LruCache<PeerId, u64>,
}

impl Inner {
    /// Get announce accounts on cache.
    fn get_announce_accounts(
        &self,
    ) -> impl Iterator<Item = &AnnounceAccount> + ExactSizeIterator {
        self.account_peers.iter().map(|(_k, v)| v)
    }

    /// Select a connected peer on some shortest path to `peer_id`.
    /// If there are several such peers, pick the least recently used one.
    fn find_route_from_peer_id(&mut self, peer_id: &PeerId) -> Result<PeerId, FindRouteError> {
        let peers = self.next_hops.get(peer_id).ok_or(FindRouteError::PeerUnreachable)?;
        let next_hop = peers
            .iter()
            .min_by_key(|p| self.last_routed.get(*p).copied().unwrap_or(0))
            .ok_or(FindRouteError::PeerUnreachable)?;
        self.last_routed.put(next_hop.clone(), self.find_route_calls);
        self.find_route_calls += 1;
        Ok(next_hop.clone())
    }

    // Find route back with given hash and removes it from cache.
    fn fetch_route_back(&mut self, clock: &time::Clock, hash: CryptoHash) -> Option<PeerId> {
        self.route_back.remove(clock, &hash)
    }

    /// Checks whenever edge is newer than the one we already have.
    /// Works only for local edges.
    fn is_local_edge_newer(&self, other_peer: &PeerId, nonce: u64) -> bool {
        self.local_edges_info.get(other_peer).map_or(0, |x| x.nonce()) < nonce
    }

    /// Get AnnounceAccount for the given AccountId.
    fn get_announce(&mut self, account_id: &AccountId) -> Option<AnnounceAccount> {
        if let Some(announce_account) = self.account_peers.get(account_id) {
            return Some(announce_account.clone());
        }
        match self.store.get_account_announcement(&account_id) {
            Err(e) => {
                warn!(target: "network", "Error loading announce account from store: {:?}", e);
                None
            }
            Ok(None) => None,
            Ok(Some(a)) => {
                self.account_peers.put(account_id.clone(), a.clone());
                Some(a)
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum FindRouteError {
    PeerUnreachable,
    AccountNotFound,
    RouteBackNotFound,
}

impl RoutingTableView {
    pub fn new(store: store::Store, my_peer_id: PeerId) -> Self {
        Self(Mutex::new(Inner{
            my_peer_id,
            account_peers: LruCache::new(ANNOUNCE_ACCOUNT_CACHE_SIZE),
            next_hops: Default::default(),
            local_edges_info: Default::default(),
            route_back: RouteBackCache::default(),
            store,
            find_route_calls: 0,
            last_routed: LruCache::new(LAST_ROUTED_CACHE_SIZE),
        }))
    }

    /// Checks whenever edge is newer than the one we already have.
    /// Works only for local edges.
    pub(crate) fn is_local_edge_newer(&self, other_peer: &PeerId, nonce: u64) -> bool {
        self.0.lock().is_local_edge_newer(other_peer,nonce)
    } 

    pub(crate) fn set_next_hops(&mut self, routing_table: Arc<routing::NextHopTable>) {
        self.0.lock().next_hops = routing_table;
    }

    pub(crate) fn reachable_peers(&self) -> usize {
        // There is an implicit assumption here that all next_hops entries are non-empty.
        // To enforce this, we would need to make NextHopTable a newtype rather than an alias,
        // and add appropriate constructors, which would filter out empty entries.
        self.0.lock().next_hops.len()
    }

    pub(crate) fn find_route(
        &self,
        clock: &time::Clock,
        target: &PeerIdOrHash,
    ) -> Result<PeerId, FindRouteError> {
        let mut inner = self.0.lock();
        match target {
            PeerIdOrHash::PeerId(peer_id) => inner.find_route_from_peer_id(peer_id),
            PeerIdOrHash::Hash(hash) => {
                inner.fetch_route_back(clock, *hash).ok_or(FindRouteError::RouteBackNotFound)
            }
        }
    }

    pub(crate) fn view_route(&self, peer_id: &PeerId) -> Option<&Vec<PeerId>> {
        self.0.lock().next_hops.get(peer_id)
    }

    /// Find peer that owns this AccountId.
    pub(crate) fn account_owner(
        &self,
        account_id: &AccountId,
    ) -> Result<PeerId, FindRouteError> {
        self.0.lock().get_announce(account_id)
            .map(|announce_account| announce_account.peer_id)
            .ok_or(FindRouteError::AccountNotFound)
    }

    /// Add (account id, peer id) to routing table.
    /// Note: There is at most one peer id per account id.
    pub(crate) fn add_account(&self, announce_account: AnnounceAccount) {
        let mut inner = self.0.lock();
        let account_id = announce_account.account_id.clone();
        inner.account_peers.put(account_id.clone(), announce_account.clone());
        // Add account to store
        if let Err(e) = inner.store.set_account_announcement(&account_id, &announce_account) {
            warn!(target: "network", "Error saving announce account to store: {:?}", e);
        }
    }

    // TODO(MarX, #1694): Allow one account id to be routed to several peer id.
    pub(crate) fn contains_account(&self, announce_account: &AnnounceAccount) -> bool {
        self.0.lock().get_announce(&announce_account.account_id).map_or(false, |current_announce_account| {
            current_announce_account.epoch_id == announce_account.epoch_id
        })
    }

    pub(crate) fn add_route_back(
        &self,
        clock: &time::Clock,
        hash: CryptoHash,
        peer_id: PeerId,
    ) {
        self.0.lock().route_back.insert(clock, hash, peer_id);
    }

    pub(crate) fn compare_route_back(&self, hash: CryptoHash, peer_id: &PeerId) -> bool {
        self.0.lock().route_back.get(&hash).map_or(false, |value| value == peer_id)
    }

    pub(crate) fn info(&self) -> RoutingTableInfo {
        let inner = self.0.lock();
        let account_peers = inner
            .get_announce_accounts()
            .map(|announce_account| {
                (announce_account.account_id.clone(), announce_account.peer_id.clone())
            })
            .collect();
        RoutingTableInfo { account_peers, next_hops: inner.next_hops.clone() }
    }

    /// Public interface for `account_peers`.
    /// Get keys currently on cache.
    pub(crate) fn get_accounts_keys(&self) -> Vec<AccountId> {
        self.0.lock().account_peers.iter().map(|(k, _v)| k).cloned().collect()
    }

    /// Get announce accounts on cache.
    pub(crate) fn get_announce_accounts(
        &self,
    ) -> Vec<AnnounceAccount> {
        return self.0.lock().get_announce_accounts().cloned().collect()
    }

    /// Get AnnounceAccount for the given AccountId.
    pub(crate) fn get_announce(&self, account_id: &AccountId) -> Option<AnnounceAccount> {
        self.0.lock().get_announce(account_id)
    }

    pub(crate) fn get_local_edge(&self, other_peer: &PeerId) -> Option<&Edge> {
        self.0.lock().local_edges_info.get(other_peer)
    }

    pub(crate) fn add_local_edge(&self, edge: Edge) {
        let mut inner = self.0.lock();
        if let Some(other_peer) = edge.other(&inner.my_peer_id) {
            if !inner.is_local_edge_newer(other_peer, edge.nonce()) {
                return;
            }
            inner.local_edges_info.insert(other_peer.clone(), edge);
        }
    }

    pub(crate) fn remove_local_edge(&mut self, peer_id: &PeerId) {
        self.0.lock().local_edges_info.remove(peer_id);
    }
}

#[derive(Debug)]
pub struct RoutingTableInfo {
    pub account_peers: HashMap<AccountId, PeerId>,
    pub next_hops: Arc<routing::NextHopTable>,
}
