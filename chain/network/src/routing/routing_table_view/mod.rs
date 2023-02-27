use crate::network_protocol::PeerIdOrHash;
use crate::routing;
use crate::routing::route_back_cache::RouteBackCache;
use crate::store;
use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::time;
use near_primitives::types::AccountId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
mod tests;

const ANNOUNCE_ACCOUNT_CACHE_SIZE: usize = 10_000;
const LAST_ROUTED_CACHE_SIZE: usize = 10_000;

pub(crate) struct RoutingTableView(Mutex<Inner>);

struct Inner {
    /// Maps an account_id to a peer owning it.
    account_peers: LruCache<AccountId, AnnounceAccount>,
    /// Subset of account_peers, which we have broadcasted to the peers.
    /// It is used to skip rebroadcasting the same data multiple times.
    /// It contains less entries than account_peers in case some AnnounceAccounts
    /// have been loaded from storage without broadcasting.
    account_peers_broadcasted: LruCache<AccountId, AnnounceAccount>,

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

    /// Get AnnounceAccount for the given AccountId.
    fn get_announce(&mut self, account_id: &AccountId) -> Option<AnnounceAccount> {
        if let Some(aa) = self.account_peers.get(account_id) {
            return Some(aa.clone());
        }
        match self.store.get_account_announcement(&account_id) {
            Err(e) => {
                tracing::warn!(target: "network", "Error loading announce account from store: {:?}", e);
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
    RouteBackNotFound,
}

impl RoutingTableView {
    pub fn new(store: store::Store) -> Self {
        Self(Mutex::new(Inner {
            account_peers: LruCache::new(ANNOUNCE_ACCOUNT_CACHE_SIZE),
            account_peers_broadcasted: LruCache::new(ANNOUNCE_ACCOUNT_CACHE_SIZE),
            next_hops: Default::default(),
            route_back: RouteBackCache::default(),
            store,
            find_route_calls: 0,
            last_routed: LruCache::new(LAST_ROUTED_CACHE_SIZE),
        }))
    }

    pub(crate) fn update(&self, next_hops: Arc<routing::NextHopTable>) {
        self.0.lock().next_hops = next_hops;
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

    pub(crate) fn view_route(&self, peer_id: &PeerId) -> Option<Vec<PeerId>> {
        self.0.lock().next_hops.get(peer_id).cloned()
    }

    /// Find peer that owns this AccountId.
    pub(crate) fn account_owner(&self, account_id: &AccountId) -> Option<PeerId> {
        self.0.lock().get_announce(account_id).map(|announce_account| announce_account.peer_id)
    }

    /// Adds accounts to the routing table.
    /// Returns the diff: new values that should be broadcasted.
    /// Note: There is at most one peer id per account id.
    pub(crate) fn add_accounts(&self, aas: Vec<AnnounceAccount>) -> Vec<AnnounceAccount> {
        let mut inner = self.0.lock();
        let mut res = vec![];
        for aa in aas {
            // We skip broadcasting stuff that is already broadcasted.
            if inner.account_peers_broadcasted.get(&aa.account_id).map(|x| &x.epoch_id)
                == Some(&aa.epoch_id)
            {
                continue;
            }
            inner.account_peers.put(aa.account_id.clone(), aa.clone());
            inner.account_peers_broadcasted.put(aa.account_id.clone(), aa.clone());
            // Add account to store. Best effort
            if let Err(e) = inner.store.set_account_announcement(&aa.account_id, &aa) {
                tracing::warn!(target: "network", "Error saving announce account to store: {:?}", e);
            }
            res.push(aa);
        }
        res
    }

    pub(crate) fn add_route_back(&self, clock: &time::Clock, hash: CryptoHash, peer_id: PeerId) {
        self.0.lock().route_back.insert(clock, hash, peer_id);
    }

    pub(crate) fn compare_route_back(&self, hash: CryptoHash, peer_id: &PeerId) -> bool {
        self.0.lock().route_back.get(&hash).map_or(false, |value| value == peer_id)
    }

    pub(crate) fn info(&self) -> RoutingTableInfo {
        let inner = self.0.lock();
        let account_peers =
            inner.account_peers.iter().map(|(id, aa)| (id.clone(), aa.peer_id.clone())).collect();
        RoutingTableInfo { account_peers, next_hops: inner.next_hops.clone() }
    }

    /// Public interface for `account_peers`.
    /// Get keys currently on cache.
    pub(crate) fn get_accounts_keys(&self) -> Vec<AccountId> {
        self.0.lock().account_peers.iter().map(|(k, _v)| k).cloned().collect()
    }

    /// Get announce accounts on cache.
    pub(crate) fn get_announce_accounts(&self) -> Vec<AnnounceAccount> {
        self.0.lock().account_peers.iter().map(|(_, v)| v.clone()).collect()
    }

    /// Get AnnounceAccount for the given AccountIds, that we already broadcasted.
    pub(crate) fn get_broadcasted_announces<'a>(
        &'a self,
        account_ids: impl Iterator<Item = &'a AccountId>,
    ) -> HashMap<AccountId, AnnounceAccount> {
        let mut inner = self.0.lock();
        account_ids
            .filter_map(|id| {
                inner.account_peers_broadcasted.get(id).map(|a| (id.clone(), a.clone()))
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct RoutingTableInfo {
    pub account_peers: HashMap<AccountId, PeerId>,
    pub next_hops: Arc<routing::NextHopTable>,
}
