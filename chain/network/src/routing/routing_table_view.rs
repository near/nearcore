use crate::routing::route_back_cache::RouteBackCache;
use crate::store;
use itertools::Itertools;
use lru::LruCache;
use near_network_primitives::time;
use near_network_primitives::types::{Edge, PeerIdOrHash};
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::AccountId;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

const ANNOUNCE_ACCOUNT_CACHE_SIZE: usize = 10_000;
const ROUND_ROBIN_MAX_NONCE_DIFFERENCE_ALLOWED: usize = 10;
const ROUND_ROBIN_NONCE_CACHE_SIZE: usize = 10_000;

pub struct RoutingTableView {
    /// PeerId associated for every known account id.
    account_peers: LruCache<AccountId, AnnounceAccount>,
    /// Active PeerId that are part of the shortest path to each PeerId.
    pub(crate) peer_forwarding: Arc<HashMap<PeerId, Vec<PeerId>>>,
    /// Store last update for known edges. This is limited to list of adjacent edges to `my_peer_id`.
    pub(crate) local_edges_info: HashMap<PeerId, Edge>,
    /// Hash of messages that requires routing back to respective previous hop.
    route_back: RouteBackCache,
    /// Access to store on disk
    store: store::Store,
    /// Number of times each active connection was used to route a message.
    /// If there are several options use route with minimum nonce.
    /// New routes are added with minimum nonce.
    route_nonce: LruCache<PeerId, usize>,
}

#[derive(Debug)]
pub(crate) enum FindRouteError {
    Disconnected,
    PeerNotFound,
    AccountNotFound,
    RouteBackNotFound,
}

impl RoutingTableView {
    pub fn new(store: store::Store) -> Self {
        // Find greater nonce on disk and set `component_nonce` to this value.

        Self {
            account_peers: LruCache::new(ANNOUNCE_ACCOUNT_CACHE_SIZE),
            peer_forwarding: Default::default(),
            local_edges_info: Default::default(),
            route_back: RouteBackCache::default(),
            store,
            route_nonce: LruCache::new(ROUND_ROBIN_NONCE_CACHE_SIZE),
        }
    }

    /// Checks whenever edge is newer than the one we already have.
    /// Works only for local edges.
    pub(crate) fn is_local_edge_newer(&self, other_peer: &PeerId, nonce: u64) -> bool {
        self.local_edges_info.get(other_peer).map_or(0, |x| x.nonce()) < nonce
    }

    /// Find peer that is connected to `source` and belong to the shortest path
    /// from `source` to `peer_id`.
    fn find_route_from_peer_id(&mut self, peer_id: &PeerId) -> Result<PeerId, FindRouteError> {
        if let Some(routes) = self.peer_forwarding.get(peer_id) {
            match (routes.iter())
                .map(|peer_id| {
                    (self.route_nonce.get(peer_id).cloned().unwrap_or_default(), peer_id)
                })
                .minmax()
                .into_option()
            {
                None => Err(FindRouteError::Disconnected),
                // Neighbor with minimum and maximum nonce respectively.
                Some(((min_v, next_hop), (max_v, _))) => {
                    // Strategy similar to Round Robin. Select node with least nonce and send it. Increase its
                    // nonce by one. Additionally if the difference between the highest nonce and the lowest
                    // nonce is greater than some threshold increase the lowest nonce to be at least
                    // max nonce - threshold.
                    self.route_nonce.put(
                        next_hop.clone(),
                        std::cmp::max(
                            min_v + 1,
                            max_v.saturating_sub(ROUND_ROBIN_MAX_NONCE_DIFFERENCE_ALLOWED),
                        ),
                    );

                    Ok(next_hop.clone())
                }
            }
        } else {
            Err(FindRouteError::PeerNotFound)
        }
    }

    pub(crate) fn find_route(
        &mut self,
        clock: &time::Clock,
        target: &PeerIdOrHash,
    ) -> Result<PeerId, FindRouteError> {
        match target {
            PeerIdOrHash::PeerId(peer_id) => self.find_route_from_peer_id(peer_id),
            PeerIdOrHash::Hash(hash) => {
                self.fetch_route_back(clock, *hash).ok_or(FindRouteError::RouteBackNotFound)
            }
        }
    }

    pub(crate) fn view_route(&self, peer_id: &PeerId) -> Option<&Vec<PeerId>> {
        self.peer_forwarding.get(peer_id)
    }

    /// Find peer that owns this AccountId.
    pub(crate) fn account_owner(
        &mut self,
        account_id: &AccountId,
    ) -> Result<PeerId, FindRouteError> {
        self.get_announce(account_id)
            .map(|announce_account| announce_account.peer_id)
            .ok_or(FindRouteError::AccountNotFound)
    }

    /// Add (account id, peer id) to routing table.
    /// Note: There is at most on peer id per account id.
    pub(crate) fn add_account(&mut self, announce_account: AnnounceAccount) {
        let account_id = announce_account.account_id.clone();
        self.account_peers.put(account_id.clone(), announce_account.clone());

        // Add account to store
        if let Err(e) = self.store.set_account_announcement(&account_id, &announce_account) {
            warn!(target: "network", "Error saving announce account to store: {:?}", e);
        }
    }

    // TODO(MarX, #1694): Allow one account id to be routed to several peer id.
    pub(crate) fn contains_account(&mut self, announce_account: &AnnounceAccount) -> bool {
        self.get_announce(&announce_account.account_id).map_or(false, |current_announce_account| {
            current_announce_account.epoch_id == announce_account.epoch_id
        })
    }

    pub fn remove_local_edges<'a>(&mut self, peers: impl Iterator<Item = &'a PeerId>) {
        for other_peer in peers {
            self.local_edges_info.remove(other_peer);
        }
    }

    pub(crate) fn add_route_back(
        &mut self,
        clock: &time::Clock,
        hash: CryptoHash,
        peer_id: PeerId,
    ) {
        self.route_back.insert(clock, hash, peer_id);
    }

    // Find route back with given hash and removes it from cache.
    fn fetch_route_back(&mut self, clock: &time::Clock, hash: CryptoHash) -> Option<PeerId> {
        self.route_back.remove(clock, &hash)
    }

    pub(crate) fn compare_route_back(&self, hash: CryptoHash, peer_id: &PeerId) -> bool {
        self.route_back.get(&hash).map_or(false, |value| value == peer_id)
    }

    pub(crate) fn info(&self) -> RoutingTableInfo {
        let account_peers = self
            .get_announce_accounts()
            .map(|announce_account| {
                (announce_account.account_id.clone(), announce_account.peer_id.clone())
            })
            .collect();
        RoutingTableInfo { account_peers, peer_forwarding: self.peer_forwarding.clone() }
    }

    /// Public interface for `account_peers`.
    /// Get keys currently on cache.
    pub(crate) fn get_accounts_keys(&self) -> impl Iterator<Item = &AccountId> + ExactSizeIterator {
        self.account_peers.iter().map(|(k, _v)| k)
    }

    /// Get announce accounts on cache.
    pub(crate) fn get_announce_accounts(
        &self,
    ) -> impl Iterator<Item = &AnnounceAccount> + ExactSizeIterator {
        self.account_peers.iter().map(|(_k, v)| v)
    }

    /// Get account announce from
    pub(crate) fn get_announce(&mut self, account_id: &AccountId) -> Option<AnnounceAccount> {
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

    pub(crate) fn get_local_edge(&self, other_peer: &PeerId) -> Option<&Edge> {
        self.local_edges_info.get(other_peer)
    }
}

#[derive(Debug)]
pub struct RoutingTableInfo {
    pub account_peers: HashMap<AccountId, PeerId>,
    pub peer_forwarding: Arc<HashMap<PeerId, Vec<PeerId>>>,
}
