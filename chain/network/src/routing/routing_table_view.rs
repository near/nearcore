use crate::routing::route_back_cache::RouteBackCache;
use lru::LruCache;
use near_network_primitives::types::{Edge, PeerIdOrHash, Ping, Pong};
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::time::Clock;
use near_primitives::types::AccountId;
use near_store::{ColAccountAnnouncements, Store};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;

const ANNOUNCE_ACCOUNT_CACHE_SIZE: usize = 10_000;
const PING_PONG_CACHE_SIZE: usize = 1_000;
const ROUND_ROBIN_MAX_NONCE_DIFFERENCE_ALLOWED: usize = 10;
const ROUND_ROBIN_NONCE_CACHE_SIZE: usize = 10_000;
/// Routing table will clean edges if there is at least one node that is not reachable
/// since `SAVE_PEERS_MAX_TIME` seconds. All peers disconnected since `SAVE_PEERS_AFTER_TIME`
/// seconds will be removed from cache and persisted in disk.
pub const SAVE_PEERS_MAX_TIME: Duration = Duration::from_secs(7_200);
pub const DELETE_PEERS_AFTER_TIME: Duration = Duration::from_secs(3_600);

pub struct RoutingTableView {
    /// PeerId associated for every known account id.
    account_peers: LruCache<AccountId, AnnounceAccount>,
    /// Active PeerId that are part of the shortest path to each PeerId.
    pub peer_forwarding: Arc<HashMap<PeerId, Vec<PeerId>>>,
    /// Store last update for known edges. This is limited to list of adjacent edges to `my_peer_id`.
    pub local_edges_info: HashMap<PeerId, Edge>,
    /// Hash of messages that requires routing back to respective previous hop.
    pub route_back: RouteBackCache,
    /// Access to store on disk
    store: Store,
    /// Number of times each active connection was used to route a message.
    /// If there are several options use route with minimum nonce.
    /// New routes are added with minimum nonce.
    route_nonce: LruCache<PeerId, usize>,
    /// Ping received by nonce.
    ping_info: LruCache<usize, (Ping, usize)>,
    /// Ping received by nonce.
    pong_info: LruCache<usize, (Pong, usize)>,
    /// List of pings sent for which we haven't received any pong yet.
    waiting_pong: LruCache<PeerId, LruCache<usize, Instant>>,
    /// Last nonce sent to each peer through pings.
    last_ping_nonce: LruCache<PeerId, usize>,
}

#[derive(Debug)]
pub enum FindRouteError {
    Disconnected,
    PeerNotFound,
    AccountNotFound,
    RouteBackNotFound,
}

impl RoutingTableView {
    pub fn new(store: Store) -> Self {
        // Find greater nonce on disk and set `component_nonce` to this value.

        Self {
            account_peers: LruCache::new(ANNOUNCE_ACCOUNT_CACHE_SIZE),
            peer_forwarding: Default::default(),
            local_edges_info: Default::default(),
            route_back: RouteBackCache::default(),
            store,
            route_nonce: LruCache::new(ROUND_ROBIN_NONCE_CACHE_SIZE),
            ping_info: LruCache::new(PING_PONG_CACHE_SIZE),
            pong_info: LruCache::new(PING_PONG_CACHE_SIZE),
            waiting_pong: LruCache::new(PING_PONG_CACHE_SIZE),
            last_ping_nonce: LruCache::new(PING_PONG_CACHE_SIZE),
        }
    }

    /// Checks whenever edge is newer than the one we already have.
    /// Works only for local edges.
    pub fn is_local_edge_newer(&self, other_peer: &PeerId, nonce: u64) -> bool {
        self.local_edges_info.get(other_peer).map_or(0, |x| x.nonce()) < nonce
    }

    pub fn reachable_peers(&self) -> impl Iterator<Item = &PeerId> {
        self.peer_forwarding.keys()
    }

    /// Find peer that is connected to `source` and belong to the shortest path
    /// from `source` to `peer_id`.
    pub fn find_route_from_peer_id(&mut self, peer_id: &PeerId) -> Result<PeerId, FindRouteError> {
        if let Some(routes) = self.peer_forwarding.get(peer_id).cloned() {
            if routes.is_empty() {
                return Err(FindRouteError::Disconnected);
            }

            // Strategy similar to Round Robin. Select node with least nonce and send it. Increase its
            // nonce by one. Additionally if the difference between the highest nonce and the lowest
            // nonce is greater than some threshold increase the lowest nonce to be at least
            // max nonce - threshold.
            let nonce_peer = routes
                .iter()
                .map(|peer_id| (self.route_nonce.get(peer_id).cloned().unwrap_or(0), peer_id))
                .collect::<Vec<_>>();

            // Neighbor with minimum and maximum nonce respectively.
            let (mut min_v, next_hop) = nonce_peer.iter().min().cloned().unwrap();
            let (max_v, _) = nonce_peer.into_iter().max().unwrap();

            min_v = std::cmp::max(
                min_v,
                max_v.saturating_sub(ROUND_ROBIN_MAX_NONCE_DIFFERENCE_ALLOWED),
            );
            self.route_nonce.put(next_hop.clone(), min_v + 1);

            Ok(next_hop.clone())
        } else {
            Err(FindRouteError::PeerNotFound)
        }
    }

    pub fn find_route(&mut self, target: &PeerIdOrHash) -> Result<PeerId, FindRouteError> {
        match target {
            PeerIdOrHash::PeerId(peer_id) => self.find_route_from_peer_id(peer_id),
            PeerIdOrHash::Hash(hash) => {
                self.fetch_route_back(*hash).ok_or(FindRouteError::RouteBackNotFound)
            }
        }
    }

    /// Find peer that owns this AccountId.
    pub fn account_owner(&mut self, account_id: &AccountId) -> Result<PeerId, FindRouteError> {
        self.get_announce(account_id)
            .map(|announce_account| announce_account.peer_id)
            .ok_or(FindRouteError::AccountNotFound)
    }

    /// Add (account id, peer id) to routing table.
    /// Note: There is at most on peer id per account id.
    pub fn add_account(&mut self, announce_account: AnnounceAccount) {
        let account_id = announce_account.account_id.clone();
        self.account_peers.put(account_id.clone(), announce_account.clone());

        // Add account to store
        let mut update = self.store.store_update();
        if let Err(e) = update
            .set_ser(ColAccountAnnouncements, account_id.as_ref().as_bytes(), &announce_account)
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

    pub fn remove_local_edges<'a>(&mut self, peers: impl Iterator<Item = &'a PeerId>) {
        for other_peer in peers {
            self.local_edges_info.remove(other_peer);
        }
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
        let cnt = self.ping_info.get(&(ping.nonce as usize)).map(|v| v.1).unwrap_or(0);

        self.ping_info.put(ping.nonce as usize, (ping, cnt + 1));
    }

    /// Return time of the round trip of ping + pong
    pub fn add_pong(&mut self, pong: Pong) -> Option<f64> {
        let mut res = None;

        if let Some(nonces) = self.waiting_pong.get_mut(&pong.source) {
            res = nonces.pop(&(pong.nonce as usize)).map(|sent| {
                Clock::instant().saturating_duration_since(sent).as_secs_f64() * 1000f64
            });
        }

        let cnt = self.pong_info.get(&(pong.nonce as usize)).map(|v| v.1).unwrap_or(0);

        self.pong_info.put(pong.nonce as usize, (pong, (cnt + 1)));

        res
    }

    // for unit tests
    pub fn sending_ping(&mut self, nonce: usize, target: PeerId) {
        let entry = if let Some(entry) = self.waiting_pong.get_mut(&target) {
            entry
        } else {
            self.waiting_pong.put(target.clone(), LruCache::new(10));
            self.waiting_pong.get_mut(&target).unwrap()
        };

        entry.put(nonce, Clock::instant());
    }

    pub fn get_ping(&mut self, peer_id: PeerId) -> usize {
        if let Some(entry) = self.last_ping_nonce.get_mut(&peer_id) {
            *entry += 1;
            *entry - 1
        } else {
            self.last_ping_nonce.put(peer_id, 1);
            0
        }
    }

    /// Fetch `ping_info` and `pong_info` for units tests.
    pub fn fetch_ping_pong(
        &self,
    ) -> (
        impl Iterator<Item = (&usize, &(Ping, usize))>,
        impl Iterator<Item = (&usize, &(Pong, usize))>,
    ) {
        (self.ping_info.iter(), self.pong_info.iter())
    }

    pub fn info(&mut self) -> RoutingTableInfo {
        let account_peers = self
            .get_announce_accounts()
            .into_iter()
            .map(|announce_account| (announce_account.account_id, announce_account.peer_id))
            .collect();
        RoutingTableInfo { account_peers, peer_forwarding: self.peer_forwarding.clone() }
    }

    /// Public interface for `account_peers`
    ///
    /// Get keys currently on cache.
    pub fn get_accounts_keys(&mut self) -> Vec<AccountId> {
        self.account_peers.iter().map(|(k, _v)| (k.clone())).collect()
    }

    /// Get announce accounts on cache.
    pub fn get_announce_accounts(&mut self) -> Vec<AnnounceAccount> {
        self.account_peers.iter().map(|(_k, v)| v).cloned().collect()
    }

    /// Get number of accounts
    pub fn get_announce_accounts_size(&mut self) -> usize {
        self.account_peers.len()
    }

    /// Get account announce from
    pub fn get_announce(&mut self, account_id: &AccountId) -> Option<AnnounceAccount> {
        if let Some(announce_account) = self.account_peers.get(account_id) {
            Some(announce_account.clone())
        } else {
            self.store
                .get_ser(ColAccountAnnouncements, account_id.as_ref().as_bytes())
                .map(|res: Option<AnnounceAccount>| {
                    if let Some(announce_account) = res {
                        self.account_peers.put(account_id.clone(), announce_account.clone());
                        Some(announce_account)
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|e| {
                    warn!(target: "network", "Error loading announce account from store: {:?}", e);
                    None
                })
        }
    }

    pub fn get_local_edge(&self, other_peer: &PeerId) -> Option<&Edge> {
        self.local_edges_info.get(other_peer)
    }
}

#[derive(Debug)]
pub struct RoutingTableInfo {
    pub account_peers: HashMap<AccountId, PeerId>,
    pub peer_forwarding: Arc<HashMap<PeerId, Vec<PeerId>>>,
}
