use crate::types::{AnnounceAccount, PeerId};
use near_primitives::types::AccountId;
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub enum RoutingTableUpdate {
    NewAccount,
    NewRoute,
    UpdatedAccount,
    Ignore,
}

impl RoutingTableUpdate {
    pub fn is_new(&self) -> bool {
        match self {
            RoutingTableUpdate::NewAccount => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RoutingTableEntry {
    /// Keep track of several routes for the same account id.
    pub routes: Vec<AnnounceAccount>,
    /// Last time a route was used.
    last_update: Instant,
    /// Index of the last route reported to the Peer Manager to send message.
    last_route_reported: usize,
    /// When new peers get connected to us, we will send one (among the shortest routes)
    /// for every account id known to us.
    /// This is the index of the last best route shared.
    last_shortest_route_reported: usize,
    /// Maximum number of routes that we should keep track.
    max_routes_to_save: usize,
}

impl RoutingTableEntry {
    pub fn new(route: AnnounceAccount, max_routes_to_save: usize) -> Self {
        Self {
            routes: vec![route],
            last_update: Instant::now(),
            last_route_reported: 0,
            last_shortest_route_reported: 0,
            max_routes_to_save,
        }
    }

    fn route_through_peer_index(&self, peer_id: &PeerId) -> Option<usize> {
        self.routes
            .iter()
            .enumerate()
            .find(|(_, route)| route.peer_id() == *peer_id)
            .map(|(position, _)| position)
    }

    fn add_route(&mut self, route: AnnounceAccount) -> RoutingTableUpdate {
        self.last_update = Instant::now();
        let peer_index = self.route_through_peer_index(&route.peer_id());

        if let Some(peer_index) = peer_index {
            // We already have a route through this peer.
            if self.routes[peer_index].num_hops() <= route.num_hops() {
                // Ignore this route since we already have a route at least as good
                // as this one through this peer.
                RoutingTableUpdate::Ignore
            } else {
                // Overwrite old route through this peer with better route.
                self.routes[peer_index] = route;
                RoutingTableUpdate::UpdatedAccount
            }
        } else {
            if self.routes.len() == self.max_routes_to_save {
                // Don't exceed maximum number of routes to store for every
                RoutingTableUpdate::Ignore
            } else {
                self.routes.push(route);
                RoutingTableUpdate::NewRoute
            }
        }
    }

    fn next_peer_id(&mut self) -> PeerId {
        self.last_route_reported += 1;
        if self.last_route_reported == self.routes.len() {
            self.last_route_reported = 0;
        }
        self.routes[self.last_route_reported].peer_id()
    }

    pub fn next_best_route(&mut self) -> AnnounceAccount {
        // Save unwrap (there is at least one route always).
        let shortest_route_hops = self.routes.iter().map(|route| route.num_hops()).min().unwrap();

        loop {
            self.last_shortest_route_reported += 1;
            if self.last_shortest_route_reported == self.routes.len() {
                self.last_shortest_route_reported = 0;
            }

            if self.routes[self.last_shortest_route_reported].num_hops() == shortest_route_hops {
                return self.routes[self.last_shortest_route_reported].clone();
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct RoutingTable {
    pub account_peers: HashMap<AccountId, RoutingTableEntry>,
    last_purge: Instant,
    ttl_account_id_router: Duration,
    /// Maximum number of routes that we should keep track for each Account id
    max_routes_to_save: usize,
}

impl RoutingTable {
    pub fn new(ttl_account_id_router: Duration, max_routes_to_save: usize) -> Self {
        Self {
            account_peers: HashMap::new(),
            last_purge: Instant::now(),
            ttl_account_id_router,
            max_routes_to_save,
        }
    }

    fn remove_old_routes(&mut self) {
        let now = Instant::now();
        let ttl_account_id_router = self.ttl_account_id_router;

        if (now - self.last_purge) >= ttl_account_id_router {
            self.account_peers = self
                .account_peers
                .drain()
                .filter(|entry| now - entry.1.last_update < ttl_account_id_router)
                .collect();

            self.last_purge = Instant::now();
        }
    }

    pub fn update(&mut self, data: AnnounceAccount) -> RoutingTableUpdate {
        self.remove_old_routes();
        match self.account_peers.get_mut(&data.account_id) {
            // If this account id is already tracked in the routing table ...
            Some(entry) => entry.add_route(data),
            // If we don't have this account id store it in the routing table.
            None => {
                self.account_peers.insert(
                    data.account_id.clone(),
                    RoutingTableEntry::new(data, self.max_routes_to_save),
                );
                RoutingTableUpdate::NewAccount
            }
        }
    }

    /// Remove all routes that contains this peer as the first hop.
    pub fn remove(&mut self, peer_id: &PeerId) {
        let mut to_delete = vec![];
        for (account_id, mut value) in self.account_peers.iter_mut() {
            value.routes =
                value.routes.drain(..).filter(|route| &route.peer_id() != peer_id).collect();

            if value.routes.is_empty() {
                to_delete.push(account_id.clone());
            }
        }

        for account_id in to_delete.into_iter() {
            self.account_peers.remove(&account_id);
        }
    }

    pub fn get_route(&mut self, account_id: &AccountId) -> Option<PeerId> {
        self.remove_old_routes();
        self.account_peers.get_mut(account_id).map(|entry| {
            entry.last_update = Instant::now();
            entry.next_peer_id()
        })
    }
}
