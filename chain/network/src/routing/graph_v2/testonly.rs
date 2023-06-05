use crate::network_protocol;
use crate::routing::graph_v2::AdvertisedRoute;
use crate::routing::graph_v2::Inner;
use crate::routing::GraphV2;
use crate::routing::NextHopTable;
use crate::types::Edge;
use near_primitives::network::PeerId;
use std::collections::{HashMap, HashSet};

impl Inner {
    pub(crate) fn verify_edges(&mut self, edges: &Vec<Edge>) -> bool {
        // In tests we make fake edges and don't bother to sign them
        for edge in edges {
            self.edge_cache.write_verified_nonce(edge);
        }
        true
    }
}

impl GraphV2 {
    pub(crate) fn compute_next_hops(&self) -> (NextHopTable, HashMap<PeerId, u32>) {
        self.inner.lock().compute_next_hops(&HashSet::new())
    }

    pub(crate) fn update_distance_vector(
        &self,
        root: PeerId,
        routes: Vec<AdvertisedRoute>,
        edges: Vec<Edge>,
    ) -> bool {
        self.inner.lock().handle_distance_vector(&network_protocol::DistanceVector {
            root,
            routes,
            edges,
        })
    }
}
