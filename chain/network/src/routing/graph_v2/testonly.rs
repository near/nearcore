use crate::network_protocol;
use crate::routing::graph_v2::Inner;
use crate::routing::GraphV2;
use crate::routing::NextHopTable;
use crate::types::Edge;
use near_async::time::FakeClock;
use near_primitives::network::PeerId;
use std::collections::{HashMap, HashSet};

impl Inner {
    pub(crate) fn verify_edges(&self, _edges: &Vec<Edge>) -> bool {
        // In tests we make fake edges and don't bother to sign them
        true
    }
}

impl GraphV2 {
    pub(crate) fn compute_next_hops(&self) -> (NextHopTable, HashMap<PeerId, u32>) {
        self.inner.lock().compute_next_hops(&HashSet::new())
    }

    pub(crate) fn update_shortest_path_tree(&self, root: PeerId, edges: Vec<Edge>) -> bool {
        self.inner.lock().handle_shortest_path_tree_message(
            &FakeClock::default().clock(),
            &network_protocol::ShortestPathTree { root, edges },
        )
    }
}
