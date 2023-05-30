use crate::routing::graph_v2::Inner;
use crate::routing::GraphV2;
use crate::routing::NextHopTable;
use crate::types::Edge;
use near_primitives::network::PeerId;
use std::collections::{HashMap, HashSet};

impl Inner {
    pub(crate) fn verify_edges(&self, _edges: &Vec<Edge>) -> bool {
        true
    }
}

impl GraphV2 {
    pub(crate) fn compute_next_hops(&self) -> (NextHopTable, HashMap<PeerId, u32>) {
        self.inner.lock().compute_next_hops(&HashSet::new())
    }
}
