use crate::network_protocol;
use crate::routing::graph_v2::AdvertisedPeerDistance;
use crate::routing::graph_v2::Inner;
use crate::routing::{GraphV2, NetworkTopologyChange, NextHopTable};
use crate::types::Edge;
use near_async::time;
use near_primitives::network::PeerId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

impl Inner {
    pub(crate) fn verify_and_cache_edge_nonces(&mut self, edges: &Vec<Edge>) -> bool {
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
        distances: Vec<AdvertisedPeerDistance>,
        edges: Vec<Edge>,
    ) -> bool {
        self.inner.lock().handle_distance_vector(&network_protocol::DistanceVector {
            root,
            distances,
            edges,
        })
    }

    pub(crate) async fn process_network_event(
        self: &Arc<Self>,
        event: NetworkTopologyChange,
    ) -> Option<network_protocol::DistanceVector> {
        let clock = time::FakeClock::default();
        let (to_broadcast, oks) =
            self.batch_process_network_changes(&clock.clock(), vec![event]).await;
        assert!(oks[0]);
        to_broadcast
    }

    pub(crate) async fn process_invalid_network_event(
        self: &Arc<Self>,
        event: NetworkTopologyChange,
    ) {
        let clock = time::FakeClock::default();
        let (_, oks) = self.batch_process_network_changes(&clock.clock(), vec![event]).await;
        assert!(!oks[0]);
    }

    // Checks that the DistanceVector message for the local node is valid
    // and correctly advertises the node's available routes.
    pub(crate) fn verify_own_distance_vector(
        &self,
        expected_distances: HashMap<PeerId, u32>,
        distance_vector: &network_protocol::DistanceVector,
    ) {
        let mut inner = self.inner.lock();

        assert_eq!(expected_distances, inner.my_distances);

        let mut expected_distances_by_id: Vec<Option<u32>> = vec![None; inner.edge_cache.max_id()];
        for (peer_id, distance) in expected_distances.iter() {
            let id = inner.edge_cache.get_id(peer_id) as usize;
            expected_distances_by_id[id] = Some(*distance);
        }

        assert_eq!(
            expected_distances_by_id,
            inner.validate_routing_distances(distance_vector).unwrap()
        );
    }
}
