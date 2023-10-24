use crate::routing::edge_cache::{EdgeCache, EdgeKey};
use crate::types::Edge;
use near_primitives::network::PeerId;

impl EdgeCache {
    pub(crate) fn is_active(&self, edge: &Edge) -> bool {
        self.active_edges.contains_key(&edge.key().into())
    }

    pub(crate) fn get_nonce_for_active_edge(&self, key: &EdgeKey) -> Option<u64> {
        self.active_edges.get(key).map(|val| val.edge.nonce())
    }

    pub(crate) fn check_mapping_external(&self, mapped_nodes: &Vec<PeerId>) {
        // Check the mapped ids for externally visible properties of the mapping
        let mut assigned_ids: Vec<u32> =
            mapped_nodes.iter().map(|peer_id| self.get_id(peer_id)).collect();
        assigned_ids.sort();
        assigned_ids.dedup();
        assert_eq!(mapped_nodes.len(), assigned_ids.len());
        for id in assigned_ids {
            assert!(id < (self.max_id() as u32));
        }
    }

    pub(crate) fn check_mapping_internal(&self, mapped_nodes: &Vec<PeerId>) {
        // Check internally that the set of mapped nodes is exactly those which are expected
        assert_eq!(mapped_nodes.len(), self.p2id.len());
        for peer_id in mapped_nodes {
            assert!(self.p2id.contains_key(&peer_id));
        }

        // Check internally that the mapped ids and unused ids together are precisely 0,1,2,...
        let universe = Vec::from_iter(0..(self.max_id() as u32));
        let mut actual_ids: Vec<u32> = self.p2id.values().cloned().collect();
        actual_ids.append(&mut self.unused.clone());
        actual_ids.sort();
        assert_eq!(universe, actual_ids);

        // An id should be in use iff it's id 0 (the local node's id)
        // or if it's assigned to some node incident with an active edge
        for id in universe {
            let should_be_used = id == 0 || self.degree[id as usize] != 0;
            assert_eq!(should_be_used, !self.unused.contains(&id));
        }

        // Check exact consistency of the degree counts with the active edges
        let mut expected_degree = vec![0; self.max_id()];
        for (key, edge) in &self.active_edges {
            assert!(edge.refcount > 0);
            expected_degree[self.get_id(&key.peer0) as usize] += 1;
            expected_degree[self.get_id(&key.peer1) as usize] += 1;
        }
        assert_eq!(expected_degree, self.degree);
    }

    pub(crate) fn check_mapping(&self, mapped_nodes: Vec<PeerId>) {
        self.check_mapping_external(&mapped_nodes);
        self.check_mapping_internal(&mapped_nodes);
    }
}
