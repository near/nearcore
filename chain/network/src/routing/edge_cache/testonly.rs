use crate::routing::edge_cache::{EdgeCache, EdgeKey};
use near_primitives::network::PeerId;

impl EdgeCache {
    pub(crate) fn get_nonce_for_active_edge(&self, key: &EdgeKey) -> Option<u64> {
        self.active_edges.get(key).map(|val| val.edge.nonce())
    }

    pub(crate) fn check_mapping(&self, active_nodes: Vec<PeerId>) {
        // Check the mapping itself for externally visible properties
        assert_eq!(active_nodes.len(), self.active_nodes_ct());
        let mut assigned_ids: Vec<u32> =
            active_nodes.iter().map(|peer_id| self.get_id(peer_id)).collect();
        assigned_ids.sort();
        assigned_ids.dedup();
        assert_eq!(active_nodes.len(), assigned_ids.len());
        for id in assigned_ids {
            assert!(id < (self.max_id() as u32));
        }

        // Check internally that the set of mapped nodes is exactly those which are active_nodes
        assert_eq!(active_nodes.len(), self.p2id.len());
        for peer_id in &active_nodes {
            assert!(self.p2id.contains_key(&peer_id));
        }

        // Check internally that the mapped ids and unused ids together are precisely 0,1,2,...
        let expected_ids = Vec::from_iter(0..(self.max_id() as u32));
        let mut actual_ids: Vec<u32> = self.p2id.values().cloned().collect();
        actual_ids.append(&mut self.unused.clone());
        actual_ids.sort();
        assert_eq!(expected_ids, actual_ids);

        // Check internally that ids are unused iff their degree is 0
        for id in expected_ids {
            assert_eq!(self.degree[id as usize] == 0, self.unused.contains(&id));
        }
    }
}
