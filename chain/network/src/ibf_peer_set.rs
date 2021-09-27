use std::collections::HashMap;

use rand::Rng;

use near_primitives::network::PeerId;

use crate::ibf_set::IbfSet;
use crate::routing::{Edge, SimpleEdge};

pub type SlotMapId = u64;

/// In order to reduce memory usage/bandwidth used we map each edge to u64.
/// SlotMap contains mapping from SimpleToHash, and vice versa.
#[derive(Default)]
pub struct SlotMap {
    id: u64,
    id2e: HashMap<SlotMapId, SimpleEdge>,
    e2id: HashMap<SimpleEdge, SlotMapId>,
}

impl SlotMap {
    pub fn insert(&mut self, edge: &SimpleEdge) -> Option<SlotMapId> {
        if self.e2id.contains_key(edge) {
            return None;
        }

        let new_id = self.id as SlotMapId;
        self.id += 1;

        self.e2id.insert(edge.clone(), new_id);
        self.id2e.insert(new_id, edge.clone());

        Some(new_id)
    }

    pub fn get(&self, edge: &SimpleEdge) -> Option<SlotMapId> {
        self.e2id.get(edge).cloned()
    }

    fn get_by_id(&self, id: &SlotMapId) -> Option<SimpleEdge> {
        self.id2e.get(id).cloned()
    }

    fn pop(&mut self, edge: &SimpleEdge) -> Option<SlotMapId> {
        if let Some(&id) = self.e2id.get(edge) {
            self.e2id.remove(edge);
            self.id2e.remove(&id);

            return Some(id);
        }
        None
    }
}

/// IBfPeerSet contains collection of IbfSets, each for one connected peer.
#[derive(Default)]
pub struct IbfPeerSet {
    peers: HashMap<PeerId, IbfSet<SimpleEdge>>,
    slot_map: SlotMap,
    edges: u64,
}

impl IbfPeerSet {
    pub fn get(&self, peer_id: &PeerId) -> Option<&IbfSet<SimpleEdge>> {
        self.peers.get(peer_id)
    }

    /// Add IbfSet assigned to given peer, defined by `seed`.
    pub fn add_peer(
        &mut self,
        peer_id: PeerId,
        seed: Option<u64>,
        edges_info: &mut HashMap<(PeerId, PeerId), Edge>,
    ) -> u64 {
        if let Some(ibf_set) = self.peers.get(&peer_id) {
            return ibf_set.get_seed();
        }
        let seed = if let Some(seed) = seed {
            seed
        } else {
            let mut rng = rand::thread_rng();
            rng.gen()
        };

        let mut ibf_set = IbfSet::new(seed);
        // Initialize IbfSet with edges
        for (key, e) in edges_info.iter() {
            let se = SimpleEdge::new(key.0.clone(), key.1.clone(), e.nonce);
            if let Some(id) = self.slot_map.get(&se) {
                ibf_set.add_edge(&se, id);
            }
        }
        let seed = ibf_set.get_seed();
        self.peers.insert(peer_id.clone(), ibf_set);
        seed
    }

    /// Remove IbfSet associated with peer.
    pub fn remove_peer(&mut self, peer_id: &PeerId) {
        self.peers.remove(peer_id);
    }

    /// Add edge to each IbfSet for each peer.
    pub fn add_edge(&mut self, edge: &SimpleEdge) -> Option<SlotMapId> {
        let id = self.slot_map.insert(edge);
        if let Some(id) = id {
            self.edges += 1;
            for (_, val) in self.peers.iter_mut() {
                val.add_edge(edge, id);
            }
        }
        id
    }

    /// Remove edge from each IbfSet for each peer.
    pub fn remove_edge(&mut self, edge: &SimpleEdge) -> bool {
        if let Some(_id) = self.slot_map.pop(edge) {
            self.edges -= 1;
            for (_, val) in self.peers.iter_mut() {
                val.remove_edge(&edge);
            }
            return true;
        }
        false
    }

    /// Recover edges based on list of SlotMapId
    fn recover_edges(&self, edges: &[SlotMapId]) -> Vec<SimpleEdge> {
        edges.iter().filter_map(|v| self.slot_map.get_by_id(v)).collect()
    }

    /// After we recover list of hashes, split edges between those that we know, and ones we don't know about.
    pub fn split_edges_for_peer(
        &self,
        peer_id: &PeerId,
        unknown_edges: &[u64],
    ) -> (Vec<SimpleEdge>, Vec<u64>) {
        if let Some(ibf) = self.get(peer_id) {
            let (known_edges, unknown_edges) = ibf.get_edges_by_hashes_ext(unknown_edges);
            return (self.recover_edges(known_edges.as_slice()), unknown_edges);
        }
        Default::default()
    }
}

#[cfg(test)]
mod test {
    use crate::ibf_peer_set::{IbfPeerSet, SimpleEdge, SlotMap, SlotMapId};
    use crate::ibf_set::IbfSet;
    use crate::routing::{Edge, ValidIBFLevel};
    use crate::test_utils::random_peer_id;
    use near_primitives::network::PeerId;
    use std::collections::HashMap;

    #[test]
    fn test_slot_map() {
        let p0 = random_peer_id();
        let p1 = random_peer_id();
        let p2 = random_peer_id();

        let e0 = SimpleEdge::new(p0.clone(), p1.clone(), 0);
        let e1 = SimpleEdge::new(p1.clone(), p2.clone(), 0);
        let e2 = SimpleEdge::new(p1.clone(), p2.clone(), 3);

        let mut sm = SlotMap::default();
        assert_eq!(0 as SlotMapId, sm.insert(&e0).unwrap());

        assert!(sm.insert(&e0).is_none());

        assert_eq!(1 as SlotMapId, sm.insert(&e1).unwrap());
        assert_eq!(2 as SlotMapId, sm.insert(&e2).unwrap());

        assert_eq!(Some(2 as SlotMapId), sm.pop(&e2));
        assert_eq!(None, sm.pop(&e2));
        assert_eq!(Some(0 as SlotMapId), sm.pop(&e0));
        assert_eq!(None, sm.pop(&e0));

        assert_eq!(Some(1 as SlotMapId), sm.get(&e1));

        assert_eq!(Some(e1.clone()), sm.get_by_id(&(1 as SlotMapId)));
        assert_eq!(None, sm.get_by_id(&(1000 as SlotMapId)));

        assert_eq!(Some(1 as SlotMapId), sm.pop(&e1));
        assert_eq!(None, sm.get(&e1));
        assert_eq!(None, sm.pop(&e1));

        assert_eq!(3 as SlotMapId, sm.insert(&e2).unwrap());
        assert_eq!(Some(3 as SlotMapId), sm.pop(&e2));

        assert_eq!(None, sm.get_by_id(&(1 as SlotMapId)));
        assert_eq!(None, sm.get_by_id(&(1000 as SlotMapId)));
    }

    #[test]
    fn test_adding_ibf_peer_set_adding_peers() {
        let peer_id = random_peer_id();
        let peer_id2 = random_peer_id();
        let mut ips = IbfPeerSet::default();

        let mut ibf_set = IbfSet::<SimpleEdge>::new(1111);

        let edge = Edge::make_fake_edge(peer_id.clone(), peer_id2.clone(), 111);
        let mut edges_info: HashMap<(PeerId, PeerId), Edge> = Default::default();
        edges_info.insert((peer_id.clone(), peer_id2.clone()), edge.clone());

        // Add Peer
        ips.add_peer(peer_id.clone(), Some(1111), &mut edges_info);

        // Remove Peer
        assert!(ips.get(&peer_id).is_some());
        assert!(ips.get(&peer_id2).is_none());
        ips.remove_peer(&peer_id);
        assert!(ips.get(&peer_id).is_none());

        // Add Peer again
        ips.add_peer(peer_id.clone(), Some(1111), &mut edges_info);

        // Add edge
        let e = SimpleEdge::new(peer_id.clone(), peer_id2.clone(), 111);
        let se = ips.add_edge(&e).unwrap();
        ibf_set.add_edge(&e, se);
        assert!(ips.add_edge(&e).is_none());

        assert!(ips.remove_edge(&e));
        assert!(!ips.remove_edge(&e));

        assert!(ips.add_edge(&e).is_some());

        let mut hashes = ibf_set.get_ibf(ValidIBFLevel(10)).try_recover().0;
        assert_eq!(1, hashes.len());

        for x in 0..4 {
            hashes.push(x);
        }

        // try to recover the edge
        assert_eq!(4, ips.split_edges_for_peer(&peer_id, &hashes).1.len());
        assert_eq!(vec!(edge.to_simple_edge()), ips.split_edges_for_peer(&peer_id, &hashes).0);
    }
}
