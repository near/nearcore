use near_primitives::network::PeerId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::ibf_set::IbfSet;
use crate::routing::Edge;

pub type SlotMapId = u64;

/*
#[derive(Default, Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub struct SlotMapId {
    pub id: u64,
}
 */

#[derive(Default)]
struct SlotMap {
    id: u64,
    id2e: HashMap<SlotMapId, SimpleEdge>,
    e2id: HashMap<SimpleEdge, SlotMapId>,
}

#[allow(dead_code)]
impl SlotMap {
    fn insert(&mut self, edge: &SimpleEdge) -> Option<SlotMapId> {
        if let Some(_) = self.e2id.get(edge) {
            return None;
        }

        let new_id = self.id as SlotMapId;
        self.id += 1;

        self.e2id.insert(edge.clone(), new_id);
        self.id2e.insert(new_id, edge.clone());

        Some(new_id)
    }

    fn get(&self, edge: &SimpleEdge) -> Option<SlotMapId> {
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

#[derive(Hash, Clone, Eq, PartialEq, Debug)]
pub struct SimpleEdge {
    pub peer0: PeerId,
    pub peer1: PeerId,
    pub nonce: u64,
}

#[derive(Default)]
pub struct IbfPeerSet {
    peers: HashMap<PeerId, Arc<Mutex<IbfSet<SimpleEdge>>>>,
    slot_map: SlotMap,
}

impl IbfPeerSet {
    pub fn get(&self, peer_id: &PeerId) -> Option<Arc<Mutex<IbfSet<SimpleEdge>>>> {
        self.peers.get(peer_id).cloned()
    }

    pub fn add_peer(
        &mut self,
        peer_id: PeerId,
        ibf_set: Arc<Mutex<IbfSet<SimpleEdge>>>,
        edges_info: &mut HashMap<(PeerId, PeerId), Edge>,
    ) {
        if let None = self.peers.insert(peer_id, ibf_set.clone()) {
            let mut peer_ibf_set = ibf_set.lock().unwrap();
            for (_, e) in edges_info.iter() {
                let se =
                    SimpleEdge { peer0: e.peer0.clone(), peer1: e.peer1.clone(), nonce: e.nonce };
                if let Some(id) = self.slot_map.get(&se) {
                    peer_ibf_set.add_edge(&se, id);
                }
            }
        }
    }

    pub fn remove_peer(&mut self, peer_id: &PeerId) {
        self.peers.remove(peer_id);
    }

    pub fn add_edge(&mut self, edge: &SimpleEdge) -> Option<SlotMapId> {
        let id = self.slot_map.insert(edge);
        if let Some(id) = id {
            for (_, val) in self.peers.iter() {
                val.lock().unwrap().add_edge(edge, id);
            }
        }
        id
    }

    pub fn remove_edge(&mut self, edge: &SimpleEdge) -> bool {
        if let Some(_id) = self.slot_map.pop(edge) {
            for (_, val) in self.peers.iter() {
                val.lock().unwrap().remove_edge(&edge);
            }
            return true;
        }
        false
    }

    pub fn recover_edges(
        &self,
        unknown_edges: &[SlotMapId],
        edges_info: &HashMap<(PeerId, PeerId), Edge>,
    ) -> Vec<Edge> {
        unknown_edges
            .iter()
            .filter_map(|v| self.slot_map.get_by_id(v))
            .filter_map(|v| edges_info.get(&(v.peer0.clone(), v.peer1.clone())))
            .cloned()
            .collect()
    }

    pub fn split_edges_for_peer(
        &self,
        peer_id: &PeerId,
        unknown_edges: &[u64],
        edges_info: &HashMap<(PeerId, PeerId), Edge>,
    ) -> (Vec<Edge>, Vec<u64>) {
        if let Some(ibf) = self.get(peer_id) {
            let (known_edges, unknown_edges) =
                ibf.lock().unwrap().get_edges_by_hashes(unknown_edges);
            return (self.recover_edges(known_edges.as_slice(), edges_info), unknown_edges);
        }
        (Default::default(), Default::default())
    }
}

#[cfg(test)]
mod test {
    use crate::ibf_peer_set::{IbfPeerSet, SimpleEdge, SlotMap, SlotMapId};
    use crate::ibf_set::IbfSet;
    use crate::routing::Edge;
    use crate::test_utils::random_peer_id;
    use near_primitives::network::PeerId;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_slot_map() {
        let p0 = random_peer_id();
        let p1 = random_peer_id();
        let p2 = random_peer_id();

        let e0 = SimpleEdge { peer0: p0.clone(), peer1: p1.clone(), nonce: 0 };
        let e1 = SimpleEdge { peer0: p1.clone(), peer1: p2.clone(), nonce: 0 };
        let e2 = SimpleEdge { peer0: p1.clone(), peer1: p2.clone(), nonce: 3 };

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
        let ibf_set = Arc::new(Mutex::new(IbfSet::new()));

        let edge = Edge::make_fake_edge(peer_id.clone(), peer_id2.clone(), 111);
        let mut edges_info: HashMap<(PeerId, PeerId), Edge> = Default::default();
        edges_info.insert((peer_id.clone(), peer_id2.clone()), edge.clone());

        // Add Peer
        ips.add_peer(peer_id.clone(), ibf_set.clone(), &mut edges_info);

        // Remove Peer
        assert!(ips.get(&peer_id).is_some());
        assert!(ips.get(&peer_id2).is_none());
        ips.remove_peer(&peer_id);
        assert!(ips.get(&peer_id).is_none());

        // Add Peer again
        ips.add_peer(peer_id.clone(), ibf_set.clone(), &mut edges_info);

        // Add edge
        let e = SimpleEdge { peer0: peer_id.clone(), peer1: peer_id2.clone(), nonce: 111 };
        assert!(ips.add_edge(&e).is_some());
        assert!(ips.add_edge(&e).is_none());

        assert!(ips.remove_edge(&e));
        assert!(!ips.remove_edge(&e));

        assert!(ips.add_edge(&e).is_some());

        let mut hashes = ibf_set.lock().unwrap().get_ibf(10).try_recover().0;

        for x in 0..4 {
            hashes.push(x);
        }

        // try to recover the edge
        assert_eq!(vec!(edge), ips.split_edges_for_peer(&peer_id, &hashes, &edges_info).0);
        assert_eq!(4, ips.split_edges_for_peer(&peer_id, &hashes, &edges_info).1.len());
    }
}
