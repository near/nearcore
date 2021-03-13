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
    fn insert(&mut self, edge: &SimpleEdge) -> SlotMapId {
        let new_id = self.id as SlotMapId;
        self.id += 1;

        self.e2id.insert(edge.clone(), new_id);
        self.id2e.insert(new_id, edge.clone());

        new_id
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

#[derive(Hash, Clone, Eq, PartialEq)]
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

    pub fn get_edges_by_ids(&self, edges: &[SlotMapId]) -> Vec<SimpleEdge> {
        edges.iter().filter_map(|v| self.slot_map.get_by_id(v)).collect()
    }

    pub fn add_peer(
        &mut self,
        peer_id: PeerId,
        ibf_set: Arc<Mutex<IbfSet<SimpleEdge>>>,
        edges_info: &mut HashMap<(PeerId, PeerId), Edge>,
    ) {
        if let None = self.peers.insert(peer_id, ibf_set.clone()) {
            let mut peer_ibf_set = ibf_set.lock().unwrap();
            // TODO: Keep (SimpleEdge + 32 bytes hash) in self.edges_info
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

    pub fn add_edge(&mut self, edge: &SimpleEdge) {
        let id = self.slot_map.insert(edge);

        for (_, val) in self.peers.iter() {
            val.lock().unwrap().add_edge(edge, id);
        }
    }

    pub fn remove_edge(&mut self, edge: &SimpleEdge) {
        if let Some(_id) = self.slot_map.pop(edge) {
            for (_, val) in self.peers.iter() {
                val.lock().unwrap().remove_edge(&edge);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::ibf_peer_set::{SimpleEdge, SlotMap, SlotMapId};
    use crate::test_utils::random_peer_id;

    #[test]
    fn test_slot_map() {
        let p0 = random_peer_id();
        let p1 = random_peer_id();
        let p2 = random_peer_id();

        let e0 = SimpleEdge { peer0: p0.clone(), peer1: p1.clone(), nonce: 0 };
        let e1 = SimpleEdge { peer0: p1.clone(), peer1: p2.clone(), nonce: 0 };
        let e2 = SimpleEdge { peer0: p1.clone(), peer1: p2.clone(), nonce: 3 };

        let mut sm = SlotMap::default();
        assert_eq!(0 as SlotMapId, sm.insert(&e0));
        assert_eq!(1 as SlotMapId, sm.insert(&e1));
        assert_eq!(2 as SlotMapId, sm.insert(&e2));

        assert_eq!(Some(2 as SlotMapId), sm.pop(&e2));
        assert_eq!(None, sm.pop(&e2));
        assert_eq!(Some(0 as SlotMapId), sm.pop(&e0));
        assert_eq!(None, sm.pop(&e0));

        assert_eq!(Some(1 as SlotMapId), sm.get(&e1));
        assert_eq!(Some(1 as SlotMapId), sm.pop(&e1));
        assert_eq!(None, sm.get(&e1));
        assert_eq!(None, sm.pop(&e1));

        assert_eq!(3 as SlotMapId, sm.insert(&e2));
        assert_eq!(Some(3 as SlotMapId), sm.pop(&e2));
    }
}
