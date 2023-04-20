use super::*;
use std::collections::HashMap;

#[derive(Default, Debug, Clone, Hash, PartialEq, Eq)]
pub struct Component {
    pub peers: Vec<PeerId>,
    pub edges: Vec<Edge>,
}

impl Component {
    pub fn normal(mut self) -> Self {
        self.peers.sort();
        self.edges.sort_by(|a, b| a.key().cmp(b.key()));
        self
    }
}

impl Store {
    /// Reads all the components from the database.
    /// Panics if any of the invariants has been violated.
    pub fn list_components(&self) -> Vec<Component> {
        let edges: HashMap<_, _> =
            self.0.iter::<schema::ComponentEdges>().map(|x| x.unwrap()).collect();
        let peers: HashMap<_, _> =
            self.0.iter::<schema::PeerComponent>().map(|x| x.unwrap()).collect();
        let lcn: HashMap<(), _> =
            self.0.iter::<schema::LastComponentNonce>().map(|x| x.unwrap()).collect();
        // all component nonces should be <= LastComponentNonce
        let lcn = lcn.get(&()).unwrap_or(&0);
        for (c, _) in &edges {
            assert!(c <= lcn);
        }
        for (_, c) in &peers {
            assert!(c <= lcn);
        }
        // Each edge has to be incident to at least one peer in the same component.
        for (c, es) in &edges {
            for e in es {
                let key = e.key();
                assert!(peers.get(&key.0) == Some(c) || peers.get(&key.1) == Some(c));
            }
        }
        let mut cs = HashMap::<u64, Component>::new();
        for (c, es) in edges {
            cs.entry(c).or_default().edges = es;
        }
        for (p, c) in peers {
            cs.entry(c).or_default().peers.push(p);
        }
        cs.into_iter().map(|(_, v)| v).collect()
    }
}
