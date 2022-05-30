use crate::schema;
use near_network_primitives::types::Edge;
use near_primitives::network::PeerId;
use smart_default::SmartDefault;
use std::collections::{HashMap, HashSet};
use tracing::debug;

/// TEST-ONLY
#[derive(SmartDefault, Debug, Hash, PartialEq, Eq)]
pub struct Component {
    pub peers: Vec<PeerId>,
    pub edges: Vec<Edge>,
}

/// Everytime a group of peers becomes unreachable at the same time; We store edges belonging to
/// them in components. We remove all of those edges from memory, and save them to database,
/// If any of them become reachable again, we re-add whole component.
///
/// To store components, we have following column in the DB.
/// DBCol::LastComponentNonce -> stores component_nonce: u64, which is the lowest nonce that
///                          hasn't been used yet. If new component gets created it will use
///                          this nonce.
/// DBCol::ComponentEdges     -> Mapping from `component_nonce` to list of edges
/// DBCol::PeerComponent      -> Mapping from `peer_id` to last component nonce if there
///                          exists one it belongs to.
pub struct ComponentStore(schema::Store);
impl ComponentStore {
    pub fn new(s: schema::Store) -> Self {
        Self(s)
    }

    pub fn push_component(
        &mut self,
        peers: &HashSet<PeerId>,
        edges: &Vec<Edge>,
    ) -> Result<(), schema::Error> {
        debug!(target: "network", "push_component: We are going to remove the following peers: {}", peers.len());
        let component = self.0.get::<schema::LastComponentNonce>(&())?.unwrap_or(0) + 1;
        let mut update = self.0.new_update();
        update.set::<schema::LastComponentNonce>(&(), &component);
        update.set::<schema::ComponentEdges>(&component, &edges);
        for peer_id in peers {
            update.set::<schema::PeerComponent>(peer_id, &component);
        }
        update.commit()
    }

    pub fn pop_component(&mut self, peer_id: &PeerId) -> Result<Vec<Edge>, schema::Error> {
        // Fetch the component assigned to the peer.
        let component = match self.0.get::<schema::PeerComponent>(peer_id)? {
            Some(c) => c,
            _ => return Ok(vec![]),
        };
        let edges = self.0.get::<schema::ComponentEdges>(&component)?.unwrap_or(vec![]);
        let mut update = self.0.new_update();
        update.delete::<schema::ComponentEdges>(&component);
        let mut peers_checked = HashSet::new();
        for edge in &edges {
            let key = edge.key().clone();
            for peer_id in [&key.0, &key.1] {
                if !peers_checked.insert(peer_id.clone()) {
                    // Store doesn't accept 2 mutations modifying the same row in a single
                    // transaction, even if they are identical. Therefore tracking peers_checked
                    // is critical for correctness, rather than just an optimization minimizing
                    // the number of lookups.
                    continue;
                }
                match self.0.get::<schema::PeerComponent>(&peer_id)? {
                    Some(c) if c == component => update.delete::<schema::PeerComponent>(&peer_id),
                    _ => {}
                }
            }
        }
        update.commit()?;
        Ok(edges)
    }

    /// TEST-ONLY
    /// Reads all the components from the database.
    /// Panics if any of the invariants has been violated.
    #[allow(dead_code)]
    pub fn snapshot(&self) -> Vec<Component> {
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
