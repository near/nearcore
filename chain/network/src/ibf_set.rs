use crate::ibf::{Ibf, IbfElem};
use crate::ibf_peer_set::SlotMapId;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use tracing::error;

pub const MIN_IBF_LEVEL: u64 = 10;
pub const MAX_IBF_LEVEL: u64 = 17;

#[derive(Default)]
pub struct IbfSet<T: Hash + Clone> {
    seed: u64,
    ibf: Vec<Ibf<DefaultHasher>>,
    h2e: HashMap<u64, SlotMapId>,
    hasher: DefaultHasher,
    pd: PhantomData<T>,
}

impl<T> IbfSet<T>
where
    T: Hash + Clone,
{
    pub fn get_ibf_vec(&self, k: usize) -> Vec<IbfElem> {
        self.ibf[k - MIN_IBF_LEVEL as usize].data.clone()
    }

    pub fn get_ibf(&self, k: usize) -> Ibf<DefaultHasher> {
        self.ibf[k - MIN_IBF_LEVEL as usize].clone()
    }

    pub fn new() -> Self {
        let mut ibfs = Vec::new();
        for i in MIN_IBF_LEVEL..=MAX_IBF_LEVEL {
            ibfs.push(Ibf::new(1 << i, i));
        }
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(u64::max_value());
        Self {
            ibf: ibfs,
            h2e: Default::default(),
            hasher,
            pd: PhantomData::<T>::default(),
            seed: 0,
        }
    }

    pub fn get_edges_by_hashes(&self, unknown_edges: &[u64]) -> Vec<SlotMapId> {
        unknown_edges.iter().filter_map(|v| self.h2e.get(v)).cloned().collect()
    }

    pub fn get_edges_by_hashes2(&self, unknown_edges: &[u64]) -> (Vec<SlotMapId>, Vec<u64>) {
        (
            unknown_edges.iter().filter_map(|v| self.h2e.get(v)).cloned().collect(),
            unknown_edges
                .iter()
                .filter_map(|v| if let None = self.h2e.get(v) { Some(*v) } else { None })
                .collect(),
        )
    }

    pub fn set_seed(&mut self, seed: u64) {
        if self.seed != 0 {
            error!("seed already set");
        }
        self.seed = seed;
        self.hasher.write_u64(seed);
    }

    pub fn add_edge(&mut self, item: &T, id: SlotMapId) -> bool {
        let mut h = self.hasher.clone();
        item.hash(&mut h);
        let h = h.finish();
        if let Some(_) = self.h2e.insert(h, id) {
            error!("hash already exists in IbfSet");
            return false;
        }
        for ibf in self.ibf.iter_mut() {
            ibf.add(h);
        }
        true
    }

    pub fn remove_edge(&mut self, item: &T) -> bool {
        let mut h = self.hasher.clone();
        item.hash(&mut h);
        let h = h.finish();
        if self.h2e.remove(&h) == None {
            error!("trying to remove not existing edge from IbfSet");
            return false;
        }
        for ibf in self.ibf.iter_mut() {
            ibf.add(h);
        }
        true
    }

    /*
    fn add_edges(&mut self, items: &HashSet<T>) {
        for e in items.iter() {
            self.add_edge(e);
        }
    }

    fn add_edges2(&mut self, items: &Vec<T>) {
        for e in items.iter() {
            self.add_edge(e);
        }
    }

    fn split(&mut self, items: &Vec<u64>) -> (Vec<SlotMapId>, Vec<u64>) {
        let mut local_id = Vec::new();
        let mut remote_hash = Vec::new();
        for e in items {
            if let Some(e) = self.h2e.get(e) {
                local_id.push(e.clone());
            } else {
                remote_hash.push(*e);
            }
        }
        (local_id, remote_hash)
    }

    fn get_edges(&mut self, items: &Vec<u64>) -> Vec<SlotMapId> {
        let mut local_id = Vec::new();
        for e in items {
            if let Some(e) = self.h2e.get(e) {
                local_id.push(e.clone());
            }
        }
        local_id
    }
    */
}

#[cfg(test)]
mod test {
    /*
        use crate::blt_graph::BltGraph;
        use crate::graph::Graph;
        use std::cmp::max;
        use std::iter::FromIterator;
        use std::time::Instant;

        #[test]
        fn test_999000_550_small_diff() {
            test_general(999000, 500);
        }

        #[test]
        fn test_0_500000_full_sync() {
            test_general(0, 500000);
        }

        fn test_general(common: usize, one_side: usize) {
            let mut g = Graph::new(1000000);
            let mut rng = rand::thread_rng();
            g.add_random_edges(common, &mut rng);

            let mut a = g.clone();
            a.add_random_edges(one_side, &mut rng);
            let mut b = g.clone();
            b.add_random_edges(one_side, &mut rng);

            let mut blt_a = BltGraph::new();
            let mut blt_b = BltGraph::new();

            blt_a.add_edges(&a.edges);
            blt_b.add_edges(&b.edges);

            let start = Instant::now();

            // We have created Ibf for each power of 2.
            // We are going to exchange Ibf starting from 2^10, because it takes less than 1ms to compute
            // and the size of meta data is just 8kb.

            // Let's say Alice and Bob each have their own graph.
            // They graphs differ, Alice may have some edges Bob doesn't, and vice versa.
            // Both of them want to have the same graph with added edges from the other party.
            for i in 10..20 {
                // Each item is one round trip
                if blt_b.blts[i].capacity > max(a.edges.len(), b.edges.len()) / 10 {
                    // 2 round trips

                    // Alice send list of all hashes to Bob
                    let hashes_from_a: Vec<u64> = Vec::from_iter(blt_a.h2e.keys().cloned().into_iter());

                    // Bob responds with edges for Alice and with that he needs
                    let (_, edges_b_needs) = blt_b.split(&hashes_from_a);
                    // and edges he knows about
                    let hashes_from_b: Vec<u64> = Vec::from_iter(blt_b.h2e.keys().cloned().into_iter());

                    // Alice knows which edges she is missing.
                    let (_, edges_a_needs) = blt_a.split(&hashes_from_b);

                    //  Alice sends edges which Bob is missing
                    let edges_for_b = blt_a.get_edges(&edges_b_needs);

                    // Bob applies edges
                    b.add_edges(&edges_for_b);
                    blt_b.add_edges2(&edges_for_b);

                    // Bob returns edges Alice needs
                    let edges_for_a = blt_b.get_edges(&edges_a_needs);

                    // Alice adds edges from Bob
                    a.add_edges(&edges_for_a);
                    blt_a.add_edges2(&edges_for_a);

                    println!(
                        "FULL SYNC {} capacity: {} total: {}ms a.edges: {} b.edges: {} added_a: {} added_b: {}",
                        i,
                        blt_b.blts[i].capacity,
                        start.elapsed().as_millis(),
                        a.edges.len(),
                        b.edges.len(),
                        edges_for_a.len(),
                        edges_for_b.len()
                    );
                    break;
                }
                // 1 round trip

                // Alice asks BOB to give her Bob's Ibf[i] and gets response.
                let mut bob_response = blt_b.blts[i].clone();

                // Alice merges the result
                bob_response.merge(&blt_a.blts[i]);

                // Alice tries to recover the result, she sees list of edges which are in the difference
                let (res, success) = bob_response.try_recover();

                let (edges_for_b, edges_a_needs) = blt_a.split(&res);

                // Alice sends edges Bob doesn't have and he adds them
                b.add_edges(&edges_for_b);
                blt_b.add_edges2(&edges_for_b);

                // Bob sends list to Alice list of edges she asked for and she adds them
                let edges_for_a = blt_b.get_edges(&edges_a_needs);
                a.add_edges(&edges_for_a);
                blt_a.add_edges2(&edges_for_a);

                println!(
                    "{} capacity: {} {} {} total: {}ms a.edges: {} b.edges: {} added_a: {} added_b: {}",
                    i,
                    blt_b.blts[i].capacity,
                    res.len(),
                    success,
                    start.elapsed().as_millis(),
                    a.edges.len(),
                    b.edges.len(),
                    edges_for_a.len(),
                    edges_for_b.len()
                );
                if success {
                    // All edges were recovered
                    break;
                }
            }
            assert_eq!(a.edges.len(), b.edges.len());
            assert!(a.edges.len() as f64 >= common as f64 + 1.9 * (one_side as f64));
        }
    */
}
