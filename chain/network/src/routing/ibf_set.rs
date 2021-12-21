use crate::routing::ibf::{Ibf, IbfBox};
use crate::routing::ibf_peer_set::{SlotMapId, ValidIBFLevel, MAX_IBF_LEVEL, MIN_IBF_LEVEL};
use near_stable_hasher::StableHasher;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use tracing::{error, warn};

/// Stores list of `Ibf` data structures of various sizes.
/// In the current implementation we use sizes from 2^10+  2 ... 2^17 + 2.
#[derive(Default)]
pub struct IbfSet<T: Hash + Clone> {
    seed: u64,
    ibf: Vec<Ibf>,
    h2e: HashMap<u64, SlotMapId>,
    hasher: StableHasher,
    pd: PhantomData<T>,
}

impl<T: Hash + Clone> Debug for IbfSet<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("").field(&self.seed).field(&self.ibf.len()).field(&self.h2e.len()).finish()
    }
}

impl<T> IbfSet<T>
where
    T: Hash + Clone,
{
    pub fn get_ibf_vec(&self, k: ValidIBFLevel) -> &Vec<IbfBox> {
        &self.ibf[(k.0 - MIN_IBF_LEVEL.0) as usize].data
    }

    /// Get seed used to generate this IbfSet
    pub fn get_seed(&self) -> u64 {
        self.seed
    }

    /// Get `Ibf` based on selected `k`.
    pub fn get_ibf(&self, k: ValidIBFLevel) -> &Ibf {
        &self.ibf[(k.0 - MIN_IBF_LEVEL.0) as usize]
    }

    pub fn new(seed: u64) -> Self {
        let ibf = (MIN_IBF_LEVEL.0..=MAX_IBF_LEVEL.0).map(|i| Ibf::new(1 << i, seed ^ i));
        let mut hasher = StableHasher::default();
        hasher.write_u64(seed);
        Self {
            seed,
            ibf: ibf.collect(),
            h2e: Default::default(),
            hasher,
            pd: PhantomData::<T>::default(),
        }
    }

    /// Get list of edges based on given list of hashes.
    pub fn get_edges_by_hashes_ext(&self, edges: &[u64]) -> (Vec<SlotMapId>, Vec<u64>) {
        let mut known_edges = vec![];
        let mut unknown_edges = vec![];

        for hash in edges {
            if let Some(edge) = self.h2e.get(hash) {
                known_edges.push(*edge)
            } else {
                unknown_edges.push(*hash)
            }
        }
        (known_edges, unknown_edges)
    }

    /// Add edge to all IBFs
    pub fn add_edge(&mut self, item: &T, id: SlotMapId) -> bool {
        let mut h = self.hasher.clone();
        item.hash(&mut h);
        let h = h.finish();
        if self.h2e.insert(h, id).is_some() {
            warn!(target: "network", "hash already exists in IbfSet");
            return false;
        }
        for ibf in self.ibf.iter_mut() {
            ibf.add(h);
        }
        true
    }

    /// Remove edge from all IBFs
    pub fn remove_edge(&mut self, item: &T) -> bool {
        let mut h = self.hasher.clone();
        item.hash(&mut h);
        let h = h.finish();
        if self.h2e.remove(&h).is_none() {
            error!(target: "network", "trying to remove not existing edge from IbfSet");
            return false;
        }
        for ibf in self.ibf.iter_mut() {
            ibf.remove(h);
        }
        true
    }
}

#[cfg(test)]
mod test {
    use crate::routing::ibf_peer_set::{SlotMapId, ValidIBFLevel};
    use crate::routing::ibf_set::IbfSet;

    #[test]
    fn test_ibf_set() {
        let mut a = IbfSet::<u64>::new(12);
        let mut b = IbfSet::<u64>::new(12);

        for i in 0..10000 {
            a.add_edge(&(i as u64), (i + 1000000) as SlotMapId);
        }
        for i in 0..10 {
            a.remove_edge(&(i as u64));
        }
        for i in 0..10000 {
            b.add_edge(&(i + 100_u64), (i + 2000000) as SlotMapId);
        }
        for i in 10..=17 {
            let mut ibf1 = a.get_ibf(ValidIBFLevel(i)).clone();
            let ibf2 = b.get_ibf(ValidIBFLevel(i));
            ibf1.merge(&ibf2.data, ibf2.seed);
            let (mut res, diff) = ibf1.try_recover();
            assert_eq!(0, diff);
            assert_eq!(200 - 10, res.len());

            for x in 0..333 {
                res.push(x + 33333333);
            }
            assert_eq!(100 - 10, a.get_edges_by_hashes_ext(&res).0.len());
            assert_eq!(100 + 333, a.get_edges_by_hashes_ext(&res).1.len());
        }
    }
}
