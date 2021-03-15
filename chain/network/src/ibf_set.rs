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
}

#[cfg(test)]
mod test {
    use crate::ibf_peer_set::SlotMapId;
    use crate::ibf_set::IbfSet;

    #[test]
    fn test_ibf_set() {
        let mut a = IbfSet::<u64>::new();
        let mut b = IbfSet::<u64>::new();

        for i in 0..10000 {
            a.add_edge(&(i as u64), (i + 1000000) as SlotMapId);
        }
        for i in 0..10 {
            a.remove_edge(&(i as u64));
        }
        for i in 0..10000 {
            b.add_edge(&(i + 100 as u64), (i + 2000000) as SlotMapId);
        }
        for i in 10..=17 {
            let mut ibf1 = a.get_ibf(i);
            let ibf2 = b.get_ibf(i);
            ibf1.merge(&ibf2);
            let (mut res, diff) = ibf1.try_recover();
            assert_eq!(0, diff);
            assert_eq!(200 - 10, res.len());

            for x in 0..333 {
                res.push(x + 33333333);
            }
            assert_eq!(100 - 10, a.get_edges_by_hashes(&res).len());

            assert_eq!(100 - 10, a.get_edges_by_hashes2(&res).0.len());
            assert_eq!(100 + 333, a.get_edges_by_hashes2(&res).1.len());
        }
    }
}
