use borsh::{BorshDeserialize, BorshSerialize};
use std::cmp::{max, min};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use tracing::error;

/// Ibf consists of multiple boxes, this constant specified the number of unique boxes
/// into which each element is inserted into IBF. According to https://www.ics.uci.edu/~eppstein/pubs/EppGooUye-SIGCOMM-11.pdf
/// either number 3 or 4 is optimal.
const NUM_HASHES: usize = 3;

/// IbfElem represents a simple box, which may contain 0, 1 or more elements.
/// Each box stores xor of elements inserted inside the box, and xor of their hashes.
///
/// The box considered to be empty if both xor of elements inserted and their hashes is equal to 0.
/// To check whenever only one element is inside the box, we can check whenever hash of `xor_element`
/// is equal to `xor_hash`.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, Default)]
pub struct IbfBox {
    xor_elem: u64,
    xor_hash: u64,
}

impl IbfBox {
    fn new(elem: u64, hash: u64) -> IbfBox {
        IbfBox { xor_elem: elem, xor_hash: hash }
    }

    fn merge(&mut self, rhs: &IbfBox) {
        self.xor_elem ^= rhs.xor_elem;
        self.xor_hash ^= rhs.xor_hash;
    }
}

type IbfHasher = DefaultHasher;

/// Data structure representing Inverse Bloom Filter (IBF). It's used to store elements of a set,
/// in the current implementation they are of type 'u64', but it could be extended. Each `IbfSet`
/// is represented by a list `data` of boxes. Each box contains xor of elements inserted inside given
/// box and xor of hashes of elements inserted into box.
///
/// This data structure has a few important properties:
/// - given two IBFs each representing set of numbers. If you merge them together you get new IBF
/// representing set, which is a symmetric difference of those two sets.
/// - given IBF with size of `n`, you are able to recover the elements put into the set,
/// as long as the number of elements added is less than around 50%-60% for sufficiently large `n`.
///
///
/// https://www.ics.uci.edu/~eppstein/pubs/EppGooUye-SIGCOMM-11.pdf
#[derive(Clone)]
pub struct Ibf {
    /// Defines length of Ibf, which is 2^k + NUM_HASHES - 1
    k: i32,
    /// Vector containing all elements of IBF. Each one has xor of values stored, and xor of hashes stored.
    pub data: Vec<IbfBox>,
    /// Hashing object used to generate pseudo-random number
    hasher: IbfHasher,
    /// Hash seed used to generate IBF
    pub seed: u64,
}

impl Ibf {
    pub fn new(capacity: usize, seed: u64) -> Self {
        let k = Ibf::calculate_k(capacity);
        let new_capacity = (1 << k) + NUM_HASHES - 1;

        let mut hasher = IbfHasher::default();
        hasher.write_u64(seed);
        Self { data: vec![IbfBox::default(); new_capacity], hasher, k, seed }
    }

    /// Create Ibf from vector of elements and seed
    pub fn from_vec(data: &[IbfBox], seed: u64) -> Self {
        let k = Ibf::calculate_k(data.len());

        let mut hasher = IbfHasher::default();
        hasher.write_u64(seed);
        Self { data: data.into(), hasher, k, seed }
    }

    /// Calculate minimum parameter 'k', such that given IBF has at least 'capacity` elements.
    fn calculate_k(capacity: usize) -> i32 {
        let mut k = 0;
        // In order make computation of indexes not require doing division, we allocate
        // 2^k + NUM_HASHES - 1 hashes. This allows us to use compute indexes using bits.
        while (1 << k) + NUM_HASHES - 1 < capacity {
            k += 1;
        }
        k
    }

    /// Add element to the set. It's up to the caller to make sure this method is called only if
    /// the item is not in the set.
    pub fn add(&mut self, elem: u64) {
        self.insert(elem)
    }

    /// Remove element from the set. It's up to the caller to make sure this method is called only
    /// if the item is in the set.
    /// It's also worth noting that add/remove functions are identical.
    pub fn remove(&mut self, elem: u64) {
        self.insert(elem)
    }

    /// Compute hash of element
    fn compute_hash(&self, elem: u64) -> u64 {
        let mut h = self.hasher.clone();
        h.write_u64(elem);
        h.finish()
    }

    /// Add element to IBF, which involves updating values of 3 boxes.
    fn insert(&mut self, elem: u64) {
        self.insert_value(elem);
    }

    /// Merge two sets together, the result will be IbfSet representing symmetric difference
    /// between those sets.
    pub fn merge(&mut self, rhs_data: &[IbfBox], rhs_seed: u64) -> bool {
        if self.data.len() != rhs_data.len() || self.seed != rhs_seed {
            error!(target: "network",
                "Failed to merge len: {} {} seed: {} {}",
                self.data.len(),
                rhs_data.len(),
                self.seed,
                rhs_seed
            );
            return false;
        }
        for (lhs, rhs) in self.data.iter_mut().zip(rhs_data) {
            lhs.merge(rhs)
        }
        true
    }

    #[cfg(test)]
    fn recover(&mut self) -> Result<Vec<u64>, &'static str> {
        let (result, difference) = self.try_recover();

        if difference != 0 {
            for i in 0..self.data.len() {
                if self.data[i].xor_elem != 0 {
                    println!(
                        "{} {:?} {}",
                        i,
                        self.data[i],
                        self.compute_hash(self.data[i].xor_elem)
                    );
                }
            }
            return Err("unable to recover result");
        }
        Ok(result)
    }

    /// Try to recover elements inserted into IBF.
    ///
    /// Returns list of recovered elements and number of boxes that were not recoverable.
    pub fn try_recover(&mut self) -> (Vec<u64>, u64) {
        let mut result = Vec::with_capacity(self.data.len());
        let mut to_check = Vec::with_capacity(self.data.len());
        for i in 0..self.data.len() {
            to_check.push(i);

            while let Some(i) = to_check.pop() {
                let elem = self.data[i].xor_elem;
                if elem == 0 && self.data[i].xor_hash == 0 {
                    continue;
                }
                let elem_hash = self.compute_hash(elem);
                if elem_hash != self.data[i].xor_hash {
                    continue;
                }

                result.push(elem);
                self.remove_element_and_add_recovered_items_to_queue(
                    elem,
                    elem_hash,
                    &mut to_check,
                );
            }
        }
        let elems_that_differ = self.data.iter().filter(|it| it.xor_elem != 0).count() as u64;
        (result, elems_that_differ)
    }

    /// For given hash, generate list of indexes, where given element should be inserted.
    fn generate_idx(&mut self, elem_hash: u64) -> [usize; NUM_HASHES] {
        let mask = (1 << self.k) - 1;
        let pos0 = elem_hash & mask;
        let mut pos1 = (elem_hash >> self.k) & mask;
        let mut pos2 = (elem_hash >> (2 * self.k)) & mask;
        if pos1 >= pos0 {
            pos1 = (pos1 + 1) & mask;
        }
        if pos2 >= min(pos0, pos1) {
            pos2 = (pos2 + 1) & mask;
        }
        if pos2 >= max(pos0, pos1) {
            pos2 = (pos2 + 1) & mask;
        }
        [pos0 as usize, pos1 as usize, pos2 as usize]
    }

    /// Remove element from IBF, and add elements that were recovered to queue.
    fn remove_element_and_add_recovered_items_to_queue(
        &mut self,
        elem: u64,
        elem_hash: u64,
        queue: &mut Vec<usize>,
    ) {
        let pos_list = self.generate_idx(elem_hash);

        for pos in pos_list {
            self.data[pos].merge(&IbfBox::new(elem, elem_hash));
            queue.push(pos);
        }
    }

    /// Insert value into IBF
    fn insert_value(&mut self, elem: u64) {
        let elem_hash = self.compute_hash(elem);
        let pos_list = self.generate_idx(elem_hash);

        for pos in pos_list {
            self.data[pos].merge(&IbfBox::new(elem, elem_hash));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::routing::ibf::Ibf;

    fn create_blt(elements: impl IntoIterator<Item = u64>, capacity: usize) -> Ibf {
        let mut sketch = Ibf::new(capacity, 0);
        for item in elements.into_iter() {
            sketch.add(item);
        }
        sketch
    }

    #[test]
    fn create_blt_test() {
        let set = 1_000_000_300_000_u64..1_000_000_301_000_u64;

        assert_eq!(1000, create_blt(set, 2048).recover().unwrap().len())
    }
}
