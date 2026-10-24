//! What is specific to ECCs in the pool.

use crate::InsertTransactionResult;
use crate::grouped_pool::GroupedPool;
use crate::item::{GroupId, PoolItem};
use crate::types::PoolKey;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{EccTransaction, ValidatedEcc};
use std::collections::{HashMap, VecDeque};

/// The group of an ECC: the ECCs to one contract are in one group.
pub(crate) fn group_id(ecc: &EccTransaction) -> GroupId<'_> {
    GroupId::Ecc { contract_id: ecc.contract_id() }
}

impl PoolItem for ValidatedEcc {
    fn hash(&self) -> CryptoHash {
        self.get_hash()
    }

    fn size(&self) -> u64 {
        self.get_size()
    }

    fn group(&self) -> GroupId<'_> {
        group_id(self.to_ecc())
    }

    /// ECCs are taken in the order of arrival, so a new one goes to the back.
    fn add_to_group(group: &mut VecDeque<Self>, item: Self) {
        group.push_back(item);
    }

    /// ECCs have no order but the order of arrival, which is kept all the time.
    fn sort_group(_group: &mut VecDeque<Self>) {}
}

impl GroupedPool<ValidatedEcc> {
    /// Puts back ECCs that were taken out of the pool, e.g. because their chunk missed a block.
    /// The ECCs are expected in the order of arrival. They go ahead of everything waiting for
    /// their contract, including ECCs put back earlier, and keep their order among themselves.
    /// So when several batches are put back, the newest one has to go first.
    ///
    /// The ECCs are admitted one by one in this order. An ECC that does not fit is refused, and
    /// a smaller one after it may still fit. Returns the number of ECCs that were added or are
    /// already present.
    pub(crate) fn reintroduce_items(
        &mut self,
        eccs: impl IntoIterator<Item = ValidatedEcc>,
        size_limit: Option<u64>,
    ) -> usize {
        // For each group, how many ECCs of this batch were already put at its front.
        let mut put_back: HashMap<PoolKey, usize> = HashMap::new();
        eccs.into_iter()
            .map(|ecc| {
                self.insert_with(ecc, size_limit, |key, group, ecc| {
                    let behind = put_back.entry(key).or_default();
                    group.insert(*behind, ecc);
                    *behind += 1;
                })
            })
            .filter(|result| *result != InsertTransactionResult::NoSpaceLeft)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grouped_pool::{GroupedPoolIterator, PoolMetrics};
    use crate::metrics;
    use near_primitives::epoch_info::RngSeed;

    const TEST_SEED: RngSeed = [3; 32];

    fn new_pool() -> GroupedPool<ValidatedEcc> {
        let metrics =
            PoolMetrics::new(&metrics::ECC_POOL_COUNT, &metrics::ECC_POOL_SIZE, "test_ecc_pool");
        GroupedPool::new(TEST_SEED, metrics)
    }

    /// An ECC for `contract_id` with a payload of `payload_len` bytes, all equal to `tag`.
    fn ecc_with_payload(contract_id: &str, tag: u8, payload_len: usize) -> ValidatedEcc {
        let contract_id = contract_id.parse().unwrap();
        ValidatedEcc::new_for_test(EccTransaction::new(contract_id, vec![tag; payload_len]))
    }

    fn ecc(contract_id: &str, tag: u8) -> ValidatedEcc {
        ecc_with_payload(contract_id, tag, 8)
    }

    fn tag_of(ecc: &ValidatedEcc) -> u8 {
        ecc.payload()[0]
    }

    /// Takes everything out of the pool. Returns the tags of the ECCs of each group in the order
    /// they were taken.
    fn drain_groups(pool: &mut GroupedPool<ValidatedEcc>) -> Vec<Vec<u8>> {
        let mut groups = vec![];
        let mut iter = GroupedPoolIterator::new(pool);
        while let Some(group) = iter.next() {
            let mut tags = vec![];
            while let Some(ecc) = group.next() {
                tags.push(tag_of(&ecc));
            }
            groups.push(tags);
        }
        groups
    }

    fn insert_all(
        pool: &mut GroupedPool<ValidatedEcc>,
        eccs: impl IntoIterator<Item = ValidatedEcc>,
    ) {
        eccs.into_iter().for_each(|ecc| {
            assert_eq!(pool.insert(ecc, None), InsertTransactionResult::Success);
        });
    }

    #[test]
    fn test_eccs_come_out_in_one_group_per_contract_in_arrival_order() {
        let mut pool = new_pool();
        let eccs = [("a.near", 1), ("b.near", 1), ("a.near", 2), ("a.near", 3), ("b.near", 2)];
        insert_all(&mut pool, eccs.map(|(contract_id, tag)| ecc(contract_id, tag)));
        assert_eq!(pool.len(), 5);

        let mut groups = drain_groups(&mut pool);

        groups.sort();
        assert_eq!(groups, vec![vec![1, 2], vec![1, 2, 3]]);
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.size(), 0);
    }

    /// An ECC that was taken out of the pool and is put back comes out before the ECCs that
    /// arrived after it, including the ones that arrived while it was away.
    #[test]
    fn test_put_back_ecc_goes_to_front() {
        let mut pool = new_pool();
        insert_all(&mut pool, (1..=3).map(|tag| ecc("a.near", tag)));

        // Take the first ECC out of the pool, like a chunk producer packing it into a chunk.
        let taken = {
            let mut iter = GroupedPoolIterator::new(&mut pool);
            iter.next().unwrap().next().unwrap()
        };
        assert_eq!(tag_of(&taken), 1);
        assert_eq!(pool.len(), 2);
        insert_all(&mut pool, [ecc("a.near", 4)]);

        assert_eq!(pool.reintroduce_items([taken], None), 1);

        assert_eq!(drain_groups(&mut pool), vec![vec![1, 2, 3, 4]]);
    }

    /// ECCs put back in one batch keep their order among themselves.
    #[test]
    fn test_put_back_batch_keeps_arrival_order() {
        let mut pool = new_pool();
        insert_all(&mut pool, [ecc("a.near", 3), ecc("b.near", 2)]);

        let batch = [ecc("a.near", 1), ecc("b.near", 1), ecc("a.near", 2)];
        assert_eq!(pool.reintroduce_items(batch, None), 3);

        let mut groups = drain_groups(&mut pool);
        groups.sort();
        assert_eq!(groups, vec![vec![1, 2], vec![1, 2, 3]]);
    }

    /// ECCs of several blocks are put back one batch at a time, the newest block first, like a
    /// reorg does. They end up in the order of arrival.
    #[test]
    fn test_put_back_batches_newest_first_restore_arrival_order() {
        let mut pool = new_pool();
        insert_all(&mut pool, [ecc("a.near", 5), ecc("b.near", 3)]);

        let newer = [ecc("a.near", 3), ecc("a.near", 4), ecc("b.near", 2)];
        let older = [ecc("a.near", 1), ecc("a.near", 2), ecc("b.near", 1)];
        assert_eq!(pool.reintroduce_items(newer, None), 3);
        assert_eq!(pool.reintroduce_items(older, None), 3);

        let mut groups = drain_groups(&mut pool);
        groups.sort();
        assert_eq!(groups, vec![vec![1, 2, 3], vec![1, 2, 3, 4, 5]]);
    }

    /// When a batch does not fit, the earliest ECCs get the space.
    #[test]
    fn test_put_back_refuses_the_later_eccs_when_there_is_no_space() {
        let mut pool = new_pool();
        insert_all(&mut pool, [ecc("a.near", 9)]);
        let ecc_size = ecc("a.near", 1).get_size();

        let batch = [ecc("a.near", 1), ecc("a.near", 2), ecc("a.near", 3)];
        // There is room for the ECC that is waiting and two more.
        assert_eq!(pool.reintroduce_items(batch, Some(3 * ecc_size)), 2);

        assert_eq!(drain_groups(&mut pool), vec![vec![1, 2, 9]]);
    }

    #[test]
    fn test_duplicate_ecc() {
        let mut pool = new_pool();
        let first = ecc("a.near", 1);
        let size = first.get_size();
        assert_eq!(pool.insert(first.clone(), None), InsertTransactionResult::Success);

        // A duplicate is reported even if the pool is full.
        assert_eq!(pool.insert(first.clone(), Some(size)), InsertTransactionResult::Duplicate);
        assert_eq!(pool.reintroduce_items([first], Some(size)), 1);

        assert_eq!(pool.len(), 1);
        assert_eq!(pool.size(), size);
    }

    #[test]
    fn test_size_limit() {
        let mut pool = new_pool();
        let ecc_size = ecc_with_payload("a.near", 1, 100).get_size();
        let limit = Some(2 * ecc_size);
        for tag in 1..=2 {
            let ecc = ecc_with_payload("a.near", tag, 100);
            assert_eq!(pool.insert(ecc, limit), InsertTransactionResult::Success);
        }

        let refused = ecc_with_payload("b.near", 3, 100);
        assert_eq!(pool.insert(refused, limit), InsertTransactionResult::NoSpaceLeft);

        assert_eq!(pool.len(), 2);
        assert_eq!(pool.size(), 2 * ecc_size);
        assert_eq!(pool.group_count(), 1);
    }

    #[test]
    fn test_remove_items() {
        let mut pool = new_pool();
        let (a1, a2, b1) = (ecc("a.near", 1), ecc("a.near", 2), ecc("b.near", 1));
        insert_all(&mut pool, [a1.clone(), a2.clone(), b1.clone()]);

        // The last one was not in the pool at all.
        let not_in_pool = ecc("c.near", 1);
        pool.remove_items([&a1, &b1, &not_in_pool].map(|ecc| (ecc.hash(), ecc.group())));

        assert_eq!(pool.len(), 1);
        assert_eq!(pool.size(), a2.get_size());
        // The group of `b.near` is gone along with its last ECC.
        assert_eq!(pool.group_count(), 1);
        assert_eq!(drain_groups(&mut pool), vec![vec![2]]);
    }

    /// The size follows the ECCs through the iterator: what is taken out and not put back leaves
    /// the pool, what is not taken stays.
    #[test]
    fn test_size_follows_pool_iterator() {
        let mut pool = new_pool();
        let (a1, a2) = (ecc("a.near", 1), ecc("a.near", 2));
        insert_all(&mut pool, [a1.clone(), a2.clone()]);

        {
            let mut iter = GroupedPoolIterator::new(&mut pool);
            let group = iter.next().unwrap();
            assert_eq!(group.next().map(|ecc| ecc.get_hash()), Some(a1.get_hash()));
        }

        assert_eq!(pool.len(), 1);
        assert_eq!(pool.size(), a2.get_size());
        drain_groups(&mut pool);
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.size(), 0);
    }
}
