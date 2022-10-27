use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{Balance, NumShards, ShardId};
use near_primitives::utils::min_heap::{MinHeap, PeekMut};

/// Assign chunk producers (a.k.a. validators) to shards.  The i-th element
/// of the output corresponds to the validators assigned to the i-th shard.
///
/// This function ensures that every shard has at least `min_validators_per_shard`
/// assigned to it, and attempts to balance the stakes between shards (keep the total
/// stake assigned to each shard approximately equal).
///
/// This function performs best when the number of chunk producers is greater or
/// equal than `num_shards * min_validators_per_shard` in which case each chunk
/// producer will be assigned to a single shard.  If there are fewer producers,
/// some of them will be assigned to multiple shards.
///
/// Panics if chunk_producers vector is not sorted in descending order by
/// producer’s stake.
pub fn assign_shards<T: HasStake + Eq + Clone>(
    chunk_producers: Vec<T>,
    num_shards: NumShards,
    min_validators_per_shard: usize,
) -> Result<Vec<Vec<T>>, NotEnoughValidators> {
    for (idx, pair) in chunk_producers.windows(2).enumerate() {
        assert!(
            pair[0].get_stake() >= pair[1].get_stake(),
            "chunk_producers isn’t sorted; first discrepancy at {}",
            idx
        );
    }

    // If there’s not enough chunk producers to fill up a single shard there’s
    // nothing we can do.  Return with an error.
    let num_chunk_producers = chunk_producers.len();
    if num_chunk_producers < min_validators_per_shard {
        return Err(NotEnoughValidators);
    }

    let mut result: Vec<Vec<T>> = (0..num_shards).map(|_| Vec::new()).collect();

    // Initially, sort by number of validators first so we fill shards up.
    let mut shard_index: MinHeap<(usize, Balance, ShardId)> =
        (0..num_shards).map(|s| (0, 0, s)).collect();

    // First, distribute chunk producers until all shards have at least the
    // minimum requested number.  If there are not enough validators to satisfy
    // that requirement, assign some of the validators to multiple shards.
    let mut chunk_producers = chunk_producers.into_iter().enumerate().cycle();
    assign_with_possible_repeats(
        &mut shard_index,
        &mut result,
        &mut chunk_producers,
        min_validators_per_shard,
    );

    // Second, if there are any unassigned chunk producers left, distribute them
    // between shards trying to balance total stake.
    let remaining_producers =
        num_chunk_producers.saturating_sub(num_shards as usize * min_validators_per_shard);
    if remaining_producers > 0 {
        // Re-index shards to favour lowest stake first.
        let mut shard_index: MinHeap<(Balance, usize, ShardId)> = shard_index
            .into_iter()
            .map(|(count, stake, shard_id)| (stake, count, shard_id))
            .collect();

        for (_, cp) in chunk_producers.take(remaining_producers) {
            let (least_stake, least_validator_count, shard_id) =
                shard_index.pop().expect("shard_index should never be empty");
            shard_index.push((least_stake + cp.get_stake(), least_validator_count + 1, shard_id));
            result[usize::try_from(shard_id).unwrap()].push(cp);
        }
    }

    Ok(result)
}

fn assign_with_possible_repeats<T: HasStake + Eq, I: Iterator<Item = (usize, T)>>(
    shard_index: &mut MinHeap<(usize, Balance, ShardId)>,
    result: &mut Vec<Vec<T>>,
    cp_iter: &mut I,
    min_validators_per_shard: usize,
) {
    let mut buffer = Vec::with_capacity(shard_index.len());
    // Stores (shard_id, cp_index) meaning that cp at cp_index has already been
    // added to shard shard_id.  Used to make sure we don’t add a cp to the same
    // shard multiple times.
    let mut seen = std::collections::HashSet::<(ShardId, usize)>::with_capacity(
        result.len() * min_validators_per_shard,
    );

    while shard_index.peek().unwrap().0 < min_validators_per_shard {
        // cp_iter is an infinite cycle iterator so getting next value can never
        // fail.  cp_index is index of each element in the iterator but the
        // indexing is done before cycling thus the same cp always gets the same
        // cp_index.
        let (cp_index, cp) = cp_iter.next().unwrap();
        // Decide which shard to assign this chunk producer to.  We mustn’t
        // assign producers to a single shard multiple times.
        loop {
            match shard_index.peek_mut() {
                None => {
                    // No shards left which don’t already contain this chunk
                    // producer.  Skip it and move to another producer.
                    break;
                }
                Some(top) if top.0 >= min_validators_per_shard => {
                    // `shard_index` is sorted by number of chunk producers,
                    // thus all remaining shards have min_validators_per_shard
                    // producers already assigned to them.  Don’t assign current
                    // one to any shard and move to next cp.
                    break;
                }
                Some(mut top) if seen.insert((top.2, cp_index)) => {
                    // Chunk producer is not yet assigned to the shard and the
                    // shard still needs more producers.  Assign `cp` to it and
                    // move to next one.
                    top.0 += 1;
                    top.1 += cp.get_stake();
                    result[usize::try_from(top.2).unwrap()].push(cp);
                    break;
                }
                Some(top) => {
                    // This chunk producer is already assigned to this shard.
                    // Pop the shard from the heap for now and try assigning the
                    // producer to the next shard.  (We’ll look back at the
                    // shard once we figure out what to do with current `cp`).
                    buffer.push(PeekMut::pop(top));
                }
            }
        }
        // Any shards we skipped over (because `cp` was already assigned to
        // them) need to be put back into the heap.
        shard_index.extend(buffer.drain(..));
    }
}

/// Marker struct to communicate the error where you try to assign validators to shards
/// and there are not enough to even meet the minimum per shard.
#[derive(Debug)]
pub struct NotEnoughValidators;

pub trait HasStake {
    fn get_stake(&self) -> Balance;
}

impl HasStake for ValidatorStake {
    fn get_stake(&self) -> Balance {
        self.stake()
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::types::{Balance, NumShards};
    use std::collections::HashSet;

    const EXPONENTIAL_STAKES: [Balance; 12] = [100, 90, 81, 73, 66, 59, 53, 48, 43, 39, 35, 31];

    #[test]
    fn test_exponential_distribution_few_shards() {
        // algorithm works well when there are few shards relative to the number of chunk producers
        test_distribution_common(&EXPONENTIAL_STAKES, 3, 3);
    }

    #[test]
    fn test_exponential_distribution_several_shards() {
        // algorithm performs less well when there are more shards
        test_distribution_common(&EXPONENTIAL_STAKES, 6, 13);
    }

    #[test]
    fn test_exponential_distribution_many_shards() {
        // algorithm performs even worse when there are many shards
        test_distribution_common(&EXPONENTIAL_STAKES, 24, 41);
    }

    /// Tests situation where assigning with possible repeats encounters a state
    /// in which the same validator would end up assigned to the same shard
    /// twice.
    ///
    /// The way this scenario works is as follows.  There are three validators
    /// [100, 90, 81] and they are distributed among two shards.  First the code
    /// will assign 100 to shard 0 and then 90 to shard 1.  At that point, both
    /// shards will have one validator but shard 1 will have less total stake so
    /// the code will assign validator 81 to it.  In the last step, shard 0 will
    /// have only one validator so the code will try to assign validator 100 to
    /// it.  However, that validator is already assigned to that shard so the
    /// algorithm will need to discard it and try another one.
    #[test]
    fn test_duplicate_validator() {
        test_distribution_common(&EXPONENTIAL_STAKES[..3], 2, 11);
    }

    /// Tests behaviour when there’s not enough validators to fill required
    /// minimum number of spots per shard.
    #[test]
    fn test_not_enough_validators() {
        // One validator cannot fill three slots.
        assert!(assign_shards(&[100], 1, 3).is_err())
    }

    #[test]
    fn test_step_distribution_shards() {
        let num_shards = 2;
        let min_validators_per_shard = 2;
        // Note: Could divide as {{100} {10, 10, 10, 10, 10, 10, 10, 10, 10, 10}}
        // the stakes are equal with this assignment, but this would not result in
        // the minimum of 2 validators in the first shard
        let stakes = &[100, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10];
        let assignment = assign_shards(stakes, num_shards, min_validators_per_shard).unwrap();

        // The algorithm ensures the minimum number of validators is present
        // in each shard, even if it makes the stakes more uneven.
        assert_eq!(
            &[(min_validators_per_shard, 110), (stakes.len() - min_validators_per_shard, 90)],
            &assignment[..]
        );
    }

    /// Calls [`super::assign_shards`] and performs basic validation of the
    /// result.  Returns sorted and aggregated data in the form of a vector of
    /// `(count, stake)` tuples where first element is number of chunk producers
    /// in a shard and second is total stake assigned to that shard.
    fn assign_shards(
        stakes: &[Balance],
        num_shards: NumShards,
        min_validators_per_shard: usize,
    ) -> Result<Vec<(usize, Balance)>, super::NotEnoughValidators> {
        let chunk_producers = stakes.iter().copied().enumerate().collect();
        let assignments =
            super::assign_shards(chunk_producers, num_shards, min_validators_per_shard)?;

        // All chunk producers must be assigned at least once.  Furthermore, no
        // chunk producer can be assigned to more than one shard than chunk
        // producer with lowest number of assignments.
        let mut chunk_producers_counts = vec![0; stakes.len()];
        for cp in assignments.iter().flat_map(|shard| shard.iter()) {
            chunk_producers_counts[cp.0] += 1;
        }
        let min = chunk_producers_counts.iter().copied().min().unwrap();
        let max = chunk_producers_counts.iter().copied().max().unwrap();
        assert!(0 < min && max <= min + 1);

        let mut assignments = assignments
            .into_iter()
            .enumerate()
            .map(|(shard_id, cps)| {
                // All shards must have at least min_validators_per_shard validators.
                assert!(
                    cps.len() >= min_validators_per_shard,
                    "Shard {} has only {} chunk producers; expected at least {}",
                    shard_id,
                    cps.len(),
                    min_validators_per_shard
                );
                // No validator can exist twice in the same shard.
                assert_eq!(
                    cps.len(),
                    cps.iter().map(|cp| cp.0).collect::<HashSet<_>>().len(),
                    "Shard {} contains duplicate chunk producers: {:?}",
                    shard_id,
                    cps
                );
                // If all is good, aggregate as (cps_count, total_stake) pair.
                (cps.len(), cps.iter().map(|cp| cp.1).sum())
            })
            .collect::<Vec<_>>();
        assignments.sort();
        Ok(assignments)
    }

    fn test_distribution_common(stakes: &[Balance], num_shards: NumShards, diff_tolerance: i128) {
        let min_validators_per_shard = 2;
        let validators_per_shard =
            std::cmp::max(stakes.len() / (num_shards as usize), min_validators_per_shard);
        let average_stake_per_shard = (validators_per_shard as Balance)
            * stakes.iter().sum::<Balance>()
            / (stakes.len() as Balance);
        let assignment = assign_shards(stakes, num_shards, min_validators_per_shard)
            .expect("There should have been enough validators");
        for (shard_id, &cps) in assignment.iter().enumerate() {
            // Validator distribution should be even.
            assert_eq!(
                validators_per_shard, cps.0,
                "Shard {} has {} validators, expected {}",
                shard_id, cps.0, validators_per_shard
            );

            // Stake distribution should be even
            let diff = (cps.1 as i128) - (average_stake_per_shard as i128);
            assert!(
                diff.abs() < diff_tolerance,
                "Shard {}'s stake {} is {} away from average; expected less than {} away",
                shard_id,
                cps.1,
                diff.abs(),
                diff_tolerance
            );
        }
    }

    impl super::HasStake for (usize, Balance) {
        fn get_stake(&self) -> Balance {
            self.1
        }
    }
}
