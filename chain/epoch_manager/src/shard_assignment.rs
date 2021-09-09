use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::Balance;
use near_primitives::utils::min_heap::MinHeap;
use std::cmp;

/// Assign chunk producers (a.k.a validators) to shards. The i-th element
/// of the output corresponds to the validators assigned to the i-th shard.
/// This function ensures that every shard has at least `min_validators_per_shard`
/// assigned to it, and attempts to balance the stakes between shards (keep the total
/// stake assigned to each shard approximately equal). This function performs
/// best when the number of chunk producers is greater than
/// `num_shards * min_validators_per_shard`.
pub fn assign_shards<T: HasStake + Eq + Clone>(
    chunk_producers: Vec<T>,
    num_shards: usize,
    min_validators_per_shard: usize,
) -> Result<Vec<Vec<T>>, NotEnoughValidators> {
    // Initially, sort by number of validators, then total stake
    // (i.e. favour filling under-occupied shards first).
    let mut shard_validator_heap: MinHeap<(usize, Balance, ShardId)> =
        (0..num_shards).map(|s| (0, 0, s)).collect();

    let num_chunk_producers = chunk_producers.len();
    if num_chunk_producers < min_validators_per_shard {
        // Each shard must have `min_validators_per_shard` distinct validators,
        // however there are not that many, so we cannot proceed.
        return Err(NotEnoughValidators);
    }
    let required_validator_count =
        cmp::max(num_chunk_producers, num_shards * min_validators_per_shard);

    let mut result: Vec<Vec<T>> = (0..num_shards).map(|_| Vec::new()).collect();

    // Place validators into shards while there are still some without the
    // minimum required number.
    if required_validator_count == num_chunk_producers {
        // In this case there are at least enough chunk producers that no one will need
        // to be assigned to multiple shards, so we don't need to worry about that case.
        let mut cp_iter = chunk_producers.into_iter();

        // First we assign one validator to each shard, until all have the minimum number
        while shard_validator_heap.peek().unwrap().0 < min_validators_per_shard {
            let cp = cp_iter
                .next()
                .expect("cp_iter should contain enough elements to minimally fill each shard");
            let (least_validator_count, shard_stake, shard_id) =
                shard_validator_heap.pop().expect("shard_index should never be empty");

            shard_validator_heap.push((
                least_validator_count + 1,
                shard_stake + cp.get_stake(),
                shard_id,
            ));
            result[shard_id].push(cp);
        }

        let mut cp_iter = cp_iter.peekable();
        if cp_iter.peek().is_some() {
            // If there are still validators left to assign once the minimum is reached then
            // we can change priorities to try and balance the total stake in each shard

            // re-index shards to favour lowest stake first
            let mut shard_index: MinHeap<(Balance, usize, ShardId)> = shard_validator_heap
                .into_iter()
                .map(|(count, stake, shard_id)| (stake, count, shard_id))
                .collect();

            for cp in cp_iter {
                let (least_stake, least_validator_count, shard_id) =
                    shard_index.pop().expect("shard_index should never be empty");
                shard_index.push((
                    least_stake + cp.get_stake(),
                    least_validator_count + 1,
                    shard_id,
                ));
                result[shard_id].push(cp);
            }
        }
    } else {
        // In this case there are not enough validators to fill all shards without repeats,
        // so we use a more complex assignment function.
        let mut cp_iter =
            chunk_producers.into_iter().cycle().enumerate().take(required_validator_count);

        // This function assigns validators until all shards are filled. Each validator may
        // be assigned to multiple shards. This function does not attempt to balance stakes between
        // shards because it is expected that in production there will always be many more chunk
        // producers than shards, so this case will never come up and thus the algorithm does not
        // need to be tuned for this situation.
        assign_with_possible_repeats(
            &mut shard_validator_heap,
            &mut result,
            &mut cp_iter,
            num_shards,
            min_validators_per_shard,
            num_chunk_producers,
        );
    }

    Ok(result)
}

fn assign_with_possible_repeats<T: HasStake + Eq, I: Iterator<Item = (usize, T)>>(
    shard_index: &mut MinHeap<(usize, Balance, ShardId)>,
    result: &mut Vec<Vec<T>>,
    cp_iter: &mut I,
    num_shards: usize,
    min_validators_per_shard: usize,
    num_chunk_producers: usize,
) {
    let mut buffer = Vec::with_capacity(num_shards);

    while shard_index.peek().unwrap().0 < min_validators_per_shard {
        let (assignment_index, cp) = cp_iter
            .next()
            .expect("cp_iter should contain enough elements to minimally fill each shard");
        let (least_validator_count, shard_stake, shard_id) =
            shard_index.pop().expect("shard_index should never be empty");

        if assignment_index < num_chunk_producers {
            // no need to worry about duplicates yet; still on first pass through validators
            shard_index.push((least_validator_count + 1, shard_stake + cp.get_stake(), shard_id));
            result[shard_id].push(cp);
        } else if result[shard_id].contains(&cp) {
            // `cp` is already assigned to this shard, need to assign elsewhere

            // `buffer` tracks shards `cp` is already in, these will need to be pushed back into
            // shard_index when we eventually assign `cp`.
            buffer.push((least_validator_count, shard_stake, shard_id));
            loop {
                // We can still expect that there exists a shard cp has not been assigned to
                // because we check above that there are at least `min_validators_per_shard` distinct
                // chunk producers. This means the worst case scenario is in the end every chunk
                // producer is assigned to every shard, in which case we would not be trying to
                // assign a chunk producer right now.
                let (least_validator_count, shard_stake, shard_id) =
                    shard_index.pop().expect("shard_index should never be empty");
                if result[shard_id].contains(&cp) {
                    buffer.push((least_validator_count, shard_stake, shard_id))
                } else {
                    shard_index.push((
                        least_validator_count + 1,
                        shard_stake + cp.get_stake(),
                        shard_id,
                    ));
                    result[shard_id].push(cp);
                    break;
                }
            }
            for tuple in buffer.drain(..) {
                shard_index.push(tuple);
            }
        } else {
            shard_index.push((least_validator_count + 1, shard_stake + cp.get_stake(), shard_id));
            result[shard_id].push(cp);
        }
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

type ShardId = usize;

#[cfg(test)]
mod tests {
    use super::{assign_shards, HasStake};
    use near_primitives::types::Balance;
    use std::cmp;
    use std::collections::HashSet;

    const EXPONENTIAL_STAKES: [Balance; 12] = [100, 90, 81, 73, 66, 59, 53, 48, 43, 39, 35, 31];

    #[test]
    fn test_exponential_distribution_few_shards() {
        // algorithm works well when there are few shards relative to the number of chunk producers
        test_exponential_distribution_common(3, 3);
    }

    #[test]
    fn test_exponential_distribution_several_shards() {
        // algorithm performs less well when there are more shards
        test_exponential_distribution_common(6, 13);
    }

    #[test]
    fn test_exponential_distribution_many_shards() {
        // algorithm performs even worse when there are many shards
        test_exponential_distribution_common(24, 41);
    }

    #[test]
    fn test_not_enough_validators() {
        let stakes = &[100];
        let chunk_producers = make_validators(stakes);
        let num_shards = 1;
        let min_validators_per_shard = 3; // one validator cannot fill 3 slots
        let result = assign_shards(chunk_producers, num_shards, min_validators_per_shard);
        assert!(result.is_err())
    }

    #[test]
    fn test_step_distribution_shards() {
        let num_shards = 2;
        let min_validators_per_shard = 2;
        // Note: Could divide as {{100} {10, 10, 10, 10, 10, 10, 10, 10, 10, 10}}
        // the stakes are equal with this assignment, but this would not result in
        // the minimum of 2 validators in the first shard
        let stakes = &[100, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10];
        let chunk_producers = make_validators(stakes);

        let assignment =
            assign_shards(chunk_producers, num_shards, min_validators_per_shard).unwrap();

        // The algorithm ensures the minimum number of validators is present
        // in each shard, even if it makes the stakes more uneven.
        let shard_0 = assignment.first().unwrap();
        assert_eq!(shard_0.len(), min_validators_per_shard);
        let stake_0 = shard_0.iter().map(|cp| cp.stake).sum::<Balance>();
        assert_eq!(stake_0, 110);

        let shard_1 = assignment.last().unwrap();
        assert_eq!(shard_1.len(), stakes.len() - min_validators_per_shard);
        let stake_1 = shard_1.iter().map(|cp| cp.stake).sum::<Balance>();
        assert_eq!(stake_1, 90);
    }

    fn test_exponential_distribution_common(num_shards: usize, diff_tolerance: i128) {
        let stakes = &EXPONENTIAL_STAKES;
        let chunk_producers = make_validators(stakes);
        let min_validators_per_shard = 2;

        let validators_per_shard =
            cmp::max(chunk_producers.len() / num_shards, min_validators_per_shard);
        let average_stake_per_shard = (validators_per_shard as Balance)
            * stakes.iter().sum::<Balance>()
            / (stakes.len() as Balance);
        let assignment = assign_shards(chunk_producers, num_shards, min_validators_per_shard)
            .expect("There should have been enough validators");

        // validator distribution should be even
        assert!(assignment.iter().all(|cps| cps.len() == validators_per_shard));

        // no validator should be assigned to the same shard more than once
        assert!(assignment.iter().all(|cps| cps.iter().collect::<HashSet<_>>().len() == cps.len()));

        // stake distribution should be even
        assert!(assignment.iter().all(|cps| {
            let shard_stake = cps.iter().map(|cp| cp.stake).sum::<Balance>();
            let stake_diff: i128 = (shard_stake as i128) - (average_stake_per_shard as i128);
            stake_diff.abs() < diff_tolerance
        }));
    }

    #[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
    struct ValidatorStake {
        stake: Balance,
        id: usize,
    }

    impl HasStake for ValidatorStake {
        fn get_stake(&self) -> Balance {
            self.stake
        }
    }

    fn make_validators(stakes: &[Balance]) -> Vec<ValidatorStake> {
        stakes.iter().copied().enumerate().map(|(id, stake)| ValidatorStake { stake, id }).collect()
    }
}
