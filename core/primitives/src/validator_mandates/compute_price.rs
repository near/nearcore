use {
    super::ValidatorMandatesConfig,
    near_primitives_core::types::Balance,
    std::cmp::{min, Ordering},
};

/// Given the stakes for the validators and the target number of mandates to have,
/// this function computes the mandate price to use. It works by using a binary search.
pub fn compute_mandate_price(config: ValidatorMandatesConfig, stakes: &[Balance]) -> Balance {
    let ValidatorMandatesConfig { target_mandates_per_shard, num_shards } = config;
    let total_stake = saturating_sum(stakes.iter().copied());

    // The target number of mandates cannot be larger than the total amount of stake.
    // In production the total stake is _much_ higher than
    // `num_shards * target_mandates_per_shard`, but in tests validators are given
    // low staked numbers, so we need to have this condition in place.
    let target_mandates: u128 =
        min(num_shards.saturating_mul(target_mandates_per_shard) as u128, total_stake);

    // Note: the reason to have the binary search look for the largest mandate price
    // which obtains the target number of whole mandates is because the largest value
    // minimizes the partial mandates. This can be seen as follows:
    // Let `s_i` be the ith stake, `T` be the total stake and `m` be the mandate price.
    // T / m = \sum (s_i / m) = \sum q_i + \sum r_i
    // ==> \sum q_i = (T / m) - \sum r_i     [Eq. (1)]
    // where `s_i = m * q_i + r_i` is obtained by the Euclidean algorithm.
    // Notice that the LHS of (1) is the number of whole mandates, which we
    // are assuming is equal to our target value for some range of `m` values.
    // When we use a larger `m` value, `T / m` decreases but we need the LHS
    // to remain constant, therefore `\sum r_i` must also decrease.
    binary_search(1, total_stake, target_mandates, |mandate_price| {
        saturating_sum(stakes.iter().map(|s| *s / mandate_price))
    })
}

/// Assume `f` is a non-increasing function (f(x) <= f(y) if x > y) and `low < high`.
/// This function uses a binary search to attempt to find the largest input, `x` such that
/// `f(x) == target`, `low <= x` and `x <= high`.
/// If there is no such `x` then it will return the unique input `x` such that
/// `f(x) > target`, `f(x + 1) < target`, `low <= x` and `x <= high`.
fn binary_search<F>(low: Balance, high: Balance, target: u128, f: F) -> Balance
where
    F: Fn(Balance) -> u128,
{
    debug_assert!(low < high);

    let mut low = low;
    let mut high = high;

    if f(low) == target {
        return highest_exact(low, high, target, f);
    } else if f(high) == target {
        // No need to use `highest_exact` here because we are already at the upper bound.
        return high;
    }

    while high - low > 1 {
        let mid = low + (high - low) / 2;
        let f_mid = f(mid);

        match f_mid.cmp(&target) {
            Ordering::Equal => return highest_exact(mid, high, target, f),
            Ordering::Less => high = mid,
            Ordering::Greater => low = mid,
        }
    }

    // No exact answer, return best price which gives an answer greater than
    // `target_mandates` (which is `low` because `count_whole_mandates` is a non-increasing function).
    low
}

/// Assume `f` is a non-increasing function (f(x) <= f(y) if x > y), `f(low) == target`
/// and `f(high) < target`. This function uses a binary search to find the largest input, `x`
/// such that `f(x) == target`.
fn highest_exact<F>(low: Balance, high: Balance, target: u128, f: F) -> Balance
where
    F: Fn(Balance) -> u128,
{
    debug_assert!(low < high);
    debug_assert_eq!(f(low), target);
    debug_assert!(f(high) < target);

    let mut low = low;
    let mut high = high;

    while high - low > 1 {
        let mid = low + (high - low) / 2;
        let f_mid = f(mid);

        match f_mid.cmp(&target) {
            Ordering::Equal => low = mid,
            Ordering::Less => high = mid,
            Ordering::Greater => unreachable!("Given function must be non-increasing"),
        }
    }

    low
}

fn saturating_sum<I: Iterator<Item = u128>>(iter: I) -> u128 {
    iter.fold(0, |acc, x| acc.saturating_add(x))
}

#[cfg(test)]
mod tests {
    use rand::{Rng, SeedableRng};

    use super::*;

    // Test case where the target number of mandates is larger than the total stake.
    // This should never happen in production, but nearcore tests sometimes have
    // low stake.
    #[test]
    fn test_small_total_stake() {
        let stakes = [100_u128; 1];
        let num_shards = 1;
        let target_mandates_per_shard = 1000;
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);

        assert_eq!(compute_mandate_price(config, &stakes), 1);
    }

    // Test cases where all stakes are equal.
    #[test]
    fn test_constant_dist() {
        let stakes = [11_u128; 13];
        let num_shards = 1;
        let target_mandates_per_shard = stakes.len();
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);

        // There are enough validators to have 1:1 correspondence with mandates.
        assert_eq!(compute_mandate_price(config, &stakes), stakes[0]);

        let target_mandates_per_shard = 2 * stakes.len();
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);

        // Now each validator needs to take two mandates.
        assert_eq!(compute_mandate_price(config, &stakes), stakes[0] / 2);

        let target_mandates_per_shard = stakes.len() - 1;
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);

        // Now there are more validators than we need, but
        // the mandate price still doesn't go below the common stake.
        assert_eq!(compute_mandate_price(config, &stakes), stakes[0]);
    }

    // Test cases where the stake distribution is a step function.
    #[test]
    fn test_step_dist() {
        let stakes = {
            let mut buf = [11_u128; 13];
            let n = buf.len() / 2;
            for s in buf.iter_mut().take(n) {
                *s *= 5;
            }
            buf
        };
        let num_shards = 1;
        let target_mandates_per_shard = stakes.len();
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);

        // Computed price gives whole number of seats close to the target number
        let price = compute_mandate_price(config, &stakes);
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard + 5);

        let target_mandates_per_shard = 2 * stakes.len();
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, &stakes);
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard + 11);

        let target_mandates_per_shard = stakes.len() / 2;
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, &stakes);
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard);
    }

    // Test cases where the stake distribution is exponential.
    #[test]
    fn test_exp_dist() {
        let stakes = {
            let mut buf = vec![1_000_000_000_u128; 210];
            let mut last_stake = buf[0];
            for s in buf.iter_mut().skip(1) {
                last_stake = last_stake * 97 / 100;
                *s = last_stake;
            }
            buf
        };

        // This case is similar to the mainnet data.
        let num_shards = 6;
        let target_mandates_per_shard = 68;
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, &stakes);
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard * num_shards);

        let num_shards = 1;
        let target_mandates_per_shard = stakes.len();
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, &stakes);
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard);

        let target_mandates_per_shard = stakes.len() * 2;
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, &stakes);
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard);

        let target_mandates_per_shard = stakes.len() / 2;
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, &stakes);
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard);
    }

    // Test cases where the stakes are chosen uniformly at random.
    #[test]
    fn test_rand_dist() {
        let stakes = {
            let mut stakes = vec![0_u128; 1000];
            let mut rng = rand::rngs::StdRng::seed_from_u64(0xdeadbeef);
            for s in stakes.iter_mut() {
                *s = rng.gen_range(1_u128..10_000u128);
            }
            stakes
        };

        let num_shards = 1;
        let target_mandates_per_shard = stakes.len();
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, &stakes);
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard + 3);

        let target_mandates_per_shard = 2 * stakes.len();
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, &stakes);
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard);

        let target_mandates_per_shard = stakes.len() / 2;
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, &stakes);
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard);
    }

    fn count_whole_mandates(stakes: &[u128], mandate_price: u128) -> usize {
        saturating_sum(stakes.iter().map(|s| *s / mandate_price)) as usize
    }
}
