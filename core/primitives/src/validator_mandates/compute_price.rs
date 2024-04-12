use {
    super::ValidatorMandatesConfig,
    near_primitives_core::types::Balance,
    std::cmp::{min, Ordering},
};

/// Given the stakes for the validators and the target number of mandates to have,
/// this function computes the mandate price to use. It works by iterating a
/// function in an attempt to find its fixed point. This function is motived as follows:
/// Let the validator stakes be denoted by `s_i` a let `S = \sum_i s_i` be the total
/// stake. For a given mandate price `m` we can write each `s_i = m * q_i + r_i`
/// (by the Euclidean algorithm). Hence, the number of whole mandates created by
/// that price is equal to `\sum_i q_i`. If we set this number of whole mandates
/// equal to the target number `N` then substitute back in to the previous equations
/// we have `S = m * N + \sum_i r_i`. We can rearrange this to solve for `m`,
/// `m = (S - \sum_i r_i) / N`. Note that `r_i = a_i % m` so `m` is not truly
/// isolated, but rather the RHS is the expression we want to find the fixed point for.
pub fn compute_mandate_price<F, I>(config: ValidatorMandatesConfig, stakes: F) -> Balance
where
    I: Iterator<Item = Balance>,
    F: Fn() -> I,
{
    let ValidatorMandatesConfig { target_mandates_per_shard, num_shards } = config;
    let total_stake = saturating_sum(stakes());

    // The target number of mandates cannot be larger than the total amount of stake.
    // In production the total stake is _much_ higher than
    // `num_shards * target_mandates_per_shard`, but in tests validators are given
    // low staked numbers, so we need to have this condition in place.
    let target_mandates: u128 =
        min(num_shards.saturating_mul(target_mandates_per_shard) as u128, total_stake);

    let initial_price = total_stake / target_mandates;

    // Function to compute the new estimated mandate price as well as
    // evaluate the given mandate price.
    let f = |price: u128| {
        let mut whole_mandates = 0_u128;
        let mut remainders = 0_u128;
        for s in stakes() {
            whole_mandates = whole_mandates.saturating_add(s / price);
            remainders = remainders.saturating_add(s % price);
        }
        let updated_price = if total_stake > remainders {
            (total_stake - remainders) / target_mandates
        } else {
            // This is an alternate expression we can try to find a fixed point of.
            // We use it avoid making the next price equal to 0 (which is clearly incorrect).
            // It is derived from `S = m * N + \sum_i r_i` by dividing by `m` first then
            // isolating the `m` that appears on the LHS.
            let partial_mandates = remainders / price;
            total_stake / (target_mandates + partial_mandates)
        };
        let mandate_diff = if whole_mandates > target_mandates {
            whole_mandates - target_mandates
        } else {
            target_mandates - whole_mandates
        };
        (PriceResult { price, mandate_diff }, updated_price)
    };

    // Iterate the function 25 times
    let mut results = [PriceResult::default(); 25];
    let (result_0, mut price) = f(initial_price);
    results[0] = result_0;
    for result in results.iter_mut().skip(1) {
        let (output, next_price) = f(price);
        *result = output;
        price = next_price;
    }

    // Take the best result
    let result = results.iter().min().expect("results iter is non-empty");
    result.price
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
struct PriceResult {
    price: u128,
    mandate_diff: u128,
}

impl PartialOrd for PriceResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriceResult {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.mandate_diff.cmp(&other.mandate_diff) {
            Ordering::Equal => self.price.cmp(&other.price),
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
        }
    }
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

        assert_eq!(compute_mandate_price(config, || stakes.iter().copied()), 1);
    }

    // Test cases where all stakes are equal.
    #[test]
    fn test_constant_dist() {
        let stakes = [11_u128; 13];
        let num_shards = 1;
        let target_mandates_per_shard = stakes.len();
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);

        // There are enough validators to have 1:1 correspondence with mandates.
        assert_eq!(compute_mandate_price(config, || stakes.iter().copied()), stakes[0]);

        let target_mandates_per_shard = 2 * stakes.len();
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);

        // Now each validator needs to take two mandates.
        assert_eq!(compute_mandate_price(config, || stakes.iter().copied()), stakes[0] / 2);

        let target_mandates_per_shard = stakes.len() - 1;
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);

        // Now there are more validators than we need, but
        // the mandate price still doesn't go below the common stake.
        assert_eq!(compute_mandate_price(config, || stakes.iter().copied()), stakes[0]);
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
        let price = compute_mandate_price(config, || stakes.iter().copied());
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard - 1);

        let target_mandates_per_shard = 2 * stakes.len();
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, || stakes.iter().copied());
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard - 8);

        let target_mandates_per_shard = stakes.len() / 2;
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, || stakes.iter().copied());
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
        let price = compute_mandate_price(config, || stakes.iter().copied());
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard * num_shards);

        let num_shards = 1;
        let target_mandates_per_shard = stakes.len();
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, || stakes.iter().copied());
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard);

        let target_mandates_per_shard = stakes.len() * 2;
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, || stakes.iter().copied());
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard);

        let target_mandates_per_shard = stakes.len() / 2;
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, || stakes.iter().copied());
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
        let price = compute_mandate_price(config, || stakes.iter().copied());
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard + 21);

        let target_mandates_per_shard = 2 * stakes.len();
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, || stakes.iter().copied());
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard);

        let target_mandates_per_shard = stakes.len() / 2;
        let config = ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        let price = compute_mandate_price(config, || stakes.iter().copied());
        assert_eq!(count_whole_mandates(&stakes, price), target_mandates_per_shard - 31);
    }

    fn count_whole_mandates(stakes: &[u128], mandate_price: u128) -> usize {
        saturating_sum(stakes.iter().map(|s| *s / mandate_price)) as usize
    }
}
