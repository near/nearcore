use crate::types::Balance;
use aliases::Aliases;
use borsh::{BorshDeserialize, BorshSerialize};

#[derive(
    Default, BorshSerialize, BorshDeserialize, serde::Serialize, Clone, Debug, PartialEq, Eq,
)]
pub struct WeightedIndex {
    weight_sum: Balance,
    aliases: Vec<u64>,
    no_alias_odds: Vec<Balance>,
}

impl WeightedIndex {
    pub fn new(weights: Vec<Balance>) -> Self {
        let n = Balance::from(weights.len() as u64);
        let mut aliases = Aliases::new(weights.len());

        let mut no_alias_odds = weights;
        let mut weight_sum: Balance = 0;
        for w in no_alias_odds.iter_mut() {
            weight_sum += *w;
            *w *= n;
        }

        for (index, &odds) in no_alias_odds.iter().enumerate() {
            if odds < weight_sum {
                aliases.push_small(index);
            } else {
                aliases.push_big(index);
            }
        }

        while !aliases.smalls_is_empty() && !aliases.bigs_is_empty() {
            let s = aliases.pop_small();
            let b = aliases.pop_big();

            aliases.set_alias(s, b);
            no_alias_odds[b] = no_alias_odds[b] - weight_sum + no_alias_odds[s];

            if no_alias_odds[b] < weight_sum {
                aliases.push_small(b);
            } else {
                aliases.push_big(b);
            }
        }

        while !aliases.smalls_is_empty() {
            no_alias_odds[aliases.pop_small()] = weight_sum;
        }

        while !aliases.bigs_is_empty() {
            no_alias_odds[aliases.pop_big()] = weight_sum;
        }

        Self { weight_sum, no_alias_odds, aliases: aliases.get_aliases() }
    }

    pub fn sample(&self, seed: [u8; 32]) -> usize {
        let usize_seed = Self::copy_8_bytes(&seed[0..8]);
        let balance_seed = Self::copy_16_bytes(&seed[8..24]);
        let uniform_index = usize::from_le_bytes(usize_seed) % self.aliases.len();
        let uniform_weight = Balance::from_le_bytes(balance_seed) % self.weight_sum;

        if uniform_weight < self.no_alias_odds[uniform_index] {
            uniform_index
        } else {
            self.aliases[uniform_index] as usize
        }
    }

    pub fn get_aliases(&self) -> &[u64] {
        &self.aliases
    }

    pub fn get_no_alias_odds(&self) -> &[Balance] {
        &self.no_alias_odds
    }

    fn copy_8_bytes(arr: &[u8]) -> [u8; 8] {
        let mut result = [0u8; 8];
        result.clone_from_slice(arr);
        result
    }

    fn copy_16_bytes(arr: &[u8]) -> [u8; 16] {
        let mut result = [0u8; 16];
        result.clone_from_slice(arr);
        result
    }
}

/// Sub-module to encapsulate helper struct for managing aliases
mod aliases {
    pub struct Aliases {
        aliases: Vec<usize>,
        smalls: Vec<usize>,
        bigs: Vec<usize>,
    }

    impl Aliases {
        pub fn new(n: usize) -> Self {
            Self { aliases: vec![0; n], smalls: Vec::with_capacity(n), bigs: Vec::with_capacity(n) }
        }

        pub fn push_big(&mut self, b: usize) {
            self.bigs.push(b);
        }

        pub fn pop_big(&mut self) -> usize {
            self.bigs.pop().unwrap()
        }

        pub fn bigs_is_empty(&self) -> bool {
            self.bigs.is_empty()
        }

        pub fn push_small(&mut self, s: usize) {
            self.smalls.push(s);
        }

        pub fn pop_small(&mut self) -> usize {
            self.smalls.pop().unwrap()
        }

        pub fn smalls_is_empty(&self) -> bool {
            self.smalls.is_empty()
        }

        pub fn set_alias(&mut self, index: usize, alias: usize) {
            self.aliases[index] = alias;
        }

        pub fn get_aliases(self) -> Vec<u64> {
            self.aliases.into_iter().map(|a| a as u64).collect()
        }
    }
}

#[cfg(test)]
mod test {
    use crate::hash;
    use crate::rand::WeightedIndex;

    #[test]
    fn test_should_correctly_compute_odds_and_aliases() {
        // Example taken from https://www.keithschwarz.com/darts-dice-coins/
        let weights = vec![5, 8, 4, 10, 4, 4, 5];
        let weighted_index = WeightedIndex::new(weights);

        assert_eq!(weighted_index.get_aliases(), &[1, 0, 3, 1, 3, 3, 3]);

        assert_eq!(weighted_index.get_no_alias_odds(), &[35, 40, 28, 29, 28, 28, 35]);
    }

    #[test]
    fn test_sample_should_produce_correct_distribution() {
        let weights = vec![5, 1, 1];
        let weighted_index = WeightedIndex::new(weights);

        let n_samples = 1_000_000;
        let mut seed = hash(&[0; 32]);
        let mut counts: [i32; 3] = [0, 0, 0];
        for _ in 0..n_samples {
            let index = weighted_index.sample(seed);
            counts[index] += 1;
            seed = hash(&seed);
        }

        assert_relative_closeness(counts[0], 5 * counts[1]);
        assert_relative_closeness(counts[1], counts[2]);
    }

    /// Assert y is within 0.5% of x.
    #[track_caller]
    fn assert_relative_closeness(x: i32, y: i32) {
        let diff = (y - x).abs();
        let relative_diff = f64::from(diff) / f64::from(x);
        assert!(relative_diff < 0.005);
    }

    fn hash(input: &[u8]) -> [u8; 32] {
        hash::hash(input).0
    }
}
