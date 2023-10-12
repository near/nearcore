use std::cmp::max;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::static_clock::StaticClock;
use tracing::info;

pub struct BlockStats {
    hash2depth: HashMap<CryptoHash, u64>,
    num_blocks: u64,
    max_chain_length: u64,
    last_check: Instant,
    max_divergence: u64,
    last_hash: Option<CryptoHash>,
    parent: HashMap<CryptoHash, CryptoHash>,
}

impl BlockStats {
    pub(crate) fn new() -> BlockStats {
        BlockStats {
            hash2depth: HashMap::new(),
            num_blocks: 0,
            max_chain_length: 0,
            last_check: StaticClock::instant(),
            max_divergence: 0,
            last_hash: None,
            parent: HashMap::new(),
        }
    }

    fn calculate_distance(&mut self, mut lhs: CryptoHash, mut rhs: CryptoHash) -> u64 {
        let mut dlhs = *self.hash2depth.get(&lhs).unwrap();
        let mut drhs = *self.hash2depth.get(&rhs).unwrap();

        let mut result: u64 = 0;
        while dlhs > drhs {
            lhs = *self.parent.get(&lhs).unwrap();
            dlhs -= 1;
            result += 1;
        }
        while dlhs < drhs {
            rhs = *self.parent.get(&rhs).unwrap();
            drhs -= 1;
            result += 1;
        }
        while lhs != rhs {
            lhs = *self.parent.get(&lhs).unwrap();
            rhs = *self.parent.get(&rhs).unwrap();
            result += 2;
        }
        result
    }

    pub(crate) fn add_block(&mut self, block: &Block) {
        if self.hash2depth.contains_key(block.hash()) {
            return;
        }
        let prev_height = self.hash2depth.get(block.header().prev_hash()).map(|v| *v).unwrap_or(0);
        self.hash2depth.insert(*block.hash(), prev_height + 1);
        self.num_blocks += 1;
        self.max_chain_length = max(self.max_chain_length, prev_height + 1);
        self.parent.insert(*block.hash(), *block.header().prev_hash());

        if let Some(last_hash2) = self.last_hash {
            self.max_divergence =
                max(self.max_divergence, self.calculate_distance(last_hash2, *block.hash()));
        }

        self.last_hash = Some(*block.hash());
    }

    pub fn check_stats(&mut self, force: bool) {
        let now = StaticClock::instant();
        let diff = now.duration_since(self.last_check);
        if !force && diff.lt(&Duration::from_secs(60)) {
            return;
        }
        self.last_check = now;
        let cur_ratio = (self.num_blocks as f64) / (max(1, self.max_chain_length) as f64);
        info!(
            "Block stats: ratio: {:.2}, num_blocks: {} max_chain_length: {} max_divergence: {}",
            cur_ratio, self.num_blocks, self.max_chain_length, self.max_divergence
        );
    }

    pub fn check_block_ratio(&mut self, min_ratio: Option<f64>, max_ratio: Option<f64>) {
        let cur_ratio = (self.num_blocks as f64) / (max(1, self.max_chain_length) as f64);
        if let Some(min_ratio2) = min_ratio {
            if cur_ratio < min_ratio2 {
                panic!(
                    "ratio of blocks to longest chain is too low got: {:.2} expected: {:.2}",
                    cur_ratio, min_ratio2
                );
            }
        }
        if let Some(max_ratio2) = max_ratio {
            if cur_ratio > max_ratio2 {
                panic!(
                    "ratio of blocks to longest chain is too high got: {:.2} expected: {:.2}",
                    cur_ratio, max_ratio2
                );
            }
        }
    }
}
