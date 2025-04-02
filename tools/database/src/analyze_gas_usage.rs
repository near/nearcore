use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::rc::Rc;

use clap::Parser;
use near_chain::{Block, ChainStore};
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use nearcore::config::load_config;

use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{AccountId, BlockHeight};

use nearcore::open_storage;

use crate::block_iterators::{
    CommandArgs, LastNBlocksIterator, make_block_iterator_from_command_args,
};

/// `Gas` is an u64, but it still might overflow when analyzing a large amount of blocks.
/// 1ms of compute is about 1TGas = 10^12 gas. One epoch takes 43200 seconds (43200000ms).
/// This means that the amount of gas consumed during a single epoch can reach 43200000 * 10^12 = 4.32 * 10^19
/// 10^19 doesn't fit in u64, so we need to use u128
/// To avoid overflows, let's use `BigGas` for storing gas amounts in the code.
type BigGas = u128;

/// Display gas amount in a human-friendly way
fn display_gas(gas: BigGas) -> String {
    let tera_gas = gas as f64 / 1e12;
    format!("{:.2} TGas", tera_gas)
}

#[derive(Parser)]
pub(crate) struct AnalyzeGasUsageCommand {
    /// Analyze the last N blocks in the blockchain
    #[arg(long)]
    last_blocks: Option<u64>,

    /// Analyze blocks from the given block height, inclusive
    #[arg(long)]
    from_block_height: Option<BlockHeight>,

    /// Analyze blocks up to the given block height, inclusive
    #[arg(long)]
    to_block_height: Option<BlockHeight>,
}

impl AnalyzeGasUsageCommand {
    pub(crate) fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        // Create a ChainStore and EpochManager that will be used to read blockchain data.
        let mut near_config = load_config(home, genesis_validation).unwrap();
        let node_storage = open_storage(&home, &mut near_config).unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let chain_store = Rc::new(ChainStore::new(
            store.clone(),
            false,
            near_config.genesis.config.transaction_validity_period,
        ));
        let epoch_manager = EpochManager::new_arc_handle(store, &near_config.genesis.config, None);

        // Create an iterator over the blocks that should be analyzed
        let blocks_iter_opt = make_block_iterator_from_command_args(
            CommandArgs {
                last_blocks: self.last_blocks,
                from_block_height: self.from_block_height,
                to_block_height: self.to_block_height,
            },
            chain_store.clone(),
        );

        let blocks_iter = match blocks_iter_opt {
            Some(iter) => iter,
            None => {
                println!("No arguments, defaulting to last 100 blocks");
                Box::new(LastNBlocksIterator::new(100, chain_store.clone()))
            }
        };

        // Analyze
        analyze_gas_usage(blocks_iter, &chain_store, &epoch_manager);

        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct GasUsageInShard {
    pub used_gas_per_account: BTreeMap<AccountId, BigGas>,
}

/// A shard can be split into two halves.
/// This struct represents the result of splitting a shard at `boundary_account`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ShardSplit {
    /// Account on which the shard would be split
    pub boundary_account: AccountId,
    /// Gas used by accounts < boundary_account
    pub gas_left: BigGas,
    /// Gas used by accounts => boundary_account
    pub gas_right: BigGas,
}

impl GasUsageInShard {
    pub fn new() -> GasUsageInShard {
        GasUsageInShard { used_gas_per_account: BTreeMap::new() }
    }

    pub fn add_used_gas(&mut self, account: AccountId, used_gas: BigGas) {
        let account_gas = self.used_gas_per_account.entry(account).or_insert(0);
        *account_gas = account_gas.checked_add(used_gas).unwrap();
    }

    pub fn used_gas_total(&self) -> BigGas {
        let mut result: BigGas = 0;
        for used_gas in self.used_gas_per_account.values() {
            result = result.checked_add(*used_gas).unwrap();
        }
        result
    }

    pub fn merge(&mut self, other: &GasUsageInShard) {
        for (account_id, used_gas) in &other.used_gas_per_account {
            self.add_used_gas(account_id.clone(), *used_gas);
        }
    }

    /// Calculate the optimal point at which this shard could be split into two halves with similar gas usage
    pub fn calculate_split(&self) -> Option<ShardSplit> {
        let total_gas = self.used_gas_total();
        if total_gas == 0 || self.used_gas_per_account.len() < 2 {
            return None;
        }

        // Find a split with the smallest difference between the two halves
        let mut best_split: Option<ShardSplit> = None;
        let mut best_difference: BigGas = total_gas;

        let mut gas_left: BigGas = 0;
        let mut gas_right: BigGas = total_gas;

        for (account, used_gas) in &self.used_gas_per_account {
            // We are now considering a split of (left < account) and (right >= account)

            let difference: BigGas = gas_left.abs_diff(gas_right);
            if difference < best_difference {
                best_difference = difference;
                best_split =
                    Some(ShardSplit { boundary_account: account.clone(), gas_left, gas_right });
            }

            gas_left = gas_left.checked_add(*used_gas).unwrap();
            gas_right = gas_right.checked_sub(*used_gas).unwrap();
        }
        best_split
    }

    pub fn biggest_account(&self) -> Option<(AccountId, BigGas)> {
        let mut result: Option<(AccountId, BigGas)> = None;

        for (account, used_gas) in &self.used_gas_per_account {
            match &mut result {
                None => result = Some((account.clone(), *used_gas)),
                Some((best_account, best_gas)) => {
                    if *used_gas > *best_gas {
                        *best_account = account.clone();
                        *best_gas = *used_gas
                    }
                }
            }
        }

        result
    }
}

#[derive(Clone, Debug)]
struct GasUsageStats {
    pub shards: BTreeMap<ShardUId, GasUsageInShard>,
}

impl GasUsageStats {
    pub fn new() -> GasUsageStats {
        GasUsageStats { shards: BTreeMap::new() }
    }

    pub fn add_gas_usage_in_shard(&mut self, shard_uid: ShardUId, shard_usage: GasUsageInShard) {
        match self.shards.get_mut(&shard_uid) {
            Some(existing_shard_usage) => existing_shard_usage.merge(&shard_usage),
            None => {
                let _ = self.shards.insert(shard_uid, shard_usage);
            }
        }
    }

    pub fn used_gas_total(&self) -> BigGas {
        let mut result: BigGas = 0;
        for shard_usage in self.shards.values() {
            result = result.checked_add(shard_usage.used_gas_total()).unwrap();
        }
        result
    }

    pub fn merge(&mut self, other: GasUsageStats) {
        for (shard_uid, shard_usage) in other.shards {
            self.add_gas_usage_in_shard(shard_uid, shard_usage);
        }
    }
}

fn get_gas_usage_in_block(
    block: &Block,
    chain_store: &ChainStore,
    epoch_manager: &EpochManagerHandle,
) -> GasUsageStats {
    let block_info = epoch_manager.get_block_info(block.hash()).unwrap();
    let epoch_id = block_info.epoch_id();
    let shard_layout = epoch_manager.get_shard_layout(epoch_id).unwrap();

    let mut result = GasUsageStats::new();

    // Go over every chunk in this block and gather data
    for chunk_header in block.chunks().iter_deprecated() {
        let shard_id = chunk_header.shard_id();
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);

        let mut gas_usage_in_shard = GasUsageInShard::new();

        // The outcome of each transaction and receipt executed in this chunk is saved in the database as an ExecutionOutcome.
        // Go through all ExecutionOutcomes from this chunk and record the gas usage.
        let outcome_ids =
            chain_store.get_outcomes_by_block_hash_and_shard_id(block.hash(), shard_id).unwrap();
        for outcome_id in outcome_ids {
            let outcome = chain_store
                .get_outcome_by_id_and_block_hash(&outcome_id, block.hash())
                .unwrap()
                .unwrap()
                .outcome;

            // Sanity check - make sure that the executor of this outcome belongs to this shard
            let account_shard_id = shard_layout.account_id_to_shard_id(&outcome.executor_id);
            assert_eq!(account_shard_id, shard_id);

            gas_usage_in_shard.add_used_gas(outcome.executor_id, outcome.gas_burnt.into());
        }

        result.add_gas_usage_in_shard(shard_uid, gas_usage_in_shard);
    }

    result
}

/// A struct that can be used to find N biggest accounts by gas usage in an efficient manner.
struct BiggestAccountsFinder {
    accounts: BTreeSet<(BigGas, AccountId)>,
    accounts_num: usize,
}

impl BiggestAccountsFinder {
    pub fn new(accounts_num: usize) -> BiggestAccountsFinder {
        BiggestAccountsFinder { accounts: BTreeSet::new(), accounts_num }
    }

    pub fn add_account_stats(&mut self, account: AccountId, used_gas: BigGas) {
        self.accounts.insert((used_gas, account));

        // If there are more accounts than desired, remove the one with the smallest gas usage
        while self.accounts.len() > self.accounts_num {
            self.accounts.pop_first();
        }
    }

    pub fn get_biggest_accounts(&self) -> impl Iterator<Item = (AccountId, BigGas)> + '_ {
        self.accounts.iter().rev().map(|(gas, account)| (account.clone(), *gas))
    }
}

// Calculates how much percent of `big` is `small` and returns it as a string.
// Example: as_percentage_of(10, 100) == "10.0%"
fn as_percentage_of(small: BigGas, big: BigGas) -> String {
    if big > 0 { format!("{:.1}%", small as f64 / big as f64 * 100.0) } else { format!("-") }
}

fn display_shard_split_stats<'a>(
    accounts: impl Iterator<Item = (&'a AccountId, &'a BigGas)>,
    total_shard_gas: BigGas,
) {
    let mut accounts_num: u64 = 0;
    let mut total_split_half_gas: BigGas = 0;
    let mut top_3_finder = BiggestAccountsFinder::new(3);

    for (account, used_gas) in accounts {
        accounts_num += 1;
        total_split_half_gas = total_split_half_gas.checked_add(*used_gas).unwrap();
        top_3_finder.add_account_stats(account.clone(), *used_gas);
    }

    let indent = "      ";
    println!(
        "{}Gas: {} ({} of shard)",
        indent,
        display_gas(total_split_half_gas),
        as_percentage_of(total_split_half_gas, total_shard_gas)
    );
    println!("{}Accounts: {}", indent, accounts_num);
    println!("{}Top 3 accounts:", indent);
    for (i, (account, used_gas)) in top_3_finder.get_biggest_accounts().enumerate() {
        println!("{}  #{}: {}", indent, i + 1, account);
        println!(
            "{}      Used gas: {} ({} of shard)",
            indent,
            display_gas(used_gas),
            as_percentage_of(used_gas, total_shard_gas)
        )
    }
}

fn analyze_gas_usage(
    blocks_iter: impl Iterator<Item = Block>,
    chain_store: &ChainStore,
    epoch_manager: &EpochManagerHandle,
) {
    // Gather statistics about gas usage in all of the blocks
    let mut blocks_count: usize = 0;
    let mut first_analyzed_block: Option<(BlockHeight, CryptoHash)> = None;
    let mut last_analyzed_block: Option<(BlockHeight, CryptoHash)> = None;

    let mut gas_usage_stats = GasUsageStats::new();

    for block in blocks_iter {
        blocks_count += 1;
        if first_analyzed_block.is_none() {
            first_analyzed_block = Some((block.header().height(), *block.hash()));
        }
        last_analyzed_block = Some((block.header().height(), *block.hash()));

        let gas_usage_in_block = get_gas_usage_in_block(&block, chain_store, epoch_manager);
        gas_usage_stats.merge(gas_usage_in_block);
    }

    // Print out the analysis
    if blocks_count == 0 {
        println!("No blocks to analyze!");
        return;
    }
    println!("");
    println!("Analyzed {} blocks between:", blocks_count);
    if let Some((block_height, block_hash)) = first_analyzed_block {
        println!("Block: height = {block_height}, hash = {block_hash}");
    }
    if let Some((block_height, block_hash)) = last_analyzed_block {
        println!("Block: height = {block_height}, hash = {block_hash}");
    }
    let total_gas = gas_usage_stats.used_gas_total();
    println!("");
    println!("Total gas used: {}", display_gas(total_gas));
    println!("");
    for (shard_uid, shard_usage) in &gas_usage_stats.shards {
        println!("Shard: {}", shard_uid);
        let shard_total_gas = shard_usage.used_gas_total();
        println!(
            "  Gas usage: {} ({} of total)",
            display_gas(shard_usage.used_gas_total()),
            as_percentage_of(shard_total_gas, total_gas)
        );
        println!("  Number of accounts: {}", shard_usage.used_gas_per_account.len());
        if let Some((biggest_account, biggest_account_gas)) = shard_usage.biggest_account() {
            println!("  Biggest account: {}", biggest_account);
            println!(
                "  Biggest account gas: {} ({} of shard)",
                display_gas(biggest_account_gas),
                as_percentage_of(biggest_account_gas, shard_total_gas)
            );
        }
        match shard_usage.calculate_split() {
            Some(shard_split) => {
                println!("  Optimal split:");
                println!("    boundary_account: {}", shard_split.boundary_account);
                let boundary_account_gas =
                    *shard_usage.used_gas_per_account.get(&shard_split.boundary_account).unwrap();
                println!("    gas(boundary_account): {}", display_gas(boundary_account_gas));
                println!(
                    "    Gas distribution (left, boundary_acc, right): ({}, {}, {})",
                    as_percentage_of(shard_split.gas_left, shard_total_gas),
                    as_percentage_of(boundary_account_gas, shard_total_gas),
                    as_percentage_of(
                        shard_split.gas_right.saturating_sub(boundary_account_gas),
                        shard_total_gas
                    )
                );
                println!("    Left (account < boundary_account):");
                let left_accounts =
                    shard_usage.used_gas_per_account.range(..shard_split.boundary_account.clone());
                display_shard_split_stats(left_accounts, shard_total_gas);
                println!("    Right (account >= boundary_account):");
                let right_accounts =
                    shard_usage.used_gas_per_account.range(shard_split.boundary_account..);
                display_shard_split_stats(right_accounts, shard_total_gas);
            }
            None => println!("  No optimal split for this shard"),
        }
        println!("");
    }

    // Find 10 biggest accounts by gas usage
    let mut biggest_accounts_finder = BiggestAccountsFinder::new(10);
    for shard in gas_usage_stats.shards.values() {
        for (account, used_gas) in &shard.used_gas_per_account {
            biggest_accounts_finder.add_account_stats(account.clone(), *used_gas);
        }
    }
    println!("10 biggest accounts by gas usage:");
    for (i, (account, gas_usage)) in biggest_accounts_finder.get_biggest_accounts().enumerate() {
        println!("#{}: {}", i + 1, account);
        println!(
            "    Used gas: {} ({} of total)",
            display_gas(gas_usage),
            as_percentage_of(gas_usage, total_gas)
        )
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use near_primitives::types::AccountId;

    use super::{GasUsageInShard, ShardSplit};

    fn account(name: &str) -> AccountId {
        AccountId::from_str(&format!("{name}.near")).unwrap()
    }

    // There is no optimal split for a shard with no accounts
    #[test]
    fn empty_shard_no_split() {
        let empty_shard = GasUsageInShard::new();
        assert_eq!(empty_shard.calculate_split(), None);
    }

    // There is no optimal split for a shard with a single account
    #[test]
    fn one_account_no_split() {
        let mut shard_usage = GasUsageInShard::new();

        shard_usage.add_used_gas(account("a"), 12345);

        assert_eq!(shard_usage.calculate_split(), None);
    }

    // A shard with two equally sized accounts should be split in half
    #[test]
    fn two_accounts_equal_split() {
        let mut shard_usage = GasUsageInShard::new();

        shard_usage.add_used_gas(account("a"), 12345);
        shard_usage.add_used_gas(account("b"), 12345);

        let optimal_split =
            ShardSplit { boundary_account: account("b"), gas_left: 12345, gas_right: 12345 };
        assert_eq!(shard_usage.calculate_split(), Some(optimal_split));
    }

    // A shard with two accounts where the first is slightly smaller should be split in half
    #[test]
    fn two_accounts_first_smaller_split() {
        let mut shard_usage = GasUsageInShard::new();

        shard_usage.add_used_gas(account("a"), 123);
        shard_usage.add_used_gas(account("b"), 12345);

        let optimal_split =
            ShardSplit { boundary_account: account("b"), gas_left: 123, gas_right: 12345 };
        assert_eq!(shard_usage.calculate_split(), Some(optimal_split));
    }

    // A shard with two accounts where the second one is slightly smaller should be split in half
    #[test]
    fn two_accounts_second_smaller_split() {
        let mut shard_usage = GasUsageInShard::new();

        shard_usage.add_used_gas(account("a"), 12345);
        shard_usage.add_used_gas(account("b"), 123);

        let optimal_split =
            ShardSplit { boundary_account: account("b"), gas_left: 12345, gas_right: 123 };
        assert_eq!(shard_usage.calculate_split(), Some(optimal_split));
    }

    // A shard with multiple accounts where all of them use 0 gas has optimal split
    #[test]
    fn many_accounts_zero_no_split() {
        let mut shard_usage = GasUsageInShard::new();

        shard_usage.add_used_gas(account("a"), 0);
        shard_usage.add_used_gas(account("b"), 0);
        shard_usage.add_used_gas(account("c"), 0);
        shard_usage.add_used_gas(account("d"), 0);
        shard_usage.add_used_gas(account("e"), 0);
        shard_usage.add_used_gas(account("f"), 0);
        shard_usage.add_used_gas(account("g"), 0);
        shard_usage.add_used_gas(account("h"), 0);

        assert_eq!(shard_usage.calculate_split(), None);
    }

    // A shard with multiple accounts where only one is nonzero has no optimal split
    #[test]
    fn many_accounts_one_nonzero_no_split() {
        let mut shard_usage = GasUsageInShard::new();

        shard_usage.add_used_gas(account("a"), 0);
        shard_usage.add_used_gas(account("b"), 0);
        shard_usage.add_used_gas(account("c"), 0);
        shard_usage.add_used_gas(account("d"), 0);
        shard_usage.add_used_gas(account("e"), 12345);
        shard_usage.add_used_gas(account("f"), 0);
        shard_usage.add_used_gas(account("g"), 0);
        shard_usage.add_used_gas(account("h"), 0);

        assert_eq!(shard_usage.calculate_split(), None);
    }

    // An example set of accounts is split correctly
    #[test]
    fn many_accounts_split() {
        let mut shard_usage = GasUsageInShard::new();

        shard_usage.add_used_gas(account("a"), 1);
        shard_usage.add_used_gas(account("b"), 3);
        shard_usage.add_used_gas(account("c"), 5);
        shard_usage.add_used_gas(account("d"), 2);
        shard_usage.add_used_gas(account("e"), 8);
        shard_usage.add_used_gas(account("f"), 1);
        shard_usage.add_used_gas(account("g"), 2);
        shard_usage.add_used_gas(account("h"), 8);

        // Optimal split:
        // 1 + 3 + 5 + 2 = 11
        // 8 + 1 + 2 + 8 = 19
        let optimal_split =
            ShardSplit { boundary_account: account("e"), gas_left: 11, gas_right: 19 };
        assert_eq!(shard_usage.calculate_split(), Some(optimal_split));
    }

    // The first account uses the most gas, it should be the only one in its half of the split
    #[test]
    fn first_heavy_split() {
        let mut shard_usage = GasUsageInShard::new();

        shard_usage.add_used_gas(account("a"), 10000);
        shard_usage.add_used_gas(account("b"), 1);
        shard_usage.add_used_gas(account("c"), 1);
        shard_usage.add_used_gas(account("d"), 1);
        shard_usage.add_used_gas(account("e"), 1);
        shard_usage.add_used_gas(account("f"), 1);
        shard_usage.add_used_gas(account("g"), 1);
        shard_usage.add_used_gas(account("h"), 1);

        let optimal_split =
            ShardSplit { boundary_account: account("b"), gas_left: 10000, gas_right: 7 };
        assert_eq!(shard_usage.calculate_split(), Some(optimal_split));
    }

    // The last account uses the most gas, it should be the only one in its half of the split
    #[test]
    fn last_heavy_split() {
        let mut shard_usage = GasUsageInShard::new();

        shard_usage.add_used_gas(account("a"), 1);
        shard_usage.add_used_gas(account("b"), 1);
        shard_usage.add_used_gas(account("c"), 1);
        shard_usage.add_used_gas(account("d"), 1);
        shard_usage.add_used_gas(account("e"), 1);
        shard_usage.add_used_gas(account("f"), 1);
        shard_usage.add_used_gas(account("g"), 1);
        shard_usage.add_used_gas(account("h"), 10000);

        let optimal_split =
            ShardSplit { boundary_account: account("h"), gas_left: 7, gas_right: 10000 };
        assert_eq!(shard_usage.calculate_split(), Some(optimal_split));
    }
}
