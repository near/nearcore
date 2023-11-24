use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use clap::Parser;
use near_chain::{Block, ChainStore};
use near_epoch_manager::EpochManager;

use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_id, ShardLayout};
use near_primitives::transaction::ExecutionOutcome;
use near_primitives::types::{AccountId, BlockHeight, EpochId, ShardId};

use near_store::{NodeStorage, ShardUId, Store};
use nearcore::open_storage;

use crate::block_iterators::{CommandArgs, LastNBlocksIterator};

/// `Gas` is an u64, but it stil might overflow when analysing a large amount of blocks.
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
pub(crate) struct AnalyseGasUsageCommand {
    /// Analyse the last N blocks in the blockchain
    #[arg(long)]
    last_blocks: Option<u64>,

    /// Analyse blocks from the given block height, inclusive
    #[arg(long)]
    from_block_height: Option<BlockHeight>,

    /// Analyse blocks up to the given block height, inclusive
    #[arg(long)]
    to_block_height: Option<BlockHeight>,
}

impl AnalyseGasUsageCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        // Create a ChainStore and EpochManager that will be used to read blockchain data.
        let mut near_config =
            nearcore::config::load_config(home, near_chain_configs::GenesisValidationMode::Full)
                .unwrap();
        let node_storage: NodeStorage = open_storage(&home, &mut near_config).unwrap();
        let store: Store =
            node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let chain_store = Rc::new(ChainStore::new(
            store.clone(),
            near_config.genesis.config.genesis_height,
            false,
        ));
        let epoch_manager =
            EpochManager::new_from_genesis_config(store, &near_config.genesis.config).unwrap();

        // Create an iterator over the blocks that should be analysed
        let blocks_iter_opt = crate::block_iterators::make_block_iterator_from_command_args(
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

        // Analyse
        analyse_gas_usage(blocks_iter, &chain_store, &epoch_manager);

        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct GasUsageInShard {
    pub used_gas_per_account: BTreeMap<AccountId, BigGas>,
    pub used_gas_total: BigGas,
}

/// A shard can be split into two halves.
/// This struct represents the result of splitting a shard at `split_account`.
#[derive(Debug, Clone)]
struct ShardSplit {
    /// Account on which the shard would be split
    pub split_account: AccountId,
    /// Gas used by accounts < split_account
    pub gas_left: BigGas,
    /// Gas used by accounts >= split_account
    pub gas_right: BigGas,
}

impl GasUsageInShard {
    pub fn new() -> GasUsageInShard {
        GasUsageInShard { used_gas_per_account: BTreeMap::new(), used_gas_total: 0 }
    }

    pub fn add_used_gas(&mut self, account: AccountId, used_gas: BigGas) {
        let old_used_gas: &BigGas = self.used_gas_per_account.get(&account).unwrap_or(&0);
        let new_used_gas: BigGas = old_used_gas.checked_add(used_gas).unwrap();
        self.used_gas_per_account.insert(account, new_used_gas);

        self.used_gas_total = self.used_gas_total.checked_add(used_gas).unwrap();
    }

    pub fn merge(&mut self, other: &GasUsageInShard) {
        for (account_id, used_gas) in &other.used_gas_per_account {
            self.add_used_gas(account_id.clone(), *used_gas);
        }
        self.used_gas_total = self.used_gas_total.checked_add(other.used_gas_total).unwrap();
    }

    /// Calculate the optimal point at which this shard could be split into two halves with similar gas usage
    pub fn calculate_split(&self) -> Option<ShardSplit> {
        let mut split_account = match self.used_gas_per_account.keys().next() {
            Some(account_id) => account_id,
            None => return None,
        };

        if self.used_gas_per_account.len() < 2 {
            return None;
        }

        let mut gas_left: BigGas = 0;
        let mut gas_right: BigGas = self.used_gas_total;

        for (account, used_gas) in self.used_gas_per_account.iter() {
            if gas_left >= gas_right {
                break;
            }

            split_account = &account;
            gas_left = gas_left.checked_add(*used_gas).unwrap();
            gas_right = gas_right.checked_sub(*used_gas).unwrap();
        }

        Some(ShardSplit { split_account: split_account.clone(), gas_left, gas_right })
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
            result = result.checked_add(shard_usage.used_gas_total).unwrap();
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
    epoch_manager: &EpochManager,
) -> GasUsageStats {
    let block_info: Arc<BlockInfo> = epoch_manager.get_block_info(block.hash()).unwrap();
    let epoch_id: &EpochId = block_info.epoch_id();
    let shard_layout: ShardLayout = epoch_manager.get_shard_layout(epoch_id).unwrap();

    let mut result = GasUsageStats::new();

    // Go over every chunk in this block and gather data
    for chunk_header in block.chunks().iter() {
        let shard_id: ShardId = chunk_header.shard_id();
        let shard_uid: ShardUId = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);

        let mut gas_usage_in_shard = GasUsageInShard::new();

        // The outcome of each transaction and receipt executed in this chunk is saved in the database as an ExecutionOutcome.
        // Go through all ExecutionOutcomes from this chunk and record the gas usage.
        let outcome_ids: Vec<CryptoHash> =
            chain_store.get_outcomes_by_block_hash_and_shard_id(block.hash(), shard_id).unwrap();
        for outcome_id in outcome_ids {
            let outcome: ExecutionOutcome = chain_store
                .get_outcome_by_id_and_block_hash(&outcome_id, block.hash())
                .unwrap()
                .unwrap()
                .outcome;

            // Sanity check - make sure that the executor of this outcome belongs to this shard
            let account_shard: ShardId =
                account_id_to_shard_id(&outcome.executor_id, &shard_layout);
            assert_eq!(account_shard, shard_id);

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

fn analyse_gas_usage(
    blocks_iter: impl Iterator<Item = Block>,
    chain_store: &ChainStore,
    epoch_manager: &EpochManager,
) {
    // Gather statistics about gas usage in all of the blocks
    let mut blocks_count: usize = 0;
    let mut first_analysed_block: Option<(BlockHeight, CryptoHash)> = None;
    let mut last_analysed_block: Option<(BlockHeight, CryptoHash)> = None;

    let mut gas_usage_stats = GasUsageStats::new();

    for block in blocks_iter {
        blocks_count += 1;
        if first_analysed_block.is_none() {
            first_analysed_block = Some((block.header().height(), *block.hash()));
        }
        last_analysed_block = Some((block.header().height(), *block.hash()));

        let gas_usage_in_block: GasUsageStats =
            get_gas_usage_in_block(&block, chain_store, epoch_manager);
        gas_usage_stats.merge(gas_usage_in_block);
    }

    // Calculates how much percent of `big` is `small` and returns it as a string.
    // Example: as_percentage_of(10, 100) == "10.0%"
    let as_percentage_of = |small: BigGas, big: BigGas| {
        if big > 0 {
            format!("{:.1}%", small as f64 / big as f64 * 100.0)
        } else {
            format!("-")
        }
    };

    // Print out the analysis
    if blocks_count == 0 {
        println!("No blocks to analyse!");
        return;
    }
    println!("");
    println!("Analysed {} blocks between:", blocks_count);
    if let Some((block_height, block_hash)) = first_analysed_block {
        println!("Block: height = {block_height}, hash = {block_hash}");
    }
    if let Some((block_height, block_hash)) = last_analysed_block {
        println!("Block: height = {block_height}, hash = {block_hash}");
    }
    let total_gas: BigGas = gas_usage_stats.used_gas_total();
    println!("");
    println!("Total gas used: {}", display_gas(total_gas));
    println!("");
    for (shard_uid, shard_usage) in &gas_usage_stats.shards {
        println!("Shard: {}", shard_uid);
        println!(
            "  Gas usage: {} ({} of total)",
            display_gas(shard_usage.used_gas_total),
            as_percentage_of(shard_usage.used_gas_total, total_gas)
        );
        println!("  Number of accounts: {}", shard_usage.used_gas_per_account.len());
        match shard_usage.calculate_split() {
            Some(shard_split) => {
                println!("  Optimal split:");
                println!("    split_account: {}", shard_split.split_account);
                println!(
                    "    gas(account < split_account): {} ({} of shard)",
                    display_gas(shard_split.gas_left),
                    as_percentage_of(shard_split.gas_left, shard_usage.used_gas_total)
                );
                println!(
                    "    gas(account >= split_account): {} ({} of shard)",
                    display_gas(shard_split.gas_right),
                    as_percentage_of(shard_split.gas_right, shard_usage.used_gas_total)
                );
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
