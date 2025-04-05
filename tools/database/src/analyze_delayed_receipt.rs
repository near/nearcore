use clap::Parser;
use near_store::adapter::StoreAdapter;
use near_store::flat::FlatStorageManager;
use near_store::{ShardTries, StateSnapshotConfig, TrieConfig, get_delayed_receipt_indices};
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;

use near_chain::ChainStore;
use near_chain::ChainStoreAccess;
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use nearcore::config::load_config;

use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::BlockHeight;

use crate::block_iterators::{
    CommandArgs, LastNBlocksIterator, make_block_iterator_from_command_args,
};
use nearcore::open_storage;

/// Analyze delayed receipts in a piece of history of the blockchain to understand congestion of each shard
#[derive(Parser)]
pub(crate) struct AnalyzeDelayedReceiptCommand {
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

impl AnalyzeDelayedReceiptCommand {
    pub(crate) fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let mut near_config = load_config(home, genesis_validation).unwrap();
        let node_storage = open_storage(&home, &mut near_config).unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let chain_store = Rc::new(ChainStore::new(
            store.clone(),
            false,
            near_config.genesis.config.transaction_validity_period,
        ));
        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config, None);

        let tip = chain_store.head().unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
        let shard_uids = shard_layout.shard_uids().collect::<Vec<_>>();
        let shard_tries = ShardTries::new(
            store.trie_store(),
            TrieConfig::default(),
            &shard_uids,
            FlatStorageManager::new(store.flat_store()),
            StateSnapshotConfig::Disabled,
        );
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
                Box::new(LastNBlocksIterator::new(100, chain_store))
            }
        };

        let mut blocks_count: usize = 0;
        let mut first_analyzed_block: Option<(BlockHeight, CryptoHash)> = None;
        let mut last_analyzed_block: Option<(BlockHeight, CryptoHash)> = None;
        let mut shard_id_to_congested = HashMap::new();

        for block in blocks_iter {
            blocks_count += 1;
            if first_analyzed_block.is_none() {
                first_analyzed_block = Some((block.header().height(), *block.hash()));
            }
            last_analyzed_block = Some((block.header().height(), *block.hash()));
            let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();

            for chunk_header in block.chunks().iter_deprecated() {
                let state_root = chunk_header.prev_state_root();
                let trie_update = shard_tries.get_trie_for_shard(
                    ShardUId::from_shard_id_and_layout(chunk_header.shard_id(), &shard_layout),
                    state_root,
                );
                let delayed_receipt_indices = get_delayed_receipt_indices(&trie_update)?;
                if delayed_receipt_indices.len() > 0 {
                    *shard_id_to_congested.entry(chunk_header.shard_id()).or_insert(0) += 1;
                }
                println!(
                    "block height {} shard {} delayed receipts {}",
                    block.header().height(),
                    chunk_header.shard_id(),
                    delayed_receipt_indices.len()
                );
            }
        }

        println!("Analyzed {} blocks between:", blocks_count);
        if let Some((block_height, block_hash)) = first_analyzed_block {
            println!("Block: height = {block_height}, hash = {block_hash}");
        }
        if let Some((block_height, block_hash)) = last_analyzed_block {
            println!("Block: height = {block_height}, hash = {block_hash}");
        }

        if blocks_count == 0 {
            return Ok(());
        }
        for shard_id in shard_layout.shard_ids() {
            let congested_chunks = *shard_id_to_congested.get(&shard_id).unwrap_or(&0);
            println!(
                "shard {} congested ratio {}",
                shard_id,
                congested_chunks as f64 / blocks_count as f64
            );
        }

        Ok(())
    }
}
