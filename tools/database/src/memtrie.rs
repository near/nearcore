use crate::utils::open_rocksdb;
use anyhow::Context;
use near_chain::types::RuntimeAdapter;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_o11y::default_subscriber;
use near_o11y::env_filter::EnvFilterBuilder;
use near_primitives::block::Tip;
use near_primitives::block_header::BlockHeader;
use near_primitives::types::ShardId;
use near_store::{DBCol, ShardUId, HEAD_KEY};
use nearcore::{NightshadeRuntime, NightshadeRuntimeExt};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Command to load an in-memory trie for research purposes.
/// Example usage: neard database load-mem-trie --shard-id 0,1,2
#[derive(clap::Parser)]
pub struct LoadMemTrieCommand {
    #[clap(long, use_value_delimiter = true, value_delimiter = ',')]
    shard_id: Option<Vec<ShardId>>,
    #[clap(long)]
    no_parallel: bool,
}

impl LoadMemTrieCommand {
    pub fn run(&self, home: &Path) -> anyhow::Result<()> {
        let env_filter = EnvFilterBuilder::from_env().verbose(Some("memtrie")).finish()?;
        let _subscriber = default_subscriber(env_filter, &Default::default()).global();
        let mut near_config = nearcore::config::load_config(
            &home,
            near_chain_configs::GenesisValidationMode::UnsafeFast,
        )
        .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
        near_config.config.store.load_mem_tries_for_tracked_shards = true;

        let rocksdb = Arc::new(open_rocksdb(home, near_store::Mode::ReadOnly)?);
        let store = near_store::NodeStorage::new(rocksdb).get_hot_store();
        let genesis_config = &near_config.genesis.config;
        // Note: this is not necessarily correct; it's just an estimate of the shard layout,
        // so that users of this tool doesn't have to specify the full shard UID.
        let head =
            store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY).unwrap().unwrap().last_block_hash;
        let block_header = store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, &borsh::to_vec(&head).unwrap())?
            .ok_or_else(|| anyhow::anyhow!("Block header not found"))?;
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis_config);

        let all_shard_uids: Vec<ShardUId> =
            epoch_manager.get_shard_layout(block_header.epoch_id()).unwrap().shard_uids().collect();
        let selected_shard_uids: Vec<ShardUId> = match &self.shard_id {
            None => all_shard_uids,
            Some(shard_ids) => all_shard_uids
                .iter()
                .filter(|uid| shard_ids.contains(&uid.shard_id()))
                .map(|uid| *uid)
                .collect(),
        };

        let runtime = NightshadeRuntime::from_config(home, store, &near_config, epoch_manager)
            .context("could not create the transaction runtime")?;

        println!("Loading memtries for shards {:?}...", selected_shard_uids);
        runtime
            .get_tries()
            .load_mem_tries_for_enabled_shards(&selected_shard_uids, !self.no_parallel)?;
        println!("Finished loading memtries, press Ctrl-C to exit.");
        std::thread::sleep(Duration::from_secs(10_000_000_000));
        Ok(())
    }
}
