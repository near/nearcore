use crate::utils::open_rocksdb;
use anyhow::Context;
use near_async::messaging::{IntoMultiSender, noop};
use near_chain::resharding::event_type::ReshardingSplitShardParams;
use near_chain::resharding::manager::ReshardingManager;
use near_chain::types::RuntimeAdapter;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_o11y::default_subscriber;
use near_o11y::env_filter::EnvFilterBuilder;
use near_primitives::block::{Block, Tip};
use near_primitives::block_header::BlockHeader;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, ShardId};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::db::RocksDB;
use near_store::db::rocksdb::snapshot::Snapshot;
use near_store::trie::mem::node::MemTrieNodeView;
use near_store::{DBCol, HEAD_KEY, Mode, ShardTries, ShardUId, Temperature};
use nearcore::{NightshadeRuntime, NightshadeRuntimeExt};
use std::collections::VecDeque;
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
    pub fn run(
        &self,
        home: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let env_filter = EnvFilterBuilder::from_env().verbose(Some("memtrie")).finish()?;
        let _subscriber = default_subscriber(env_filter, &Default::default()).global();
        let mut near_config = nearcore::config::load_config(&home, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
        near_config.config.store.load_memtries_for_tracked_shards = true;

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
        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &genesis_config, Some(home));

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
        let start_time = std::time::Instant::now();
        runtime.get_tries().load_memtries_for_enabled_shards(
            &selected_shard_uids,
            &[].into(),
            !self.no_parallel,
        )?;
        println!(
            "Finished loading memtries, took {:?}, press Ctrl-C to exit.",
            start_time.elapsed()
        );
        std::thread::sleep(Duration::from_secs(10_000_000_000));
        Ok(())
    }
}

#[derive(clap::Parser)]
pub struct SplitShardTrieCommand {
    #[clap(long)]
    shard_uid: ShardUId,
    #[clap(long)]
    boundary_account: AccountId,
}

impl SplitShardTrieCommand {
    pub fn run(
        &self,
        home: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let env_filter = EnvFilterBuilder::from_env().verbose(Some("memtrie")).finish()?;
        let _subscriber = default_subscriber(env_filter, &Default::default()).global();
        let mut near_config = nearcore::config::load_config(&home, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
        near_config.config.store.load_memtries_for_tracked_shards = true;
        let genesis_config = &near_config.genesis.config;
        let chain_id = &genesis_config.chain_id;

        // Create a database snapshot to avoid corrupting the existing database
        println!("Creating database snapshot...");
        let db_path =
            near_config.config.store.path.as_ref().cloned().unwrap_or_else(|| home.join("data"));
        let snapshot =
            Snapshot::new(db_path.as_path(), &near_config.config.store, Temperature::Hot)?;
        let snapshot_path =
            snapshot.0.as_ref().ok_or_else(|| anyhow::anyhow!("Snapshot not created"))?;
        println!("Database snapshot created at {}", snapshot_path.display());

        // Initialize storage
        let rocksdb = Arc::new(RocksDB::open(
            snapshot_path,
            &near_config.config.store,
            Mode::ReadWrite,
            Temperature::Hot,
        )?);
        let store = near_store::NodeStorage::new(rocksdb).get_hot_store();
        let mut chain_store = ChainStore::new(
            store.clone(),
            true,
            near_config.genesis.config.transaction_validity_period,
        );
        let final_head = chain_store.final_head()?;
        let block = chain_store.get_block(&final_head.prev_block_hash)?;

        // Load the old shard layout and derive a new one
        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &genesis_config, Some(home));
        let protocol_version = epoch_manager.get_epoch_protocol_version(&final_head.epoch_id)?;
        let old_shard_layout = epoch_manager.get_shard_layout(&final_head.epoch_id)?;
        println!("Old shard layout: {old_shard_layout:?}");
        let shard_layout =
            ShardLayout::derive_shard_layout(&old_shard_layout, self.boundary_account.clone());
        println!("New shard layout: {shard_layout:?}");
        let child_shards = shard_layout
            .get_children_shards_uids(self.shard_uid.shard_id())
            .ok_or_else(|| anyhow::anyhow!("Cannot get child shards"))?;
        let left_child_shard = child_shards[0];
        let right_child_shard = child_shards[1];
        println!("Left child shard: {left_child_shard}, Right child shard: {right_child_shard}");

        // Create runtime and load memtrie
        let runtime =
            NightshadeRuntime::from_config(home, store.clone(), &near_config, epoch_manager)
                .context("could not create the transaction runtime")?;
        let shard_tries = runtime.get_tries();
        println!("Loading memtrie for shard {}...", self.shard_uid);
        shard_tries.load_memtrie(&self.shard_uid, None, false)?;
        println!("Memtrie loaded");

        // Get parent shard size
        let size_calculator = MemtrieSizeCalculator::new(store.chain_store(), &shard_tries, &block);
        let parent_size = size_calculator.get_shard_trie_size(self.shard_uid)?;
        println!("Parent shard size: {parent_size} bytes");

        // Re-create epoch manager with new shard layout
        let epoch_config_store = EpochConfigStore::for_chain_id(chain_id, None)
            .ok_or_else(|| anyhow::anyhow!("Cannot load epoch config store"))?;
        let mut epoch_config = (**epoch_config_store.get_config(protocol_version)).clone();
        epoch_config.shard_layout = shard_layout;
        let epoch_config_store =
            EpochConfigStore::test_single_version(protocol_version, epoch_config);
        let epoch_manager = EpochManager::new_arc_handle_from_epoch_config_store(
            store.clone(),
            &genesis_config,
            epoch_config_store,
        );

        // Create resharding manager and prepare split params
        let sender = noop().into_multi_sender();
        let shard_tracker = ShardTracker::new(
            near_config.client_config.tracked_shards_config.clone(),
            epoch_manager.clone(),
            near_config.validator_signer.clone(),
        );
        let resharding_manager =
            ReshardingManager::new(store, epoch_manager, shard_tracker, sender);
        let chain_store_update = chain_store.store_update();
        let split_params = ReshardingSplitShardParams {
            parent_shard: self.shard_uid,
            left_child_shard,
            right_child_shard,
            boundary_account: self.boundary_account.clone(),
            resharding_block: final_head.as_ref().into(),
        };

        println!("Splitting shard {} at account {}...", self.shard_uid, self.boundary_account);
        resharding_manager.split_shard(
            chain_store_update,
            &block,
            self.shard_uid,
            shard_tries.clone(),
            split_params,
        )?;
        println!("Resharding done");

        let left_size = size_calculator.get_shard_trie_size(left_child_shard)?;
        let right_size = size_calculator.get_shard_trie_size(right_child_shard)?;
        println!("Left child state size: {left_size} bytes");
        println!("Right child state size: {right_size} bytes");
        println!(
            "WARNING: Calculated memory usages are only approximations and may differ from
            actual RAM allocation when the shards are loaded into memory."
        );

        println!("Removing database snapshot...");
        snapshot.remove()?;
        println!("Database snapshot removed");

        Ok(())
    }
}

struct MemtrieSizeCalculator<'a, 'b> {
    chain_store: ChainStoreAdapter,
    shard_tries: &'a ShardTries,
    block: &'b Block,
}

impl<'a, 'b> MemtrieSizeCalculator<'a, 'b> {
    fn new(chain_store: ChainStoreAdapter, shard_tries: &'a ShardTries, block: &'b Block) -> Self {
        Self { chain_store, shard_tries, block }
    }

    /// Get RAM usage of a shard trie
    /// Does a BFS of the whole memtrie
    fn get_shard_trie_size(&self, shard_uid: ShardUId) -> anyhow::Result<u64> {
        let chunk_extra = self.chain_store.get_chunk_extra(self.block.hash(), &shard_uid)?;
        let state_root = chunk_extra.state_root();
        println!("Shard {shard_uid}: state root: {state_root}");

        let memtries = self
            .shard_tries
            .get_memtries(shard_uid)
            .ok_or_else(|| anyhow::anyhow!("Cannot get memtrie"))?;
        let read_guard = memtries.read();
        let root_ptr = read_guard.get_root(state_root)?;

        let mut queue = VecDeque::new();
        queue.push_back(root_ptr);
        let mut total_size = 0;

        while let Some(node_ptr) = queue.pop_front() {
            total_size += node_ptr.size_of_allocation() as u64;
            match node_ptr.view() {
                MemTrieNodeView::Leaf { .. } => {}
                MemTrieNodeView::Extension { child, .. } => queue.push_back(child),
                MemTrieNodeView::Branch { children, .. }
                | MemTrieNodeView::BranchWithValue { children, .. } => {
                    queue.extend(children.iter())
                }
            }
        }

        Ok(total_size)
    }
}
