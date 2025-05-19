use crate::utils::open_rocksdb;
use anyhow::Context;
use bytesize::ByteSize;
use near_chain::resharding::event_type::ReshardingSplitShardParams;
use near_chain::resharding::flat_storage_resharder::FlatStorageResharder;
use near_chain::types::RuntimeAdapter;
use near_chain_configs::{GenesisValidationMode, ReshardingHandle};
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_o11y::default_subscriber;
use near_o11y::env_filter::EnvFilterBuilder;
use near_primitives::block::Tip;
use near_primitives::block_header::BlockHeader;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::EpochConfig;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{
    AccountId, BlockHeight, EpochId, ProtocolVersion, ShardId, ValidatorInfoIdentifier,
};
use near_primitives::views::EpochValidatorInfo;
use near_store::adapter::StoreAdapter;
use near_store::flat::FlatStorageStatus;
use near_store::trie::mem::loading::{get_state_root, load_trie_from_flat_state};
use near_store::{DBCol, HEAD_KEY, ShardUId, Store, StoreUpdate};
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

        let rocksdb = Arc::new(open_rocksdb(home, near_store::Mode::ReadOnly)?);
        let store = near_store::NodeStorage::new(rocksdb).get_hot_store();
        let final_head = store.chain_store().final_head()?;

        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &genesis_config, Some(home));
        let old_shard_layout = epoch_manager.get_shard_layout(&final_head.epoch_id)?;

        let runtime =
            NightshadeRuntime::from_config(home, store.clone(), &near_config, epoch_manager)
                .context("could not create the transaction runtime")?;

        let shard_layout =
            ShardLayout::derive_shard_layout(&old_shard_layout, self.boundary_account.clone());
        let child_shards = shard_layout
            .get_children_shards_uids(self.shard_uid.shard_id())
            .ok_or_else(|| anyhow::anyhow!("Cannot get child shards"))?;
        let left_child_shard = child_shards[0];
        let right_child_shard = child_shards[1];

        let epoch_manager = DummyEpochManager::new(shard_layout);
        let resharding_handle = ReshardingHandle::new();
        let mut resharding_config_override = near_config.config.resharding_config;
        resharding_config_override.batch_size = ByteSize::tb(1);
        let resharding_config = near_config.client_config.resharding_config.clone();
        resharding_config.update(resharding_config_override);
        let resharder =
            FlatStorageResharder::new(epoch_manager, runtime, resharding_handle, resharding_config);

        let split_params = ReshardingSplitShardParams {
            parent_shard: self.shard_uid,
            left_child_shard,
            right_child_shard,
            boundary_account: self.boundary_account.clone(),
            resharding_block: final_head.into(),
        };

        println!("Splitting shard {} at account {}...", self.shard_uid, self.boundary_account);
        resharder.start_resharding_blocking(&split_params)?;
        println!("Resharding done");

        let left_size = get_shard_trie_size(&store, left_child_shard)?;
        let right_size = get_shard_trie_size(&store, right_child_shard)?;
        println!("Left child size: {left_size} bytes");
        println!("Right child size: {right_size} bytes");

        Ok(())
    }
}

fn get_shard_trie_size(store: &Store, shard_uid: ShardUId) -> anyhow::Result<u64> {
    let flat_storage_status = store.flat_store().get_flat_storage_status(shard_uid)?;
    let flat_head = match flat_storage_status {
        FlatStorageStatus::Ready(status) => status.flat_head,
        _ => anyhow::bail!("Flat storage status not ready"),
    };
    let state_root = get_state_root(&store, flat_head.hash, shard_uid)?;
    let memtries =
        load_trie_from_flat_state(&store, shard_uid, state_root, flat_head.height, true)?;
    let root = memtries.get_root(&state_root)?.view();
    Ok(root.memory_usage())
}

struct DummyEpochManager {
    shard_layout: ShardLayout,
}

impl DummyEpochManager {
    fn new(shard_layout: ShardLayout) -> Arc<Self> {
        Arc::new(Self { shard_layout })
    }
}

impl EpochManagerAdapter for DummyEpochManager {
    fn get_epoch_config_from_protocol_version(&self, _: ProtocolVersion) -> EpochConfig {
        unimplemented!()
    }

    fn get_block_info(&self, _hash: &CryptoHash) -> Result<Arc<BlockInfo>, EpochError> {
        unimplemented!()
    }

    fn get_epoch_info(&self, _: &EpochId) -> Result<Arc<EpochInfo>, EpochError> {
        unimplemented!()
    }

    fn get_epoch_start_from_epoch_id(&self, _: &EpochId) -> Result<BlockHeight, EpochError> {
        unimplemented!()
    }

    fn num_total_parts(&self) -> usize {
        unimplemented!()
    }

    fn get_next_epoch_id(&self, _: &CryptoHash) -> Result<EpochId, EpochError> {
        Ok(Default::default())
    }

    fn get_shard_layout(&self, _: &EpochId) -> Result<ShardLayout, EpochError> {
        Ok(self.shard_layout.clone())
    }

    fn get_validator_info(
        &self,
        _: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, EpochError> {
        unimplemented!()
    }

    fn add_validator_proposals(
        &self,
        _: BlockInfo,
        _: CryptoHash,
    ) -> Result<StoreUpdate, EpochError> {
        unimplemented!()
    }

    fn init_after_epoch_sync(
        &self,
        _: &mut StoreUpdate,
        _: BlockInfo,
        _: BlockInfo,
        _: BlockInfo,
        _: &EpochId,
        _: EpochInfo,
        _: &EpochId,
        _: EpochInfo,
        _: &EpochId,
        _: EpochInfo,
    ) -> Result<(), EpochError> {
        unimplemented!()
    }
}
