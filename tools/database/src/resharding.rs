#![allow(unused)]

use std::path::Path;
use std::sync::Arc;

use near_async::time::Clock;
use near_chain::rayon_spawner::RayonAsyncComputationSpawner;
use near_chain::resharding::ReshardingResponse;
use near_chain::types::ChainConfig;
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::GenesisId;
use near_primitives::{hash::CryptoHash, types::EpochId};
use near_store::genesis::initialize_sharded_genesis_state;
use nearcore::NightshadeRuntimeExt;
use nearcore::{open_storage, NearConfig, NightshadeRuntime};

#[derive(clap::Args)]
pub(crate) struct ReshardingCommand {
    #[clap(long)]
    block_hash: CryptoHash,

    #[clap(long)]
    shard_id: u64,
}

impl ReshardingCommand {
    pub(crate) fn run(&self, mut config: NearConfig, home_dir: &Path) -> anyhow::Result<()> {
        println!("hello world!");

        let storage = open_storage(home_dir, &mut config)?;
        let epoch_manager =
            EpochManager::new_arc_handle(storage.get_hot_store(), &config.genesis.config);
        let genesis_epoch_config = epoch_manager.get_epoch_config(&EpochId::default())?;
        // Initialize genesis_state in store either from genesis config or dump before other components.
        // We only initialize if the genesis state is not already initialized in store.
        // This sets up genesis_state_roots and genesis_hash in store.
        initialize_sharded_genesis_state(
            storage.get_hot_store(),
            &config.genesis,
            &genesis_epoch_config,
            Some(home_dir),
        );

        let shard_tracker = ShardTracker::new(
            TrackedConfig::from_config(&config.client_config),
            epoch_manager.clone(),
        );
        let runtime_adapter = NightshadeRuntime::from_config(
            home_dir,
            storage.get_split_store().unwrap(),
            &config,
            epoch_manager.clone(),
        )?;

        let split_store = storage.get_split_store();

        let chain_genesis = ChainGenesis::new(&config.genesis.config);
        let genesis_block = Chain::make_genesis_block(
            epoch_manager.as_ref(),
            runtime_adapter.as_ref(),
            &chain_genesis,
        )?;
        let genesis_id = GenesisId {
            chain_id: config.client_config.chain_id.clone(),
            hash: *genesis_block.header().hash(),
        };

        let client_config = config.client_config;
        let chain_config = ChainConfig {
            save_trie_changes: client_config.save_trie_changes,
            background_migration_threads: client_config.client_background_migration_threads,
            resharding_config: client_config.resharding_config.clone(),
        };

        let mut chain = Chain::new(
            Clock::real(),
            epoch_manager.clone(),
            shard_tracker.clone(),
            runtime_adapter.clone(),
            &chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            chain_config,
            None,
            Arc::new(RayonAsyncComputationSpawner),
            None,
        )
        .unwrap();

        /* *************** */
        /* RESHARING STUFF */
        /* *************** */

        let resharding_request = chain.custom_build_state_for_resharding_preprocessing(
            &self.block_hash,
            &self.block_hash,
            self.shard_id,
        )?;

        let shard_uid = resharding_request.shard_uid;

        let response = Chain::build_state_for_split_shards(resharding_request);
        let ReshardingResponse { shard_id, sync_hash, new_state_roots } = response;

        chain.build_state_for_split_shards_postprocessing(
            shard_uid,
            &sync_hash,
            new_state_roots?,
        )?;

        Ok(())
    }
}
