use core::time;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use near_async::time::Clock;
use near_chain::rayon_spawner::RayonAsyncComputationSpawner;
use near_chain::resharding::v2::ReshardingResponse;
use near_chain::types::ChainConfig;
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
use near_chain_configs::MutableConfigValue;
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::types::EpochId;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::db::{MixedDB, ReadOrder, RecoveryDB, RocksDB};
use near_store::genesis::initialize_sharded_genesis_state;
use near_store::{Mode, NodeStorage, Store, Temperature};
use nearcore::NightshadeRuntimeExt;
use nearcore::{open_storage, NearConfig, NightshadeRuntime};

#[derive(clap::Args)]
pub(crate) struct ReshardingV2Command {
    /// The block height at which resharding V2 is performed.
    /// This should be, usually, the block before shard layout has changed.
    /// Keep in mind that resharding is done on the post state root.
    #[clap(long)]
    height: BlockHeight,

    /// The shard id before resharding.
    #[clap(long)]
    shard_id: ShardId,

    /// Path to write the new trie nodes created by the resharding operation.
    #[clap(long, group("output"))]
    write_path: Option<PathBuf>,

    /// Restore potentially missing trie nodes in cold database. This operation is idempotent.
    #[clap(long, group("output"))]
    restore: bool,
}

impl ReshardingV2Command {
    pub(crate) fn run(&self, mut config: NearConfig, home_dir: &Path) -> anyhow::Result<()> {
        Self::check_resharding_config(&mut config);

        let mut chain = self.get_chain(config, home_dir)?;

        let block_hash = *chain.get_block_by_height(self.height)?.hash();

        let resharding_request =
            chain.custom_build_state_for_resharding_v2_preprocessing(&block_hash, self.shard_id)?;

        let response = Chain::build_state_for_split_shards_v2(resharding_request);
        let ReshardingResponse { sync_hash, new_state_roots: state_roots, .. } = response;

        if self.restore {
            // In restore mode print database write statistics.
            chain.runtime_adapter.store().get_store_statistics().map(|stats| {
                stats.data.iter().for_each(|(metric, values)| {
                    tracing::info!(target: "resharding", %metric, ?values);
                })
            });
        }

        let state_roots = state_roots?;
        tracing::info!(target: "resharding", ?state_roots, "state roots");

        chain.custom_build_state_for_split_shards_v2_postprocessing(&sync_hash, state_roots)?;

        Ok(())
    }

    fn get_store(&self, home_dir: &Path, config: &mut NearConfig) -> Result<Store, anyhow::Error> {
        // Open hot and cold as usual.
        let storage = open_storage(home_dir, config)?;

        let cold_db = storage
            .cold_db()
            .expect("resharding tool can be used only on archival nodes")
            .to_owned();
        let hot_db = storage.into_inner(Temperature::Hot);

        // We need something similar to splitDB so that it correctly handles reads of missing
        // values in the columns that are not in the cold db. MixedDB let us choose read order.
        let split_db = MixedDB::new(hot_db, cold_db.clone(), ReadOrder::WriteDBFirst);

        let store = if self.restore {
            // In 'restore' mode all changes are written directly into the recovery DB built upon the cold DB.
            let recovery_db = Arc::new(RecoveryDB::new(cold_db));

            // Prepare the full mixed db.
            // It will read, in order, from hot, cold and recovery.
            // It will write only to the recovery db.
            let mixed_db = MixedDB::new(split_db, recovery_db, ReadOrder::ReadDBFirst);

            // The only way to create a Store is to go through NodeStorage.
            let storage = NodeStorage::new(mixed_db);
            storage.get_hot_store()
        } else {
            let write_path =
                self.write_path.as_ref().expect("write path must be set when not in recovery mode");

            // Open write db.
            let write_path = if write_path.is_absolute() {
                PathBuf::from(&write_path)
            } else {
                home_dir.join(&write_path)
            };
            let write_path = write_path.as_path();
            let write_config = &config.config.store;
            let write_db =
                RocksDB::open(write_path, write_config, Mode::ReadWrite, Temperature::Hot)?;
            let write_db = Arc::new(write_db);

            // Prepare the full mixed db.
            // It will read, in order, from write, hot and cold.
            // It will write only to the write db.
            let mixed_db = MixedDB::new(split_db, write_db, ReadOrder::WriteDBFirst);

            // The only way to create a Store is to go through NodeStorage.
            let storage = NodeStorage::new(mixed_db);
            storage.get_hot_store()
        };
        Ok(store)
    }

    fn get_chain(&self, mut config: NearConfig, home_dir: &Path) -> Result<Chain, anyhow::Error> {
        let store = self.get_store(home_dir, &mut config)?;

        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &config.genesis.config);
        let genesis_epoch_config = epoch_manager.get_epoch_config(&EpochId::default())?;
        initialize_sharded_genesis_state(
            store.clone(),
            &config.genesis,
            &genesis_epoch_config,
            Some(home_dir),
        );
        let shard_tracker = ShardTracker::new(
            TrackedConfig::from_config(&config.client_config),
            epoch_manager.clone(),
        );
        let runtime_adapter =
            NightshadeRuntime::from_config(home_dir, store, &config, epoch_manager.clone())?;
        let chain_genesis = ChainGenesis::new(&config.genesis.config);
        let client_config = config.client_config;
        let chain_config = ChainConfig {
            save_trie_changes: client_config.save_trie_changes,
            background_migration_threads: client_config.client_background_migration_threads,
            resharding_config: client_config.resharding_config,
        };
        let chain = Chain::new(
            Clock::real(),
            epoch_manager,
            shard_tracker,
            runtime_adapter,
            &chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            chain_config,
            None,
            Arc::new(RayonAsyncComputationSpawner),
            MutableConfigValue::new(None, "validator_signer"),
        )
        .unwrap();
        Ok(chain)
    }

    // Rely on the regular config but make sure it's configured correctly for
    // the on demand resharding. It's executed while the node is not running so
    // it should be as fast as possible - there should be no throttling.
    fn check_resharding_config(config: &mut NearConfig) {
        if config.config.resharding_config.batch_delay != time::Duration::ZERO {
            panic!("batch_delay must be zero for on demand resharding");
        };

        if config.client_config.resharding_config.get().batch_delay != time::Duration::ZERO {
            panic!("batch_delay must be zero for on demand resharding");
        };
    }
}
