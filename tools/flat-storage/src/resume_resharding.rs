use crate::commands::ResumeReshardingCmd;
use anyhow::anyhow;
use near_async::messaging::{CanSend, IntoMultiSender};
use near_async::time::Clock;
use near_chain::flat_storage_resharder::FlatStorageReshardingTaskResult;
use near_chain::rayon_spawner::RayonAsyncComputationSpawner;
use near_chain::resharding::types::{
    FlatStorageShardCatchupRequest, FlatStorageSplitShardRequest, MemtrieReloadRequest,
};
use near_chain::types::ChainConfig;
use near_chain::{Chain, ChainGenesis, ChainStore, DoomslugThresholdMode};
use near_chain_configs::MutableConfigValue;
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_store::adapter::StoreAdapter;
use near_store::flat::FlatStorageReshardingStatus;
use near_store::{ShardUId, StoreOpener};
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

pub(crate) fn resume_resharding(
    cmd: &ResumeReshardingCmd,
    home_dir: &PathBuf,
    config: &NearConfig,
    opener: StoreOpener,
) -> anyhow::Result<()> {
    let (chain, executor) = create_chain_and_executor(config, home_dir, opener)?;
    let resharder = chain.resharding_manager.flat_storage_resharder.clone();

    let shard_uid = ShardUId::new(3, cmd.shard_id); // version is fixed at 3 in resharding V3
    let resharding_status = get_resharding_status_for_shard(&shard_uid, &chain)?;

    chain.runtime_adapter.get_flat_storage_manager().create_flat_storage_for_shard(shard_uid)?;

    resharder.resume(shard_uid, &resharding_status)?;

    while executor.run()? {
        info!(target: "resharding", "running next task");
    }

    info!(target: "resharding", "operation completed");
    Ok(())
}

fn create_chain_and_executor(
    config: &NearConfig,
    home_dir: &Path,
    opener: StoreOpener,
) -> anyhow::Result<(Chain, Arc<SerialExecutor>)> {
    let node_storage = opener.open_in_mode(near_store::Mode::ReadWriteExisting).unwrap();
    let epoch_manager = EpochManager::new_arc_handle(
        node_storage.get_hot_store(),
        &config.genesis.config,
        Some(home_dir),
    );
    let runtime_adapter = NightshadeRuntime::from_config(
        home_dir,
        node_storage.get_hot_store(),
        &config,
        epoch_manager.clone(),
    )?;
    let shard_tracker = ShardTracker::new(
        config.client_config.tracked_shards_config.clone(),
        epoch_manager.clone(),
    );
    let chain_genesis = ChainGenesis::new(&config.genesis.config);
    let chain_config = ChainConfig {
        save_trie_changes: config.client_config.save_trie_changes,
        background_migration_threads: config.client_config.client_background_migration_threads,
        resharding_config: config.client_config.resharding_config.clone(),
    };
    let executor = Arc::new(SerialExecutor::new(ChainStore::new(
        node_storage.get_hot_store(),
        false,
        chain_genesis.transaction_validity_period,
    )));
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
        executor.as_multi_sender(),
    )?;
    Ok((chain, executor))
}

fn get_resharding_status_for_shard(
    shard_uid: &ShardUId,
    chain: &Chain,
) -> anyhow::Result<FlatStorageReshardingStatus> {
    use near_store::flat::FlatStorageStatus::*;
    let flat_storage_status =
        chain.runtime_adapter.store().flat_store().get_flat_storage_status(*shard_uid)?;
    match &flat_storage_status {
        Disabled | Empty | Creation(_) | Ready(_) => Err(anyhow!(
            "resharding is not in progress! flat storage status: {:?}",
            flat_storage_status
        )),
        Resharding(status) => Ok(status.clone()),
    }
}

/// Executor that runs tasks in sequence.
struct SerialExecutor {
    chain_store: Arc<Mutex<ChainStore>>,
    tasks: Mutex<VecDeque<Box<dyn Fn(&ChainStore) -> FlatStorageReshardingTaskResult + Send>>>,
}

impl SerialExecutor {
    fn new(chain_store: ChainStore) -> Self {
        Self { chain_store: Arc::new(Mutex::new(chain_store)), tasks: Mutex::new(VecDeque::new()) }
    }

    /// Runs the next task.
    ///
    /// Returns false if there are no tasks.
    fn run(&self) -> anyhow::Result<bool> {
        let task = self.tasks.lock().pop_front();
        match task {
            Some(task) => {
                match task(&self.chain_store.lock()) {
                    FlatStorageReshardingTaskResult::Successful { num_batches_done } => {
                        info!(target: "resharding", num_batches_done, "task completed");
                    }
                    FlatStorageReshardingTaskResult::Failed => {
                        return Err(anyhow!("resharding task has failed!"));
                    }
                    FlatStorageReshardingTaskResult::Cancelled => {
                        info!(target: "resharding", "task cancelled")
                    }
                    FlatStorageReshardingTaskResult::Postponed => {
                        info!(target: "resharding", "task postponed - retrying");
                        self.tasks.lock().push_back(task);
                    }
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

impl CanSend<FlatStorageSplitShardRequest> for SerialExecutor {
    fn send(&self, msg: FlatStorageSplitShardRequest) {
        let task =
            Box::new(move |chain_store: &ChainStore| msg.resharder.split_shard_task(chain_store));
        self.tasks.lock().push_back(task);
    }
}

impl CanSend<FlatStorageShardCatchupRequest> for SerialExecutor {
    fn send(&self, msg: FlatStorageShardCatchupRequest) {
        let task = Box::new(move |chain_store: &ChainStore| {
            msg.resharder.shard_catchup_task(msg.shard_uid, chain_store)
        });
        self.tasks.lock().push_back(task);
    }
}

impl CanSend<MemtrieReloadRequest> for SerialExecutor {
    fn send(&self, _: MemtrieReloadRequest) {
        // no op
    }
}
