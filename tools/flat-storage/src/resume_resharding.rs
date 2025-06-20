use crate::commands::ResumeReshardingCmd;
use near_chain::resharding::flat_storage_resharder::FlatStorageResharder;
use near_chain::resharding::trie_state_resharder::{ResumeAllowed, TrieStateResharder};
use near_chain::types::RuntimeAdapter;
use near_chain_configs::ReshardingHandle;
use near_epoch_manager::EpochManager;
use near_store::{ShardUId, StoreOpener};
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt};
use std::path::PathBuf;

pub(crate) fn resume_resharding(
    cmd: &ResumeReshardingCmd,
    home_dir: &PathBuf,
    config: &NearConfig,
    opener: StoreOpener,
) -> anyhow::Result<()> {
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

    let flat_storage_resharder = FlatStorageResharder::new(
        epoch_manager,
        runtime_adapter.clone(),
        ReshardingHandle::new(),
        config.client_config.resharding_config.clone(),
    );

    let shard_uid = ShardUId::new(3, cmd.shard_id); // version is fixed at 3 in resharding V3

    runtime_adapter.get_flat_storage_manager().create_flat_storage_for_shard(shard_uid)?;

    flat_storage_resharder.resume(shard_uid)?;

    tracing::info!(target: "resharding", "FlatStorageResharder completed");

    let trie_state_resharder = TrieStateResharder::new(
        runtime_adapter,
        ReshardingHandle::new(),
        config.client_config.resharding_config.clone(),
        ResumeAllowed::Yes,
    );
    trie_state_resharder.resume(shard_uid)?;

    tracing::info!(target: "resharding", "TrieStateResharder completed");

    Ok(())
}
