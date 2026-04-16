use anyhow::{Context, Result, bail, ensure};
use near_chain::runtime::NightshadeRuntime;
use near_chain::types::RuntimeAdapter;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::types::ShardId;
use near_replay::MemtrieShardReplayController;
use near_store::adapter::StoreAdapter;
use near_store::flat::FlatStorageStatus;
use near_store::{DBCol, NodeStorage, ShardUId};
use nearcore::{NightshadeRuntimeExt, load_config};
use std::collections::HashMap;
use std::path::Path;
use std::result::Result as StdResult;
use std::sync::Arc;

/// Determines which shards are replayed.
#[derive(Clone, Debug)]
pub enum ShardFilter {
    /// All shards in the current shard layout.
    All,
    /// Only shards whose flat storage is in the ready state.
    Available,
    /// Only the specified shard ids.
    Whitelist(Vec<ShardId>),
}

/// Replay chunks backwards from the chain head and verify results match the
/// stored ChunkExtras. Uses the database from the global `--home` argument.
#[derive(clap::Parser)]
pub struct ReplayCommand {
    /// Number of blocks to replay. If omitted, replays all available blocks.
    #[clap(long)]
    num_blocks: Option<u64>,

    /// Which shards to replay: `all`, `available`, or a comma-separated
    /// list of shard ids (e.g. `0,1,3`).
    #[clap(long, value_parser = parse_shard_filter, default_value = "all")]
    shards: ShardFilter,

    /// If set to false, log warnings on ChunkExtra mismatches instead of
    /// returning an error on the first one.
    #[clap(long, default_value_t = true)]
    fail_fast: bool,
}

impl ReplayCommand {
    pub fn run(self, home_dir: &Path, genesis_validation: GenesisValidationMode) -> Result<()> {
        let near_config =
            load_config(home_dir, genesis_validation).context("failed to load config")?;

        let opener = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
            near_config.cloud_storage_context(),
        );
        let storage = opener.open_unsafe().context("failed to open storage")?;
        let store = storage.get_hot_store();

        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );

        let runtime = NightshadeRuntime::from_config(
            home_dir,
            store.clone(),
            &near_config,
            epoch_manager.clone(),
        )
        .context("failed to create runtime")?;

        let chain_store = ChainStore::new(
            store.clone(),
            false,
            near_config.genesis.config.transaction_validity_period,
        );

        let shard_ids =
            resolve_shards(&chain_store, epoch_manager.as_ref(), &runtime, &self.shards)
                .context("failed to resolve shards")?;
        ensure!(!shard_ids.is_empty(), "no shards to replay");

        for shard_id in shard_ids {
            replay_shard(
                &chain_store,
                runtime.clone(),
                epoch_manager.clone(),
                shard_id,
                self.num_blocks,
                self.fail_fast,
            )
            .with_context(|| format!("failed to replay shard {}", shard_id))?;
        }

        Ok(())
    }
}

/// Replays chunks for a single shard, walking backwards from the chain head.
/// If `num_blocks` is `Some`, replays at most that many blocks; otherwise
/// replays until no more blocks are available. Verifies each chunk's
/// ChunkExtra against the value stored in the database and either aborts on
/// the first mismatch (`fail_fast`) or logs a warning and continues.
fn replay_shard(
    chain_store: &ChainStore,
    runtime: Arc<NightshadeRuntime>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_id: ShardId,
    num_blocks: Option<u64>,
    fail_fast: bool,
) -> Result<()> {
    let mut controller = MemtrieShardReplayController::load_memtrie(
        chain_store.clone(),
        runtime,
        epoch_manager,
        shard_id,
    )
    .context("failed to create replay controller")?;

    for i in 0..num_blocks.unwrap_or(u64::MAX) {
        let current_hash = *controller.current_block_hash();
        let height = chain_store
            .get_block(&current_hash)
            .context("failed to get current block")?
            .header()
            .height();

        let result = controller.replay_current_chunk().context("replay failed")?;
        if let Err(e) = result.verify() {
            if fail_fast {
                return Err(
                    anyhow::anyhow!(e).context(format!("chunk mismatch at height {}", height))
                );
            }
            tracing::warn!(target: "replay", %shard_id, %height, "chunk mismatch: {:#}", e);
        }
        tracing::info!(
            target: "replay",
            block = i + 1,
            %shard_id,
            %height,
            "replayed chunk"
        );

        if !controller.advance().context("advance failed")? {
            tracing::info!(
                target: "replay",
                %shard_id,
                "reached end of replay window after {} blocks",
                i + 1,
            );
            break;
        }
    }

    Ok(())
}

fn parse_shard_filter(s: &str) -> StdResult<ShardFilter, String> {
    match s {
        "all" => Ok(ShardFilter::All),
        "available" => Ok(ShardFilter::Available),
        other => {
            let shard_ids = other
                .split(',')
                .map(|id| {
                    id.trim()
                        .parse::<u64>()
                        .map(ShardId::new)
                        .map_err(|e| format!("invalid shard id '{}': {}", id.trim(), e))
                })
                .collect::<StdResult<Vec<_>, _>>()?;
            Ok(ShardFilter::Whitelist(shard_ids))
        }
    }
}

/// Resolves a `ShardFilter` into a list of shard ids based on the current
/// shard layout at the chain head. For `All` and `Whitelist`, also verifies
/// that flat storage is ready for every requested shard.
fn resolve_shards(
    chain_store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
    runtime: &NightshadeRuntime,
    filter: &ShardFilter,
) -> Result<Vec<ShardId>> {
    let head_hash = chain_store.head().context("failed to get chain head")?.last_block_hash;
    let head_block = chain_store.get_block(&head_hash).context("failed to get head block")?;
    let shard_layout = epoch_manager
        .get_shard_layout(head_block.header().epoch_id())
        .context("failed to get shard layout for head")?;

    let statuses = collect_flat_storage_statuses(runtime);

    match filter {
        ShardFilter::All => {
            let uids: Vec<_> = shard_layout.shard_uids().collect();
            for &shard_uid in &uids {
                ensure_flat_storage_ready(&statuses, shard_uid)?;
            }
            Ok(uids.into_iter().map(|uid| uid.shard_id()).collect())
        }
        ShardFilter::Available => Ok(shard_layout
            .shard_uids()
            .filter(|uid| matches!(statuses.get(uid), Some(FlatStorageStatus::Ready(_))))
            .map(|uid| uid.shard_id())
            .collect()),
        ShardFilter::Whitelist(shard_ids) => shard_ids
            .iter()
            .map(|&shard_id| {
                let shard_uid = shard_layout
                    .shard_uids()
                    .find(|uid| uid.shard_id() == shard_id)
                    .with_context(|| format!("shard id {} not in current layout", shard_id))?;
                ensure_flat_storage_ready(&statuses, shard_uid)?;
                Ok(shard_id)
            })
            .collect::<Result<Vec<_>>>(),
    }
}

fn collect_flat_storage_statuses(
    runtime: &NightshadeRuntime,
) -> HashMap<ShardUId, FlatStorageStatus> {
    let tries = runtime.get_tries();
    let flat_storage_manager = runtime.get_flat_storage_manager();
    let mut statuses: HashMap<ShardUId, FlatStorageStatus> = HashMap::new();
    for (shard_uid_bytes, _) in tries.store().store_ref().iter(DBCol::FlatStorageStatus) {
        let shard_uid = match ShardUId::try_from(shard_uid_bytes.as_ref()) {
            Ok(uid) => uid,
            Err(e) => {
                tracing::warn!(target: "replay", ?e, "failed to parse shard uid from flat storage status key");
                continue;
            }
        };
        statuses.insert(shard_uid, flat_storage_manager.get_flat_storage_status(shard_uid));
    }
    statuses
}

fn ensure_flat_storage_ready(
    statuses: &HashMap<ShardUId, FlatStorageStatus>,
    shard_uid: ShardUId,
) -> Result<()> {
    match statuses.get(&shard_uid) {
        Some(FlatStorageStatus::Ready(_)) => Ok(()),
        Some(other) => {
            bail!("flat storage not ready for shard {}: {:?}", shard_uid, other)
        }
        None => bail!("no flat storage status for shard {}", shard_uid),
    }
}
