use anyhow::{Context, Result};
use clap::builder::{PossibleValuesParser, TypedValueParser};
use near_chain::ChainStore;
use near_chain::runtime::NightshadeRuntime;
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::EpochManager;
use near_primitives::types::ShardId;
use near_replay::{
    CombinedDatabase, ReplayStorageMode, SequentialChunksReplayController, ShardFilter,
};
use near_store::{NodeStorage, Store, StoreConfig};
use nearcore::{NightshadeRuntimeExt, load_config};
use std::path::{Path, PathBuf};

/// Replay chunks from a database snapshot and verify results match the stored
/// ChunkExtras.
///
/// The node config is loaded from the global `--home` argument. The actual
/// database data paths are provided separately via `--main-db-dir` and
/// `--storage-init-db-dir` so that the tool can read from arbitrary
/// locations without requiring matching config files there.
#[derive(clap::Parser)]
pub struct ReplayCommand {
    /// Directory containing the main database (the one containing the
    /// blocks, chunks, and trie data to replay).
    #[clap(long, value_parser)]
    main_db_dir: PathBuf,

    /// Directory containing the storage init database (an earlier snapshot
    /// whose flat storage head is at or before the replay range). Used to
    /// initialize memtries / flat storage prior to replay.
    #[clap(long, value_parser)]
    storage_init_db_dir: PathBuf,

    /// Number of blocks to replay.
    #[clap(long)]
    num_blocks: u64,

    /// Storage mode used during replay.
    #[clap(long, value_parser = storage_mode_parser(), default_value = "memtries")]
    storage_mode: ReplayStorageMode,

    /// Which shards to replay: `all`, `available`, or a comma-separated
    /// list of shard ids (e.g. `0,1,3`).
    #[clap(long, value_parser = parse_shard_filter, default_value = "all")]
    shards: ShardFilter,

    /// If set, return an error on the first ChunkExtra mismatch instead of
    /// continuing to log warnings.
    #[clap(long)]
    fail_fast: bool,
}

impl ReplayCommand {
    pub fn run(self, home_dir: &Path, genesis_validation: GenesisValidationMode) -> Result<()> {
        let near_config =
            load_config(home_dir, genesis_validation).context("failed to load config")?;

        let main_store = open_store_at(&self.main_db_dir, &near_config.config.store)?;
        let storage_init_store =
            open_store_at(&self.storage_init_db_dir, &near_config.config.store)?;

        let combined_store = CombinedDatabase::new(storage_init_store, main_store.clone());

        let epoch_manager = EpochManager::new_arc_handle(
            main_store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );

        let runtime = NightshadeRuntime::from_config(
            home_dir,
            combined_store,
            &near_config,
            epoch_manager.clone(),
        )
        .context("failed to create runtime")?;

        let chain_store = ChainStore::new(
            main_store,
            false,
            near_config.genesis.config.transaction_validity_period,
        );

        let mut controller = SequentialChunksReplayController::new(
            chain_store,
            runtime,
            epoch_manager,
            self.storage_mode,
            self.shards,
        )
        .context("failed to create replay controller")?;

        for i in 0..self.num_blocks {
            let result = controller.replay_current_block().context("replay failed")?;
            let height = result.block_header.height();
            for chunk in &result.chunk_results {
                if let Err(e) = chunk.verify() {
                    if self.fail_fast {
                        return Err(e.context(format!("chunk mismatch at height {}", height)));
                    }
                    tracing::warn!(target: "replay", %height, "chunk mismatch: {:#}", e);
                }
            }
            tracing::info!(
                target: "replay",
                block = i + 1,
                %height,
                chunks = result.chunk_results.len(),
                "replayed block"
            );
            if !controller.advance().context("advance failed")? {
                tracing::info!(target: "replay", "reached end of chain after {} blocks", i + 1);
                break;
            }
        }

        Ok(())
    }
}

/// Opens a store at the given directory using the provided store config,
/// overriding the path to point at `db_dir`. Uses `open_unsafe` to bypass
/// the DB version check since we want to replay historical data without
/// running migrations.
fn open_store_at(db_dir: &Path, store_config: &StoreConfig) -> Result<Store> {
    let mut store_config = store_config.clone();
    store_config.path = Some(db_dir.to_path_buf());
    let opener = NodeStorage::opener(db_dir, &store_config, None, None);
    let storage = opener.open_unsafe().context("failed to open storage")?;
    Ok(storage.get_hot_store())
}

fn storage_mode_parser() -> impl TypedValueParser<Value = ReplayStorageMode> {
    PossibleValuesParser::new(["memtries", "flat-state"]).map(|s| match s.as_str() {
        "memtries" => ReplayStorageMode::Memtries,
        "flat-state" => ReplayStorageMode::FlatState,
        _ => unreachable!(),
    })
}

fn parse_shard_filter(s: &str) -> std::result::Result<ShardFilter, String> {
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
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(ShardFilter::Whitelist(shard_ids))
        }
    }
}
