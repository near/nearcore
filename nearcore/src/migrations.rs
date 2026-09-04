use crate::NearConfig;
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain::{Error, LatestKnown};
use near_chain_configs::GenesisConfig;
use near_crypto::Signature;
use near_epoch_manager::epoch_sync;
use near_primitives::chains::MAINNET;
use near_primitives::epoch_sync::EpochSyncProof;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::DelayedReceiptIndices;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{BlockHeight, BlockHeightDelta, EpochId, ShardId, StateChangeCause};
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{BlockHeaderInnerLiteView, LightClientBlockView};
use near_store::adapter::StoreAdapter;
use near_store::adapter::trie_store::TrieStoreUpdateAdapter;
use near_store::archive::cold_storage::{join_two_keys, rc_aware_set};
use near_store::db::metadata::{DB_VERSION, DbVersion, MIN_SUPPORTED_DB_VERSION};
use near_store::db::{ColdDB, DBTransaction, Database};
use near_store::flat::FlatStorageManager;
use near_store::{
    DBCol, LATEST_KNOWN_KEY, ShardTries, ShardUId, StateSnapshotConfig, Store, StoreConfig,
    TrieChanges, TrieConfig, TrieUpdate, get_genesis_height, set,
};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::thread;

const BATCH_SIZE: u64 = 100_000;
const MAX_SST_FILE_SIZE: u64 = 256 * 1024 * 1024; // 256 MB

pub(super) struct Migrator<'a> {
    config: &'a NearConfig,
    home_dir: &'a Path,
}

impl<'a> Migrator<'a> {
    pub fn new(config: &'a NearConfig, home_dir: &'a Path) -> Self {
        Self { config, home_dir }
    }
}

impl<'a> near_store::StoreMigrator for Migrator<'a> {
    fn check_support(&self, version: DbVersion) -> Result<(), &'static str> {
        match version {
            0..MIN_SUPPORTED_DB_VERSION => Err("database version is too old and not supported"),
            MIN_SUPPORTED_DB_VERSION..DB_VERSION => Ok(()),
            _ => unreachable!(),
        }
    }

    fn migrate(
        &self,
        hot_store: &Store,
        cold_db: Option<&ColdDB>,
        version: DbVersion,
        is_snapshot: bool,
    ) -> anyhow::Result<()> {
        match version {
            0..MIN_SUPPORTED_DB_VERSION => unreachable!(),
            45 => Ok(()), // DBCol::StatePartsApplied column added, no need to perform a migration
            46 => near_chain::resharding::migrations::migrate_46_to_47(
                hot_store,
                cold_db,
                &self.config.genesis.config,
                &self.config.config.store,
            ),
            47 => migrate_47_to_48(cold_db, &self.config.genesis.config, &self.config.config.store),
            48 => migrate_48_to_49(
                hot_store,
                cold_db,
                self.config.genesis.config.transaction_validity_period,
                self.home_dir,
                self.config.config.cold_store.as_ref(),
                is_snapshot,
            ),
            49 => Ok(()), // DBCol::ChunkProducers column added, no need to perform a migration
            50 => migrate_50_to_51(hot_store),
            DB_VERSION.. => unreachable!(),
        }
    }
}

/// Migrates the database from version 47 to 48.
///
/// This migration addresses the data loss that occurred during Resharding V2
/// on March 21, 2024. The backfill process was incomplete, and some data
/// was still missing at block height 115185108.
///
/// Note: This migration applies only to the cold store and is specific
/// to the mainnet resharding event.
fn migrate_47_to_48(
    cold_db: Option<&ColdDB>,
    genesis_config: &GenesisConfig,
    store_config: &StoreConfig,
) -> anyhow::Result<()> {
    tracing::info!(target: "migrations", "starting migration from DB version 47 to 48");

    let Some(cold_db) = cold_db else {
        tracing::info!(target: "migrations", "skipping migration 47->48 for hot store only");
        return Ok(());
    };

    // Current migration is targeted only for mainnet
    if genesis_config.chain_id != MAINNET {
        tracing::info!(target: "migrations", chain_id = ?genesis_config.chain_id, "skipping migration 47->48");
        return Ok(());
    }

    tracing::info!(target: "migrations", "starting migration 47->48 for cold store");

    let cold_store = cold_db.as_store();
    let tries = ShardTries::new(
        cold_store.trie_store(),
        TrieConfig::from_store_config(store_config),
        FlatStorageManager::new(cold_store.flat_store()),
        StateSnapshotConfig::Disabled,
    );

    // We ignore the store update, as we need to construct a transaction manually from trie changes.
    let trie_changes = recover_shard_1_at_block_height_115185108(
        &tries,
        &mut cold_store.trie_store().store_update(),
    )?;
    let mut transaction = DBTransaction::new();
    let child_shard_uid = ShardUId::new(3, ShardId::new(1));
    for op in trie_changes.insertions() {
        let key = join_two_keys(&child_shard_uid.to_bytes(), op.hash().as_bytes());
        let value = op.payload().to_vec();
        rc_aware_set(&mut transaction, DBCol::State, key, value);
    }
    tracing::info!(target: "migrations", "Writing changes to the database");
    cold_db.write(transaction);
    Ok(())
}

fn recover_shard_1_at_block_height_115185108(
    tries: &ShardTries,
    store_update: &mut TrieStoreUpdateAdapter,
) -> anyhow::Result<TrieChanges> {
    let parent_shard_uid = ShardUId::new(2, ShardId::new(1));
    let child_shard_uid = ShardUId::new(3, ShardId::new(1));

    // cspell:disable-next-line
    let prev_state_root = CryptoHash::from_str("FHagbcDYMBHFe9xc1fpMXBgt54hgnehE4ZLntBevGPRs")
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let new_delayed_receipt_indices =
        DelayedReceiptIndices { first_index: 23, next_available_index: 23 };
    let expected_new_state_root =
    // cspell:disable-next-line
        CryptoHash::from_str("8pupvmM9yj2dhSUBHA59epspyxvGzpyQmiwub6BbMwKZ")
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let prev_trie = tries.get_trie_for_shard(parent_shard_uid, prev_state_root);
    let mut trie_update = TrieUpdate::new(prev_trie);
    set(&mut trie_update, TrieKey::DelayedReceiptIndices, &new_delayed_receipt_indices);
    trie_update.commit(StateChangeCause::_UnusedReshardingV2);
    let trie_changes = trie_update.finalize()?.trie_changes;
    let new_state_root = tries.apply_all(&trie_changes, child_shard_uid, store_update);
    if new_state_root != expected_new_state_root {
        return Err(anyhow::anyhow!(
            "New state root {} does not match expected state root: {}",
            new_state_root,
            expected_new_state_root
        ));
    }
    Ok(trie_changes)
}

/// `BlockHeaderInnerLiteView` before `chunk_execution_root` was appended to it.
#[derive(BorshSerialize, BorshDeserialize)]
struct BlockHeaderInnerLiteViewV50 {
    height: BlockHeight,
    epoch_id: CryptoHash,
    next_epoch_id: CryptoHash,
    prev_state_root: CryptoHash,
    outcome_root: CryptoHash,
    timestamp: u64,
    timestamp_nanosec: u64,
    next_bp_hash: CryptoHash,
    block_merkle_root: CryptoHash,
}

/// A `DBCol::EpochLightClientBlocks` row as every released binary wrote it.
#[derive(BorshSerialize, BorshDeserialize)]
struct LightClientBlockViewV50 {
    prev_block_hash: CryptoHash,
    next_block_inner_hash: CryptoHash,
    inner_lite: BlockHeaderInnerLiteViewV50,
    inner_rest_hash: CryptoHash,
    next_bps: Option<Vec<ValidatorStakeView>>,
    approvals_after_next: Vec<Option<Box<Signature>>>,
}

impl From<LightClientBlockViewV50> for LightClientBlockView {
    fn from(row: LightClientBlockViewV50) -> Self {
        LightClientBlockView {
            prev_block_hash: row.prev_block_hash,
            next_block_inner_hash: row.next_block_inner_hash,
            inner_lite: BlockHeaderInnerLiteView {
                height: row.inner_lite.height,
                epoch_id: row.inner_lite.epoch_id,
                next_epoch_id: row.inner_lite.next_epoch_id,
                prev_state_root: row.inner_lite.prev_state_root,
                outcome_root: row.inner_lite.outcome_root,
                timestamp: row.inner_lite.timestamp,
                timestamp_nanosec: row.inner_lite.timestamp_nanosec,
                next_bp_hash: row.inner_lite.next_bp_hash,
                block_merkle_root: row.inner_lite.block_merkle_root,
                // A row this old predates spice, so the block it describes committed
                // no execution results.
                chunk_execution_root: None,
            },
            inner_rest_hash: row.inner_rest_hash,
            next_bps: row.next_bps,
            approvals_after_next: row.approvals_after_next,
        }
    }
}

/// Migrates the database from version 50 to 51.
///
/// `BlockHeaderInnerLiteView` gained `chunk_execution_root`, which changed the borsh
/// layout of `LightClientBlockView` and so of `DBCol::EpochLightClientBlocks`. That
/// column is written once per epoch, never rewritten and never garbage collected, so
/// rows an earlier binary wrote stay in the old layout until this rewrites them.
///
/// Hot store only: the column is not copied to cold storage.
fn migrate_50_to_51(hot_store: &Store) -> anyhow::Result<()> {
    tracing::info!(target: "migrations", "starting migration from DB version 50 to 51");

    let rows: Vec<_> = hot_store.iter(DBCol::EpochLightClientBlocks).collect();
    let mut store_update = hot_store.store_update();
    let mut rewritten = 0;
    let mut already_current = 0;
    for (key, value) in rows {
        let epoch_id = CryptoHash::try_from(key.as_ref())
            .map(EpochId)
            .map_err(|err| anyhow::anyhow!("epoch light client block key is not a hash: {err}"))?;

        // A row is unambiguous only if exactly one layout reads it whole. Borsh rejects
        // trailing bytes, so the wrong layout almost always fails, but say so rather
        // than guess on a row where it would not.
        let old = LightClientBlockViewV50::try_from_slice(&value).ok();
        let current = LightClientBlockView::try_from_slice(&value).ok();
        match (old, current) {
            (Some(old), None) => {
                store_update.set_ser(
                    DBCol::EpochLightClientBlocks,
                    &key,
                    &LightClientBlockView::from(old),
                );
                rewritten += 1;
            }
            (None, Some(_)) => already_current += 1,
            (Some(_), Some(_)) => {
                anyhow::bail!("epoch light client block {epoch_id:?} reads in both layouts")
            }
            (None, None) => {
                anyhow::bail!("epoch light client block {epoch_id:?} reads in neither layout")
            }
        }
    }
    store_update.commit();

    tracing::info!(target: "migrations", rewritten, already_current, "completed migration from DB version 50 to 51");
    Ok(())
}

/// This migration does three things:
/// 1. Copy block headers from hot_store to cold_db (if cold_db is present)
/// 2. Generate and save the compressed epoch sync proof
/// 3. Clear the block headers from genesis to tail in hot_store
fn migrate_48_to_49(
    hot_store: &Store,
    cold_db: Option<&ColdDB>,
    transaction_validity_period: BlockHeightDelta,
    home_dir: &Path,
    cold_store_config: Option<&StoreConfig>,
    is_snapshot: bool,
) -> anyhow::Result<()> {
    tracing::info!(target: "migrations", "starting migration from DB version 48 to 49");

    // State snapshot DBs only contain flat storage columns and lack the
    // epoch/chain data that every step of this migration requires. Skip them.
    if is_snapshot {
        tracing::info!(target: "migrations", "state snapshot DB, skipping chain-dependent migration steps");
        return Ok(());
    }

    // Fresh nodes and forknet-initialized nodes have BlockMisc cleared, so
    // HEAD is absent; nodes that only produced blocks in the genesis epoch
    // have HEAD set but head.epoch_id == EpochId::default(). In both cases
    // there are no block headers to copy, no epoch sync proof to derive, and
    // nothing to verify or delete.
    match hot_store.chain_store().head() {
        Ok(head) if head.epoch_id == EpochId::default() => {
            tracing::info!(target: "migrations", "chain is in the genesis epoch, skipping chain-dependent migration steps");
            return Ok(());
        }
        Err(Error::DBNotFoundErr(_)) => {
            tracing::info!(target: "migrations", "chain head not set (fresh/forknet DB), skipping chain-dependent migration steps");
            return Ok(());
        }
        Ok(_) => {}
        Err(err) => return Err(err.into()),
    }

    if let Some(cold_db) = cold_db {
        let cold_store_config =
            cold_store_config.expect("cold_store config must be present when cold_db exists");
        copy_block_headers_to_cold_db(hot_store, cold_db, home_dir, cold_store_config)?;
    }

    update_epoch_sync_proof(hot_store.clone(), transaction_validity_period)?;
    verify_block_headers(hot_store)?;
    delete_old_block_headers(hot_store)?;
    Ok(())
}

/// Copies block headers from hot store to cold DB via SST bulk ingestion.
///
/// Writes the column to SST files on the cold store's filesystem, then ingests
/// them with move_files=true (rename, no copy). This bypasses the normal write
/// path and reduces copy time from hours to minutes.
fn copy_block_headers_to_cold_db(
    hot_store: &Store,
    cold_db: &ColdDB,
    home_dir: &Path,
    cold_store_config: &StoreConfig,
) -> anyhow::Result<()> {
    let cold_store_path =
        home_dir.join(cold_store_config.path.as_deref().unwrap_or_else(|| Path::new("cold-data")));
    let sst_dir = cold_store_path.join("migration-sst-tmp");

    tracing::info!(target: "migrations", "copying block headers to cold db via SST ingestion");
    let sst_paths = write_block_headers_to_sst_files(hot_store, &sst_dir)?;

    // move_files=true: SST dir is on the same filesystem as cold store, so
    // ingest renames instead of copying.
    let total_sst = sst_paths.len();
    tracing::info!(target: "migrations", total_sst, "ingesting SST files into cold db, this may take ~10 minutes");
    cold_db.ingest_external_sst_files(DBCol::BlockHeader, &sst_paths, true)?;
    tracing::info!(target: "migrations", "SST ingestion into cold db complete");

    // Files were moved by ingest; clean up the empty directory.
    // Best-effort: don't fail the migration if cleanup fails.
    if let Err(err) = std::fs::remove_dir_all(&sst_dir) {
        tracing::warn!(target: "migrations", ?sst_dir, ?err, "failed to remove temporary SST directory");
    }

    tracing::info!(target: "migrations", "completed copying block headers to cold db");
    Ok(())
}

/// Writes all block headers into SST files using parallel key-range partitions.
///
/// The BlockHeader keys are CryptoHash (32 bytes, uniformly distributed). We
/// partition the key-space into 4 ranges by the first byte, giving each thread
/// its own iterator + SstFileWriter. Each partition produces sorted,
/// non-overlapping SST files named with a partition prefix for global sort order.
fn write_block_headers_to_sst_files(store: &Store, sst_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    std::fs::create_dir_all(sst_dir)?;

    // Estimate total block headers for per-partition progress reporting.
    let genesis_height = get_genesis_height(store).unwrap();
    let head_height = store.chain_store().head().unwrap().height;
    let approx_total = head_height - genesis_height;
    let approx_per_partition = (approx_total / 4).max(1);
    tracing::info!(target: "migrations", ?sst_dir, approx_total, "starting parallel SST file creation for block headers, this may take ~1 hr");

    // 4 partitions by first byte: [..0x40), [0x40..0x80), [0x80..0xC0), [0xC0..).
    let boundaries: [(Option<Vec<u8>>, Option<Vec<u8>>); 4] = [
        (None, Some(vec![0x40])),
        (Some(vec![0x40]), Some(vec![0x80])),
        (Some(vec![0x80]), Some(vec![0xC0])),
        (Some(vec![0xC0]), None),
    ];

    let handles: Vec<_> = boundaries
        .into_iter()
        .enumerate()
        .map(|(partition_id, (lower, upper))| {
            let store = store.clone();
            let sst_dir = sst_dir.to_path_buf();
            thread::Builder::new()
                .name(format!("sst-partition-{}", partition_id))
                .spawn(move || {
                    write_sst_partition(
                        store,
                        sst_dir,
                        partition_id,
                        lower,
                        upper,
                        approx_per_partition,
                    )
                })
                .expect("failed to spawn SST partition thread")
        })
        .collect();

    // Collect results from all partitions.
    let mut sst_paths = Vec::new();
    let mut total_count: u64 = 0;
    for handle in handles {
        let (paths, count) = handle.join().unwrap()?;
        sst_paths.extend(paths);
        total_count += count;
    }

    // Sort by filename — partition prefix ensures correct global key order.
    sst_paths.sort();

    tracing::info!(target: "migrations", total_count, sst_files = sst_paths.len(), "completed parallel SST file creation");

    Ok(sst_paths)
}

/// Writes one partition's block headers into SST files.
fn write_sst_partition(
    store: Store,
    sst_dir: PathBuf,
    partition_id: usize,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    approx_count: u64,
) -> anyhow::Result<(Vec<PathBuf>, u64)> {
    let mut opts = rocksdb::Options::default();
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    let mut writer = rocksdb::SstFileWriter::create(&opts);
    let mut sst_paths = Vec::new();
    let mut file_index: u64 = 0;
    let mut count: u64 = 0;
    let mut file_open = false;

    for (key, value) in store.iter_range(DBCol::BlockHeader, lower.as_deref(), upper.as_deref()) {
        if !file_open {
            let path = sst_dir.join(format!("p{:02}_{:06}.sst", partition_id, file_index));
            writer.open(&path)?;
            sst_paths.push(path);
            file_open = true;
        }

        count += 1;
        writer.put(&*key, &*value)?;

        if writer.file_size() >= MAX_SST_FILE_SIZE {
            writer.finish()?;
            file_open = false;
            file_index += 1;
            let progress = format!("{:.1}", count as f64 / approx_count as f64 * 100.0);
            tracing::info!(target: "migrations", partition_id, file_index, count, progress, "completed SST file");
        }
    }

    if file_open {
        writer.finish()?;
    }

    tracing::info!(target: "migrations", partition_id, count, sst_files = sst_paths.len(), "partition complete");

    Ok((sst_paths, count))
}

/// Test-only entry point exercising the epoch-sync-proof step of the 48->49
/// migration directly.
#[cfg(feature = "test_features")]
pub fn test_only_update_epoch_sync_proof(
    store: Store,
    transaction_validity_period: BlockHeightDelta,
) -> anyhow::Result<()> {
    update_epoch_sync_proof(store, transaction_validity_period)
}

fn update_epoch_sync_proof(
    store: Store,
    transaction_validity_period: BlockHeightDelta,
) -> anyhow::Result<()> {
    let epoch_store = store.epoch_store();

    tracing::info!(target: "migrations", "updating existing epoch sync proof to compressed format");

    // First we move any existing epoch sync proof to the compressed format
    // Note that while accessing the proof, we need to read directly from DBCol::EpochSyncProof
    // as we can't use the epoch_store.get_epoch_sync_proof() method due to
    // ProtocolFeature::ContinuousEpochSync being enabled
    if let Some(proof) = store.get_ser::<EpochSyncProof>(DBCol::EpochSyncProof, &[]) {
        let mut store_update = epoch_store.store_update();
        store_update.set_epoch_sync_proof(&proof);
        store_update.commit();
    }

    tracing::info!(target: "migrations", "generating latest epoch sync proof");
    let chain_store = store.chain_store();
    let final_head = chain_store.final_head()?;
    let genesis_height = chain_store.get_genesis_height();
    let current_epoch_start_height = epoch_store.get_epoch_start(&final_head.epoch_id)?;
    let chain_height_since_genesis = current_epoch_start_height.saturating_sub(genesis_height);
    // Too few epochs after genesis to build a proof; the runtime produces one later.
    if chain_height_since_genesis < transaction_validity_period {
        tracing::info!(
            target: "migrations",
            ?current_epoch_start_height,
            ?genesis_height,
            ?transaction_validity_period,
            "chain is too short to produce epoch sync proof, skipping"
        );
        return Ok(());
    }

    // Anchor at the current epoch (target head-2), like continuous epoch sync does at
    // a boundary; this reads BlockInfo ~3 epochs back. find_target reaches ~4 back,
    // which gc=3 has already collected: the startup crash `epoch block: <hash>`.
    let head = chain_store.head()?;
    let store_update = match epoch_sync::update_epoch_sync_proof(&epoch_store, &head.epoch_id) {
        Ok(store_update) => store_update,
        // Mid-epoch on gc=3, even head-2 reaches below the retained data. Seed the
        // freshest retained epoch (head-1, always final); the runtime advances it later.
        Err(Error::DBNotFoundErr(_)) => {
            let head_block_info = epoch_store.get_block_info(&head.last_block_hash)?;
            let head_epoch_first_header =
                chain_store.get_block_header(head_block_info.epoch_first_block())?;
            let proof = epoch_sync::derive_epoch_sync_proof_from_last_block(
                &epoch_store,
                head_epoch_first_header.prev_hash(),
                true,
            )?;
            let mut store_update = epoch_store.store_update();
            store_update.set_epoch_sync_proof(&proof);
            store_update
        }
        Err(err) => return Err(err.into()),
    };
    store_update.commit();

    Ok(())
}

// function to verify that the block headers that are generated from DBCol::Block are the same
// as the headers that are stored in DBCol::BlockHeader
fn verify_block_headers(store: &Store) -> anyhow::Result<()> {
    let chain_store = store.chain_store();
    let tail_height = chain_store.tail();
    let latest_known_height =
        store.get_ser::<LatestKnown>(DBCol::BlockMisc, LATEST_KNOWN_KEY).unwrap().height;

    tracing::info!(target: "migrations", ?tail_height, ?latest_known_height, "verifying block headers before deletion");

    for height in tail_height..(latest_known_height + 1) {
        for block_hash in chain_store.get_all_header_hashes_by_height(height) {
            let block = match chain_store.get_block(&block_hash) {
                Ok(block) => block,
                // It's possible that some blocks are missing in the DB when we have forks etc.
                Err(Error::DBNotFoundErr(_)) => continue,
                // Any other error should be propagated
                Err(err) => return Err(err.into()),
            };

            let header_from_block = block.header();
            let header_from_store = chain_store.get_block_header(&block_hash)?;
            assert_eq!(header_from_block, header_from_store.as_ref(), "block header mismatch");
        }
    }
    Ok(())
}

fn delete_old_block_headers(store: &Store) -> anyhow::Result<()> {
    tracing::info!(target: "migrations", "deleting all block headers from hot store");

    let mut store_update = store.store_update();
    store_update.delete_all(DBCol::BlockHeader);
    store_update.commit();
    let chain_store = store.chain_store();
    let tail_height = chain_store.tail();
    let latest_known_height =
        store.get_ser::<LatestKnown>(DBCol::BlockMisc, LATEST_KNOWN_KEY).unwrap().height;

    tracing::info!(target: "migrations", ?tail_height, ?latest_known_height, "adding required block headers to hot store");

    let mut store_update = chain_store.store_update();
    for height in tail_height..(latest_known_height + 1) {
        for block_hash in chain_store.get_all_header_hashes_by_height(height) {
            // We've already checked for errors and missing blocks in the verify_block_headers function
            if let Ok(block) = chain_store.get_block(&block_hash) {
                store_update.set_block_header_only(block.header());
            }
        }
        if height % BATCH_SIZE == 0 {
            tracing::info!(target: "migrations", ?height, ?latest_known_height, "committing addition of required block headers to hot store");
            store_update.commit()?;
            store_update = chain_store.store_update();
        }
    }
    store_update.commit()?;
    tracing::info!(target: "migrations", ?latest_known_height, "completed deletion of old block headers from hot store");

    Ok(())
}

#[cfg(test)]
mod migrate_50_to_51_tests {
    use super::{BlockHeaderInnerLiteViewV50, LightClientBlockViewV50, migrate_50_to_51};
    use borsh::BorshDeserialize;
    use near_primitives::hash::CryptoHash;
    use near_primitives::views::LightClientBlockView;
    use near_store::test_utils::create_test_store;
    use near_store::{DBCol, Store};

    fn inner_lite_v50() -> BlockHeaderInnerLiteViewV50 {
        BlockHeaderInnerLiteViewV50 {
            height: 42,
            epoch_id: CryptoHash::hash_bytes(b"epoch"),
            next_epoch_id: CryptoHash::hash_bytes(b"next_epoch"),
            prev_state_root: CryptoHash::hash_bytes(b"state"),
            outcome_root: CryptoHash::hash_bytes(b"outcome"),
            timestamp: 1,
            timestamp_nanosec: 1,
            next_bp_hash: CryptoHash::hash_bytes(b"bp"),
            block_merkle_root: CryptoHash::hash_bytes(b"merkle"),
        }
    }

    fn row_v50(seed: u32) -> Vec<u8> {
        borsh::to_vec(&LightClientBlockViewV50 {
            prev_block_hash: CryptoHash::hash_bytes(b"prev"),
            next_block_inner_hash: CryptoHash::hash_bytes(b"next"),
            inner_lite: inner_lite_v50(),
            // The byte a version-51 reader takes for the trailing Option tag comes
            // from here, so vary it across rows.
            inner_rest_hash: CryptoHash::hash_bytes(&seed.to_le_bytes()),
            next_bps: None,
            approvals_after_next: vec![None, None],
        })
        .unwrap()
    }

    fn key_for(seed: u32) -> CryptoHash {
        CryptoHash::hash_bytes(&seed.to_le_bytes())
    }

    fn write_row(store: &Store, key: &CryptoHash, value: &[u8]) {
        let mut store_update = store.store_update();
        store_update.set(DBCol::EpochLightClientBlocks, key.as_ref(), value);
        store_update.commit();
    }

    fn read_row(store: &Store, key: &CryptoHash) -> LightClientBlockView {
        store.get_ser(DBCol::EpochLightClientBlocks, key.as_ref()).unwrap()
    }

    #[test]
    fn rewrites_rows_written_by_a_released_binary() {
        let store = create_test_store();
        for seed in 0u32..2000 {
            write_row(&store, &key_for(seed), &row_v50(seed));
        }

        // The row a released binary wrote is not readable as the current type. That is
        // the crash the migration removes.
        let stored = store.get(DBCol::EpochLightClientBlocks, key_for(0).as_ref()).unwrap();
        assert!(LightClientBlockView::try_from_slice(&stored).is_err());

        migrate_50_to_51(&store).unwrap();

        for seed in 0u32..2000 {
            let view = read_row(&store, &key_for(seed));
            assert_eq!(view.inner_lite.chunk_execution_root, None);
            assert_eq!(view.inner_lite.height, 42);
            assert_eq!(view.inner_rest_hash, CryptoHash::hash_bytes(&seed.to_le_bytes()));
            assert_eq!(view.approvals_after_next.len(), 2);
        }
    }

    #[test]
    fn leaves_rows_already_in_the_current_layout_alone() {
        let store = create_test_store();
        let key = key_for(0);
        write_row(&store, &key, &row_v50(0));
        migrate_50_to_51(&store).unwrap();
        let migrated = read_row(&store, &key);

        migrate_50_to_51(&store).unwrap();
        assert_eq!(read_row(&store, &key), migrated);
    }

    #[test]
    fn rejects_a_row_in_neither_layout() {
        let store = create_test_store();
        write_row(&store, &key_for(0), b"not a light client block");
        let err = migrate_50_to_51(&store).unwrap_err().to_string();
        assert!(err.contains("reads in neither layout"), "got: {err}");
    }

    #[test]
    fn migrates_an_empty_column() {
        migrate_50_to_51(&create_test_store()).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::Migrator;
    use crate::config::load_test_config;
    use near_chain_configs::Genesis;
    use near_network::tcp;
    use near_store::db::ColdDB;
    use near_store::db::metadata::{DB_VERSION, DbVersion};
    use near_store::{DBCol, Mode, NodeStorage, Store, StoreConfig, StoreMigrator};
    use std::cell::RefCell;
    use strum::IntoEnumIterator;

    /// Delegates to the real [`Migrator`] and records which versions it was asked to
    /// migrate, so a test can assert the migration actually ran rather than that the
    /// database happened to already be current.
    struct RecordingMigrator<'a> {
        inner: Migrator<'a>,
        migrated: RefCell<Vec<DbVersion>>,
    }

    impl<'a> StoreMigrator for RecordingMigrator<'a> {
        fn check_support(&self, version: DbVersion) -> Result<(), &'static str> {
            self.inner.check_support(version)
        }

        fn migrate(
            &self,
            hot_store: &Store,
            cold_db: Option<&ColdDB>,
            version: DbVersion,
            is_snapshot: bool,
        ) -> anyhow::Result<()> {
            self.migrated.borrow_mut().push(version);
            self.inner.migrate(hot_store, cold_db, version, is_snapshot)
        }
    }

    /// A version-49 database has no `DBCol::ChunkProducers` column family. Assert the
    /// full upgrade path for such a database: a read-only open fails on the missing
    /// column family, a read-write open migrates it to 50 and materializes the column,
    /// and a read-only open then succeeds.
    ///
    /// Hot store only. The cold store follows the same version gate but is not covered
    /// here.
    ///
    /// Stable builds only. On nightly, `StoreOpener::ensure_version` overwrites the
    /// freshly migrated version with the 10000 sentinel, and `open_dbs` then reopens
    /// expecting `DB_VERSION`, so the migrating open fails. That applies to every
    /// version bump, not just this one, so there is no nightly upgrade path here to
    /// assert against.
    #[test]
    #[cfg_attr(
        feature = "nightly",
        ignore = "nightly overwrites the migrated version with the 10000 sentinel, so the migrating open fails"
    )]
    fn slow_test_migration_49_to_50_creates_chunk_producers_column() {
        // A fresh database is created at DB_VERSION with every column family present,
        // so build the version-49 shape by stamping the old version and then dropping
        // the column family via a filtered checkpoint.
        let (_hot_dir, hot_opener) = NodeStorage::test_opener();
        let hot_storage = hot_opener.open().unwrap();
        let hot_store = hot_storage.get_hot_store();
        hot_store.set_db_version(49);

        // Build the opener first: its resolved path is where the checkpoint has to land.
        // `DBCol::DbVersion` stays in the keep-list so the checkpoint carries version 49
        // forward.
        let checkpoint_dir = tempfile::tempdir().unwrap();
        let store_config = StoreConfig::test_config();
        let checkpoint_opener =
            NodeStorage::opener(checkpoint_dir.path(), &store_config, None, None);
        let columns_to_keep: Vec<DBCol> =
            DBCol::iter().filter(|&col| col != DBCol::ChunkProducers).collect();
        hot_store
            .database()
            .create_checkpoint(checkpoint_opener.path(), Some(&columns_to_keep))
            .unwrap();
        drop(hot_storage);

        // Read-only cannot create the missing column family. Assert on the message, not
        // just on `is_err`: a wrong checkpoint path would fail with `DbDoesNotExist` and
        // make the rest of this test vacuous.
        let Err(err) = checkpoint_opener.open_in_mode(Mode::ReadOnly) else {
            panic!("read-only open of a v49 database without the column family must fail");
        };
        assert!(
            err.to_string().contains(<&str>::from(DBCol::ChunkProducers)),
            "expected a missing-column-family error, got: {err}"
        );

        let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
        let near_config = load_test_config("test0", tcp::ListenerAddr::reserve_for_test(), genesis);
        let migrator = RecordingMigrator {
            inner: Migrator::new(&near_config, checkpoint_dir.path()),
            migrated: RefCell::new(Vec::new()),
        };
        // `Mode::ReadWriteExisting`, not `Mode::ReadWrite`: the latter can create, so a
        // wrong path would silently open a fresh DB already at DB_VERSION and every
        // assertion below would pass without a migration having run.
        let migrated_opener = NodeStorage::opener(checkpoint_dir.path(), &store_config, None, None)
            .with_migrator(&migrator);
        let migrated_storage = migrated_opener.open_in_mode(Mode::ReadWriteExisting).unwrap();
        let migrated_store = migrated_storage.get_hot_store();
        assert_eq!(migrated_store.get_db_version().unwrap(), DB_VERSION);
        // The checkpoint really was at 49, and every arm from there ran in order.
        assert_eq!(migrator.migrated.borrow().as_slice(), &[49, 50]);
        // The column family now exists and is readable. It is empty until EarlyKickout
        // activates.
        assert_eq!(migrated_store.iter(DBCol::ChunkProducers).count(), 0);
        drop(migrated_storage);

        checkpoint_opener.open_in_mode(Mode::ReadOnly).unwrap();
    }
}
