use crate::NearConfig;
use near_chain::{Error, LatestKnown};
use near_chain_configs::GenesisConfig;
use near_epoch_manager::epoch_sync::{
    derive_epoch_sync_proof_from_last_block, find_target_epoch_to_produce_proof_for,
};
use near_primitives::chains::MAINNET;
use near_primitives::epoch_sync::EpochSyncProof;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::DelayedReceiptIndices;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{BlockHeightDelta, ShardId, StateChangeCause};
use near_store::adapter::StoreAdapter;
use near_store::adapter::trie_store::TrieStoreUpdateAdapter;
use near_store::archive::cold_storage::{join_two_keys, rc_aware_set};
use near_store::db::metadata::{DB_VERSION, DbVersion, MIN_SUPPORTED_DB_VERSION};
use near_store::db::{ColdDB, DBTransaction, Database};
use near_store::flat::FlatStorageManager;
use near_store::{
    DBCol, LATEST_KNOWN_KEY, ShardTries, ShardUId, StateSnapshotConfig, Store, StoreConfig,
    TrieChanges, TrieConfig, TrieUpdate, set,
};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::mpsc;
use std::thread;

const BATCH_SIZE: u64 = 100_000;
const MAX_SST_FILE_SIZE: u64 = 256 * 1024 * 1024; // 256 MB
/// Number of parallel SST writer threads.
const NUM_SST_WRITERS: usize = 4;

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
                &self.config.config.store,
            ),
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

/// This migration does three things:
/// 1. Copy block headers from hot_store to cold_db (if cold_db is present)
/// 2. Generate and save the compressed epoch sync proof
/// 3. Clear the block headers from genesis to tail in hot_store
fn migrate_48_to_49(
    hot_store: &Store,
    cold_db: Option<&ColdDB>,
    transaction_validity_period: BlockHeightDelta,
    home_dir: &Path,
    cold_store_config: &StoreConfig,
) -> anyhow::Result<()> {
    tracing::info!(target: "migrations", "starting migration from DB version 48 to 49");

    if let Some(cold_db) = cold_db {
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
    tracing::info!(target: "migrations", sst_files = sst_paths.len(), "ingesting SST files into cold db");
    cold_db.ingest_external_sst_files(DBCol::BlockHeader, &sst_paths, true)?;
    tracing::info!(target: "migrations", "SST ingestion into cold db complete");

    // Files were moved by ingest; clean up the empty directory.
    std::fs::remove_dir_all(&sst_dir)?;

    tracing::info!(target: "migrations", "completed copying block headers to cold db");
    Ok(())
}

/// A batch of key-value pairs to be written into a single SST file.
struct SstBatch {
    file_index: u64,
    entries: Vec<(Box<[u8]>, Box<[u8]>)>,
}

/// Writes all block headers into SST files using parallel writer threads.
///
/// A reader thread iterates block headers from the hot store and groups them
/// into batches (one per SST file). Batches are sent to a pool of writer
/// threads via a bounded channel, so multiple SST files are written
/// concurrently. This increases I/O queue depth on the cold disk, utilizing
/// more of the available bandwidth.
///
/// The RocksDB iterator yields sorted keys. Each batch contains a contiguous
/// sorted range, so the resulting SST files have non-overlapping key ranges
/// as required by ingest.
fn write_block_headers_to_sst_files(store: &Store, sst_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    std::fs::create_dir_all(sst_dir)?;

    tracing::info!(
        target: "migrations",
        ?sst_dir,
        num_writers = NUM_SST_WRITERS,
        "starting parallel SST file creation for block headers"
    );

    // Bounded channel: capacity = NUM_SST_WRITERS so the reader blocks when all
    // writers are busy, providing natural backpressure.
    let (tx, rx) = mpsc::sync_channel::<SstBatch>(NUM_SST_WRITERS);

    let sst_dir_owned = sst_dir.to_path_buf();

    // Spawn writer threads that share the receiver via a mutex.
    let rx = std::sync::Arc::new(std::sync::Mutex::new(rx));
    let mut writer_handles = Vec::with_capacity(NUM_SST_WRITERS);
    for worker_id in 0..NUM_SST_WRITERS {
        let rx = rx.clone();
        let sst_dir = sst_dir_owned.clone();
        let handle = thread::Builder::new().name(format!("sst-writer-{}", worker_id)).spawn(
            move || -> anyhow::Result<Vec<PathBuf>> {
                let mut opts = rocksdb::Options::default();
                opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
                let mut writer = rocksdb::SstFileWriter::create(&opts);
                let mut paths = Vec::new();

                loop {
                    let batch = {
                        let lock = rx.lock().unwrap();
                        lock.recv()
                    };
                    let batch = match batch {
                        Ok(batch) => batch,
                        Err(_) => break, // channel closed, no more batches
                    };

                    let path = sst_dir.join(format!("{:06}.sst", batch.file_index));
                    writer.open(&path)?;
                    for (key, value) in &batch.entries {
                        writer.put(key, value)?;
                    }
                    writer.finish()?;
                    paths.push(path);
                }

                Ok(paths)
            },
        )?;
        writer_handles.push(handle);
    }

    // Reader: iterate hot store and send batches to writers.
    let mut file_index: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut total_count: u64 = 0;
    let mut batch_bytes: u64 = 0;
    let mut entries: Vec<(Box<[u8]>, Box<[u8]>)> = Vec::new();

    for (key, value) in store.iter_raw_bytes(DBCol::BlockHeader) {
        total_bytes += (key.len() + value.len()) as u64;
        batch_bytes += (key.len() + value.len()) as u64;
        total_count += 1;
        entries.push((key, value));

        if batch_bytes >= MAX_SST_FILE_SIZE {
            tx.send(SstBatch { file_index, entries })?;
            file_index += 1;
            let total_gb = format!("{:.2}", total_bytes as f64 / (1024.0 * 1024.0 * 1024.0));
            tracing::info!(
                target: "migrations",
                file_index,
                count = total_count,
                total_gb,
                "sent SST batch to writer"
            );
            entries = Vec::new();
            batch_bytes = 0;
        }
    }

    // Send the last partial batch.
    if !entries.is_empty() {
        tx.send(SstBatch { file_index, entries })?;
    }

    // Drop sender to signal writers to finish.
    drop(tx);

    // Collect results from all writers.
    let mut sst_paths: Vec<PathBuf> = Vec::new();
    for handle in writer_handles {
        let paths =
            handle.join().map_err(|e| anyhow::anyhow!("SST writer thread panicked: {:?}", e))??;
        sst_paths.extend(paths);
    }

    // Sort by filename to maintain ingestion order.
    sst_paths.sort();

    let total_gb = format!("{:.2}", total_bytes as f64 / (1024.0 * 1024.0 * 1024.0));
    tracing::info!(
        target: "migrations",
        total_gb,
        total_count,
        sst_files = sst_paths.len(),
        num_writers = NUM_SST_WRITERS,
        "completed parallel SST file creation for block headers",
    );

    Ok(sst_paths)
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

    // Generate the epoch sync proof. On short chains (e.g. tests),
    // find_target_epoch_to_produce_proof_for would walk past genesis — skip and let
    // the runtime produce it later via extend_epoch_sync_proof.
    tracing::info!(target: "migrations", "generating latest epoch sync proof");
    let chain_store = store.chain_store();
    let final_head = chain_store.final_head()?;
    let current_epoch_start_height = epoch_store.get_epoch_start(&final_head.epoch_id)?;
    if current_epoch_start_height < transaction_validity_period {
        tracing::info!(
            target: "migrations",
            ?current_epoch_start_height,
            ?transaction_validity_period,
            "chain is too short to produce epoch sync proof, skipping"
        );
        return Ok(());
    }

    let last_block_hash =
        find_target_epoch_to_produce_proof_for(&store, transaction_validity_period)?;

    tracing::info!(target: "migrations", ?last_block_hash, "deriving epoch sync proof from last final block");
    let proof = derive_epoch_sync_proof_from_last_block(&epoch_store, &last_block_hash, true)?;

    tracing::info!(target: "migrations", "storing latest epoch sync proof");
    let mut store_update = epoch_store.store_update();
    store_update.set_epoch_sync_proof(&proof);
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
