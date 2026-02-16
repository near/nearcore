use std::str::FromStr;

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
    TrieChanges, TrieConfig, TrieUpdate, get_genesis_height, set,
};

use crate::NearConfig;

const BATCH_SIZE: u64 = 100_000;

pub(super) struct Migrator<'a> {
    config: &'a NearConfig,
}

impl<'a> Migrator<'a> {
    pub fn new(config: &'a NearConfig) -> Self {
        Self { config }
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
#[allow(dead_code)]
fn migrate_48_to_49(
    hot_store: &Store,
    cold_db: Option<&ColdDB>,
    transaction_validity_period: BlockHeightDelta,
) -> anyhow::Result<()> {
    tracing::info!(target: "migrations", "starting migration from DB version 48 to 49");

    if let Some(cold_db) = cold_db {
        copy_block_headers_to_cold_db(hot_store, cold_db)?;
    }

    update_epoch_sync_proof(hot_store.clone(), transaction_validity_period)?;
    verify_block_headers(hot_store)?;
    delete_old_block_headers(hot_store)?;
    Ok(())
}

// Copy block headers from hot_store to cold_db in batches
// Note that we are using raw DBTransaction iteration to avoid deserializing and re-serializing the block headers
// Typically this is NOT recommended as ColdDB has specific ways for storing data, example RC columns.
// But in our case this is fine as the block headers are stored as-is in both hot and cold DBs.
fn copy_block_headers_to_cold_db(hot_store: &Store, cold_db: &ColdDB) -> anyhow::Result<()> {
    let genesis_height = get_genesis_height(hot_store)?.unwrap();
    let head_height = hot_store.chain_store().head().unwrap().height;
    let approx_num_blocks = head_height - genesis_height;

    tracing::info!(target: "migrations", ?approx_num_blocks, "copying block headers to cold db");

    let mut count = 0;
    let mut transaction = DBTransaction::new();
    for (key, value) in hot_store.iter_raw_bytes(DBCol::BlockHeader) {
        transaction.set(DBCol::BlockHeader, key.into_vec(), value.into_vec());
        count += 1;
        if count % BATCH_SIZE == 0 {
            cold_db.write(transaction);
            transaction = DBTransaction::new();
            let percent_complete = (count as f64 / approx_num_blocks as f64) * 100.0;
            tracing::info!(target: "migrations", ?count, ?approx_num_blocks, ?percent_complete, "copied block headers to cold db");
        }
    }
    cold_db.write(transaction);
    tracing::info!(target: "migrations", ?count, ?approx_num_blocks, "completed copying block headers to cold db");

    Ok(())
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
    if let Some(proof) = store.get_ser::<EpochSyncProof>(DBCol::EpochSyncProof, &[]).unwrap() {
        let mut store_update = epoch_store.store_update();
        store_update.set_epoch_sync_proof(&proof);
        store_update.commit();
    }

    // Now we generate the epoch sync proof and update it to latest
    tracing::info!(target: "migrations", "generating latest epoch sync proof");
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
    let tail_height = chain_store.tail().unwrap();
    let latest_known_height =
        store.get_ser::<LatestKnown>(DBCol::BlockMisc, LATEST_KNOWN_KEY)?.unwrap().height;

    tracing::info!(target: "migrations", ?tail_height, ?latest_known_height, "verifying block headers before deletion");

    for height in tail_height..(latest_known_height + 1) {
        for block_hash in chain_store.get_all_header_hashes_by_height(height)? {
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
    let tail_height = chain_store.tail().unwrap();
    let latest_known_height =
        store.get_ser::<LatestKnown>(DBCol::BlockMisc, LATEST_KNOWN_KEY)?.unwrap().height;

    tracing::info!(target: "migrations", ?tail_height, ?latest_known_height, "adding required block headers to hot store");

    let mut store_update = chain_store.store_update();
    for height in tail_height..(latest_known_height + 1) {
        for block_hash in chain_store.get_all_header_hashes_by_height(height)? {
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
