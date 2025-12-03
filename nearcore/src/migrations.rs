use near_epoch_manager::epoch_sync::{
    derive_epoch_sync_proof_from_last_final_block, find_target_epoch_to_produce_proof_for,
};
use near_primitives::epoch_sync::EpochSyncProof;
use near_primitives::types::BlockHeightDelta;
use near_store::adapter::StoreAdapter;
use near_store::archive::cold_storage::rc_aware_set;
use near_store::db::metadata::{DB_VERSION, DbVersion, MIN_SUPPORTED_DB_VERSION};
use near_store::db::{ColdDB, DBTransaction, Database};
use near_store::{DBCol, Store, get_genesis_height};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::NearConfig;

const CHUNK_SIZE: u64 = 5000;

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
            47 => migrate_47_to_48(
                hot_store,
                cold_db,
                self.config.genesis.config.transaction_validity_period,
            ),
            DB_VERSION.. => unreachable!(),
        }
    }
}

/// This migration does two things:
/// 1. Store block headers from genesis to tail in cold DB
/// 2. Generate and save the compressed epoch sync proof
/// 3. Clear the block headers from genesis to tail in hot_store
fn migrate_47_to_48(
    hot_store: &Store,
    cold_db: Option<&ColdDB>,
    transaction_validity_period: BlockHeightDelta,
) -> anyhow::Result<()> {
    tracing::info!(target: "migrations", "starting migration from DB version 47 to 48");
    if let Some(cold_db) = cold_db {
        store_block_headers_in_cold_db(hot_store, cold_db)?;
    }
    update_epoch_sync_proof(hot_store.clone(), transaction_validity_period)?;
    delete_old_block_headers(hot_store)?;
    Ok(())
}

fn store_block_headers_in_cold_db(hot_store: &Store, cold_db: &ColdDB) -> anyhow::Result<()> {
    let chain_store = hot_store.chain_store();
    let cold_store = cold_db.as_store();

    let genesis_height = get_genesis_height(hot_store)?.expect("genesis height must exist");
    let head_height = chain_store.head().unwrap().height;
    let total_blocks = head_height - genesis_height;
    let num_chunks = (total_blocks + CHUNK_SIZE - 1) / CHUNK_SIZE;

    tracing::info!(target: "migrations", ?genesis_height, ?head_height, ?total_blocks, ?num_chunks, "storing block headers in cold DB");

    (0..num_chunks).into_par_iter().for_each(|chunk_idx| {
        let start = genesis_height + chunk_idx * CHUNK_SIZE;
        let end = (start + CHUNK_SIZE).min(head_height);

        let mut transaction = DBTransaction::new();
        for height in start..end {
            let Ok(block_hash) = chain_store.get_block_hash_by_height(height) else {
                tracing::debug!(target: "migrations", ?height, "block hash not found, skipping deletion of block header");
                continue;
            };
            let block = cold_store.chain_store().get_block(&block_hash).unwrap();
            let header = borsh::to_vec(&block.header()).unwrap();
            rc_aware_set(&mut transaction, DBCol::BlockHeader, block_hash.into(), header);
        }
        cold_db.write(transaction).unwrap();
        tracing::info!(target: "migrations", ?chunk_idx, ?start, ?end, "stored block headers in cold DB chunk");
    });

    Ok(())
}

fn update_epoch_sync_proof(
    store: Store,
    transaction_validity_period: BlockHeightDelta,
) -> anyhow::Result<()> {
    let epoch_store = store.epoch_store();
    let chain_store = store.chain_store();

    tracing::info!(target: "migrations", "updating existing epoch sync proof to compressed format");

    // First we move any existing epoch sync proof to the compressed format
    // Note that while accessing the proof, we need to read directory from DBCol::EpochSyncProof
    // as we can't use the epoch_store.get_epoch_sync_proof() method due to
    // ProtocolFeature::ContinuousEpochSync being enabled
    if let Some(proof) = store.get_ser::<EpochSyncProof>(DBCol::EpochSyncProof, &[]).unwrap() {
        let mut store_update = epoch_store.store_update();
        store_update.set_epoch_sync_proof(&proof);
        store_update.commit()?;
    }

    // Now we generate the epoch sync proof and update it to latest
    tracing::info!(target: "migrations", "generating latest epoch sync proof");
    let last_block_hash =
        find_target_epoch_to_produce_proof_for(&store, transaction_validity_period)?;
    let last_block_header = chain_store.get_block_header(&last_block_hash)?;
    let second_last_block_header = chain_store.get_block_header(last_block_header.prev_hash())?;

    tracing::info!(target: "migrations", ?last_block_hash, "deriving epoch sync proof from last final block");
    let proof = derive_epoch_sync_proof_from_last_final_block(store, &second_last_block_header)?;

    tracing::info!(target: "migrations", "storing latest epoch sync proof");
    let mut store_update = epoch_store.store_update();
    store_update.set_epoch_sync_proof(&proof);
    store_update.commit()?;

    Ok(())
}

fn delete_old_block_headers(store: &Store) -> anyhow::Result<()> {
    let chain_store = store.chain_store();

    let genesis_height = get_genesis_height(store)?.expect("genesis height must exist");
    let tail_height = chain_store.tail().unwrap();
    let total_blocks = tail_height - genesis_height;
    let num_chunks = (total_blocks + CHUNK_SIZE - 1) / CHUNK_SIZE;

    tracing::info!(target: "migrations", ?genesis_height, ?tail_height, ?total_blocks, ?num_chunks, "deleting old block headers from hot store");

    (0..num_chunks).into_par_iter().for_each(|chunk_idx| {
        let start = genesis_height + chunk_idx * CHUNK_SIZE;
        let end = (start + CHUNK_SIZE).min(tail_height);

        let mut store_update = store.store_update();
        for height in start..end {
            let Ok(block_hash) = chain_store.get_block_hash_by_height(height) else {
                tracing::debug!(target: "migrations", ?height, "block hash not found, skipping deletion of block header");
                continue;
            };
            store_update.delete(DBCol::BlockHeader, block_hash.as_bytes());
        }
        store_update.commit().unwrap();
        tracing::info!(target: "migrations", ?chunk_idx, ?start, ?end, "deleted block headers in chunk");
    });

    Ok(())
}
