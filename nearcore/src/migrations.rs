use near_chain::{Error, LatestKnown};
use near_epoch_manager::epoch_sync::{
    derive_epoch_sync_proof_from_last_final_block, find_target_epoch_to_produce_proof_for,
};
use near_primitives::epoch_sync::EpochSyncProof;
use near_primitives::types::BlockHeightDelta;
use near_store::adapter::StoreAdapter;
use near_store::db::ColdDB;
use near_store::db::metadata::{DB_VERSION, DbVersion, MIN_SUPPORTED_DB_VERSION};
use near_store::{DBCol, LATEST_KNOWN_KEY, Store};

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
            47 => Ok(()), // TODO(continuous_epoch_sync): Implement the migration
            DB_VERSION.. => unreachable!(),
        }
    }
}

/// This migration does two things:
/// 1. Generate and save the compressed epoch sync proof
/// 2. Clear the block headers from genesis to tail in hot_store
#[allow(dead_code)]
fn migrate_47_to_48(
    hot_store: &Store,
    transaction_validity_period: BlockHeightDelta,
) -> anyhow::Result<()> {
    tracing::info!(target: "migrations", "starting migration from DB version 47 to 48");
    // TODO(continuous-epoch-sync): Add migration for cold storage where we copy the headers from hot store to cold store
    update_epoch_sync_proof(hot_store.clone(), transaction_validity_period)?;
    verify_block_headers(hot_store)?;
    delete_old_block_headers(hot_store)?;
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
    // Note that while accessing the proof, we need to read directly from DBCol::EpochSyncProof
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
                Err(Error::DBNotFoundErr(_)) => {
                    // It's possible that some blocks are missing in the DB when we have forks etc.
                    tracing::debug!(target: "migrations", ?height, ?block_hash, "skipping block not found");
                    continue;
                }
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
    store_update.commit()?;

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
        if height % CHUNK_SIZE == 0 {
            tracing::info!(target: "migrations", ?height, ?latest_known_height, "committing addition of required block headers to hot store");
            store_update.commit()?;
            store_update = chain_store.store_update();
        }
    }
    store_update.commit()?;
    tracing::info!(target: "migrations", ?latest_known_height, "completed deletion of old block headers from hot store");

    Ok(())
}
