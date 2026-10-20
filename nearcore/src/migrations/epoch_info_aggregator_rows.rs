use super::AGGREGATOR_KEY;
use near_chain::Error;
use near_chain_configs::GenesisConfig;
use near_epoch_manager::EpochManager;
use near_epoch_manager::epoch_info_aggregator::EpochInfoAggregator;
#[cfg(feature = "test_features")]
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_store::adapter::StoreAdapter;
use near_store::{DBCol, Store};
use std::path::Path;

/// [`migrate_53_to_54`] logs its progress every this many rows.
const ROWS_PER_PROGRESS_LOG: usize = 1000;

/// Replaces the single stored `EpochInfoAggregator` with one row per block in
/// `DBCol::EpochInfoAggregators`.
///
/// Writes rows for the final head's chain from its epoch start and for every recorded
/// descendant of the final head, forks included. On a node in header sync after epoch sync,
/// the final head is genesis, so the migration starts at the synced epoch's first block,
/// taken from the stored epoch sync proof. Blocks recorded later get their rows from
/// `EpochManager::record_block_info`. Older blocks need no rows: nothing near the head reads
/// them, and a finished epoch's summary is in `DBCol::EpochValidatorInfo`.
///
/// Hot store only: the column is not copied to cold storage.
pub(super) fn migrate_53_to_54(
    hot_store: &Store,
    genesis_config: &GenesisConfig,
    home_dir: &Path,
) -> anyhow::Result<()> {
    let epoch_manager =
        EpochManager::new_arc_handle(hot_store.clone(), genesis_config, Some(home_dir));
    migrate_53_to_54_with_epoch_manager(hot_store, &epoch_manager.read())
}

/// Test-only entry point running [`migrate_53_to_54`] with the caller's epoch configs, for
/// test chains whose configs `EpochManager::new_arc_handle` cannot rebuild.
#[cfg(feature = "test_features")]
pub fn test_only_migrate_53_to_54(
    hot_store: &Store,
    genesis_config: &GenesisConfig,
    epoch_config_store: EpochConfigStore,
) -> anyhow::Result<()> {
    let epoch_manager = EpochManager::new_arc_handle_from_epoch_config_store(
        hot_store.clone(),
        genesis_config,
        epoch_config_store,
    );
    migrate_53_to_54_with_epoch_manager(hot_store, &epoch_manager.read())
}

fn migrate_53_to_54_with_epoch_manager(
    hot_store: &Store,
    epoch_manager: &EpochManager,
) -> anyhow::Result<()> {
    tracing::info!(target: "migrations", "starting migration from DB version 53 to 54");

    let mut store_update = hot_store.store_update();
    store_update.delete(DBCol::EpochInfo, AGGREGATOR_KEY);
    store_update.commit();

    let chain_store = hot_store.chain_store();
    let final_head = match chain_store.final_head() {
        Ok(final_head) => final_head,
        Err(Error::DBNotFoundErr(_)) => {
            tracing::info!(target: "migrations", "no final head, no epoch info aggregator rows to write");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    let header_head = chain_store.header_head()?;

    // The final head's chain back to its epoch start, newest first.
    let mut chain = vec![epoch_manager.get_block_info(&final_head.last_block_hash)?];
    loop {
        let block_info = chain.last().unwrap();
        if block_info.is_genesis() || block_info.epoch_first_block() == block_info.hash() {
            break;
        }
        chain.push(epoch_manager.get_block_info(block_info.prev_hash())?);
    }

    // Each row is committed before the next one, because `aggregator_after` reads the
    // parent's row from the store.
    let mut written = 0;
    for block_info in chain.iter().rev() {
        let aggregator = if block_info.is_genesis() {
            EpochInfoAggregator::new(*block_info.epoch_id(), *block_info.hash())
        } else {
            let prev_block_info = epoch_manager.get_block_info(block_info.prev_hash())?;
            epoch_manager.aggregator_after(block_info, &prev_block_info)?
        };
        set_aggregator_row(hot_store, block_info.hash(), &aggregator, &mut written);
    }

    // After epoch sync the final head is genesis until block sync starts, and the store has
    // no headers between genesis and the previous epoch's last blocks. Start at the first
    // block of the synced epoch.
    let first_height = match hot_store.epoch_store().get_epoch_sync_proof()? {
        Some(proof) if final_head.height == chain_store.get_genesis_height() => {
            proof.current_epoch.first_block_header_in_epoch.height()
        }
        _ => final_head.height + 1,
    };
    // Descendants of the final head, by height so that every parent comes first. A block
    // whose parent has no row is on a fork below the final head and can never be extended.
    // A block that starts an epoch does not read its parent's row.
    for height in first_height..=header_head.height {
        for hash in chain_store.get_all_header_hashes_by_height(height) {
            let block_info = match epoch_manager.get_block_info(&hash) {
                Ok(block_info) => block_info,
                Err(EpochError::MissingBlock(_)) => continue,
                Err(err) => return Err(err.into()),
            };
            if block_info.epoch_first_block() != &hash
                && !hot_store.exists(DBCol::EpochInfoAggregators, block_info.prev_hash().as_ref())
            {
                continue;
            }
            let prev_block_info = epoch_manager.get_block_info(block_info.prev_hash())?;
            let aggregator = epoch_manager.aggregator_after(&block_info, &prev_block_info)?;
            set_aggregator_row(hot_store, &hash, &aggregator, &mut written);
        }
    }

    tracing::info!(target: "migrations", written, "completed migration from DB version 53 to 54");
    Ok(())
}

/// Commits the aggregator row of `block_hash` in its own store update.
fn set_aggregator_row(
    store: &Store,
    block_hash: &CryptoHash,
    aggregator: &EpochInfoAggregator,
    written: &mut usize,
) {
    let mut store_update = store.store_update();
    store_update.set_ser(DBCol::EpochInfoAggregators, block_hash.as_ref(), aggregator);
    store_update.commit();
    *written += 1;
    if written.is_multiple_of(ROWS_PER_PROGRESS_LOG) {
        tracing::info!(target: "migrations", written = *written, "wrote epoch info aggregator rows");
    }
}
