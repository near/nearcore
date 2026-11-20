use super::AGGREGATOR_KEY;
use near_chain::Error;
use near_chain_configs::GenesisConfig;
use near_epoch_manager::EpochManager;
use near_epoch_manager::aggregated_epoch_stats::AggregatedEpochStats;
#[cfg(feature = "test_features")]
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::hash::CryptoHash;
use near_store::adapter::StoreAdapter;
use near_store::{DBCol, Store};
use std::path::Path;

const ROWS_PER_PROGRESS_LOG: usize = 1000;

/// Replaces the single stored `AggregatedEpochStats` with one row per block in
/// `DBCol::AggregatedEpochStats`.
///
/// Walks the header height index from the final head's epoch first block up to the header
/// head, forks included, and writes a row for every block that starts an epoch or whose
/// parent has a row. After epoch sync, while the final head is below the synced epoch's
/// first block (genesis, or a previous-epoch block after state sync), the walk starts at that
/// first block, whose height comes from the stored epoch sync proof.
///
/// Known gaps, both for blocks missing from the header height index:
/// - `undo-block` drops header-only blocks from the index but keeps their `BlockInfo`. After
///   `undo-block` on DB version 54, those blocks get no row.
/// - Cloud archival readers do not write the index for the blocks they pull, so those blocks
///   get no row.
///
/// Hot store only: the column is not copied to cold storage.
pub(super) fn migrate_54_to_55(
    hot_store: &Store,
    genesis_config: &GenesisConfig,
    home_dir: &Path,
    is_snapshot: bool,
) -> anyhow::Result<()> {
    // State snapshot DBs keep only `BlockMisc` and flat storage columns, so they have no
    // `BlockInfo` to build rows from.
    if is_snapshot {
        tracing::info!(target: "migrations", "state snapshot DB, skipping migration from DB version 54 to 55");
        return Ok(());
    }
    let epoch_manager =
        EpochManager::new_arc_handle(hot_store.clone(), genesis_config, Some(home_dir));
    migrate_54_to_55_with_epoch_manager(hot_store, &epoch_manager.read())
}

/// Test-only entry point running [`migrate_54_to_55`] with the caller's epoch configs, for
/// test chains whose configs `EpochManager::new_arc_handle` cannot rebuild.
#[cfg(feature = "test_features")]
pub fn test_only_migrate_54_to_55(
    hot_store: &Store,
    genesis_config: &GenesisConfig,
    epoch_config_store: EpochConfigStore,
) -> anyhow::Result<()> {
    let epoch_manager = EpochManager::new_arc_handle_from_epoch_config_store(
        hot_store.clone(),
        genesis_config,
        epoch_config_store,
    );
    migrate_54_to_55_with_epoch_manager(hot_store, &epoch_manager.read())
}

fn migrate_54_to_55_with_epoch_manager(
    hot_store: &Store,
    epoch_manager: &EpochManager,
) -> anyhow::Result<()> {
    tracing::info!(target: "migrations", "starting migration from DB version 54 to 55");

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

    // Epoch sync stores no `BlockInfo` behind the synced epoch's first block, and the final head
    // stays below that block until block sync reaches it. Rows then start at that block, whose
    // height only the stored epoch sync proof records.
    let synced_epoch_start = hot_store
        .epoch_store()
        .get_epoch_sync_proof()?
        .map(|proof| proof.current_epoch.first_block_header_in_epoch.height());
    let start_height = match synced_epoch_start {
        Some(height) if final_head.height < height => height,
        _ => {
            let final_head_info = epoch_manager.get_block_info(&final_head.last_block_hash)?;
            if final_head_info.is_genesis() {
                final_head_info.height()
            } else {
                epoch_manager.get_block_info(final_head_info.epoch_first_block())?.height()
            }
        }
    };
    // By height, so that every parent comes first. A block gets a row if it starts an epoch or
    // its parent has one.
    let header_head = chain_store.header_head()?;
    let mut written = 0;
    for height in start_height..=header_head.height {
        for hash in chain_store.get_all_header_hashes_by_height(height) {
            let block_info = epoch_manager.get_block_info(&hash)?;
            let aggregator = if block_info.is_genesis() {
                AggregatedEpochStats::new(*block_info.epoch_id(), hash)
            } else if block_info.epoch_first_block() == &hash
                || hot_store.exists(DBCol::AggregatedEpochStats, block_info.prev_hash().as_ref())
            {
                let prev_block_info = epoch_manager.get_block_info(block_info.prev_hash())?;
                epoch_manager.compute_aggregated_epoch_stats(&block_info, &prev_block_info)?
            } else {
                continue;
            };
            set_aggregator_row(hot_store, &hash, &aggregator, &mut written);
        }
    }

    tracing::info!(target: "migrations", written, "completed migration from DB version 54 to 55");
    Ok(())
}

/// Commits the aggregator row of `block_hash` at once: `compute_aggregated_epoch_stats` reads
/// the parent's row from the store.
fn set_aggregator_row(
    store: &Store,
    block_hash: &CryptoHash,
    aggregator: &AggregatedEpochStats,
    written: &mut usize,
) {
    let mut store_update = store.store_update();
    store_update.set_ser(DBCol::AggregatedEpochStats, block_hash.as_ref(), aggregator);
    store_update.commit();
    *written += 1;
    if written.is_multiple_of(ROWS_PER_PROGRESS_LOG) {
        tracing::info!(target: "migrations", written = *written, "wrote epoch info aggregator rows");
    }
}

#[cfg(test)]
mod tests {
    use super::migrate_54_to_55;
    use near_chain_configs::GenesisConfig;
    use near_primitives::block::Tip;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::EpochId;
    use near_store::adapter::StoreAdapter;
    use near_store::test_utils::create_test_store;
    use std::path::Path;

    #[test]
    fn skips_state_snapshot_with_final_head_and_no_block_info() {
        let store = create_test_store();
        let final_head = Tip {
            height: 10,
            last_block_hash: CryptoHash::hash_bytes(b"final head"),
            prev_block_hash: CryptoHash::hash_bytes(b"final head parent"),
            epoch_id: EpochId::default(),
            next_epoch_id: EpochId::default(),
        };
        let mut chain_store_update = store.chain_store().store_update();
        chain_store_update.set_final_head(&final_head);
        chain_store_update.commit().unwrap();

        let is_snapshot = true;
        migrate_54_to_55(&store, &GenesisConfig::default(), Path::new("unused"), is_snapshot)
            .unwrap();
    }
}
