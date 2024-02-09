use std::collections::hash_map::Entry;
use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_primitives::error::Error;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::utils::{get_block_shard_id, get_block_shard_id_rev};
use near_store::db::STATE_TRANSITIONS_METADATA_KEY;
use near_store::{DBCol, StorageError};
use once_cell::unsync::OnceCell;

use crate::{ChainStore, ChainStoreAccess};

#[derive(BorshSerialize, BorshDeserialize, Default, Debug)]
struct StateTransitionsMetadata {
    /// Represents max not-yet-GCed height for each shard
    start_heights: HashMap<ShardId, BlockHeight>,
}

/// Removes StateTransitionData entries from the db based on `block`.
/// It is safe to delete all StateTransitionData before the last
/// final block for shards with present chunks in that block.
pub(crate) fn garbage_collect_state_transition_data(
    chain_store: &ChainStore,
    block: &Block,
) -> Result<(), Error> {
    let final_block_hash = *block.header().last_final_block();
    if final_block_hash == CryptoHash::default() {
        return Ok(());
    }
    let final_block = chain_store.get_block(&final_block_hash)?;
    let final_block_height = final_block.header().height();
    let mut metadata = chain_store
        .store()
        .get_ser::<StateTransitionsMetadata>(DBCol::Misc, STATE_TRANSITIONS_METADATA_KEY)?
        .unwrap_or_default();
    tracing::debug!(
        target: "state_transition_data",
        final_block_height,
        ?metadata,
        "garbage collecting state transition data"
    );
    // Only calculated if metadata doesn't exist or if it doesn't contain start_heights entry for the shard
    let computed_start_heights = OnceCell::<HashMap<ShardId, BlockHeight>>::new();
    let mut store_update = chain_store.store().store_update();
    for chunk in final_block.chunks().iter().filter(|chunk| chunk.is_new_chunk(final_block_height))
    {
        let shard_id = chunk.shard_id();
        let start_height = if let Some(&start_height) = metadata.start_heights.get(&shard_id) {
            start_height
        } else {
            *computed_start_heights
                .get_or_try_init(|| compute_start_heights(chain_store))?
                .get(&shard_id)
                .unwrap_or(&final_block_height)
        };
        let mut deleted_count = 0;
        for height in start_height..final_block_height {
            for block_hash in chain_store.get_all_block_hashes_by_height(height)?.values().flatten()
            {
                store_update
                    .delete(DBCol::StateTransitionData, &get_block_shard_id(block_hash, shard_id));
                deleted_count += 1;
            }
        }
        tracing::debug!(
            target: "state_transition_data",
            shard_id,
            start_height,
            deleted_count,
            "garbage collected state transition data for shard"
        );
        metadata.start_heights.insert(shard_id, final_block_height);
    }
    store_update.set_ser(DBCol::Misc, STATE_TRANSITIONS_METADATA_KEY, &metadata)?;
    store_update.commit()?;

    Ok(())
}

/// Calculates min height across all existing StateTransitionData entries for each shard
fn compute_start_heights(chain_store: &ChainStore) -> Result<HashMap<ShardId, BlockHeight>, Error> {
    let mut start_heights = HashMap::new();
    for res in chain_store.store().iter(DBCol::StateTransitionData) {
        let (block_hash, shard_id) = get_block_shard_id_rev(&res?.0).map_err(|err| {
            Error::StorageError(StorageError::StorageInconsistentState(format!(
                "Invalid StateTransitionData key: {err:?}"
            )))
        })?;
        let block_height = chain_store.get_block_height(&block_hash)?;
        match start_heights.entry(shard_id) {
            Entry::Occupied(mut entry) => {
                if block_height < *entry.get() {
                    entry.insert(block_height);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(block_height);
            }
        };
    }
    tracing::debug!(
        target: "state_transition_data",
        ?start_heights,
        "computed state transition data start heights"
    );
    Ok(start_heights)
}
