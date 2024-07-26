use std::cmp::min;

use std::collections::HashMap;

use near_chain_primitives::error::Error;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::utils::{get_block_shard_id, get_block_shard_id_rev};
use near_store::db::STATE_TRANSITION_START_HEIGHTS;
use near_store::{DBCol, StorageError};

use crate::{Chain, ChainStore, ChainStoreAccess};

/// Represents max not-yet-GCed height for each shard
type StateTransitionStartHeights = HashMap<ShardId, BlockHeight>;

impl Chain {
    pub(crate) fn garbage_collect_state_transition_data(&self, block: &Block) -> Result<(), Error> {
        let chain_store = self.chain_store();
        let final_block_hash = *block.header().last_final_block();
        if final_block_hash == CryptoHash::default() {
            return Ok(());
        }
        let final_block = chain_store.get_block(&final_block_hash)?;
        let final_block_chunk_created_heights =
            final_block.chunks().iter().map(|chunk| chunk.height_created()).collect::<Vec<_>>();
        clear_before_last_final_block(chain_store, &final_block_chunk_created_heights)?;
        Ok(())
    }
}

/// Removes StateTransitionData entries from the db based on last final block state.
/// It is safe to delete all StateTransitionData before the last final block
/// for the shards with present chunks in that block. We use chunk's height created
/// here in order to not prematurely remove state transitions for shards with
/// missing chunks in the final block.
/// TODO(resharding): this doesn't work after shard layout change
fn clear_before_last_final_block(
    chain_store: &ChainStore,
    last_final_block_chunk_created_heights: &[BlockHeight],
) -> Result<(), Error> {
    let mut start_heights = if let Some(start_heights) =
        chain_store
            .store()
            .get_ser::<StateTransitionStartHeights>(DBCol::Misc, STATE_TRANSITION_START_HEIGHTS)?
    {
        start_heights
    } else {
        compute_start_heights(chain_store)?
    };
    tracing::debug!(
        target: "state_transition_data",
        ?last_final_block_chunk_created_heights,
        ?start_heights,
        "garbage collecting state transition data"
    );
    let mut store_update = chain_store.store().store_update();
    for (shard_index, &last_final_block_height) in
        last_final_block_chunk_created_heights.iter().enumerate()
    {
        let shard_id = shard_index as ShardId;
        let start_height = *start_heights.get(&shard_id).unwrap_or(&last_final_block_height);
        let mut potentially_deleted_count = 0;
        for height in start_height..last_final_block_height {
            for block_hash in chain_store.get_all_block_hashes_by_height(height)?.values().flatten()
            {
                store_update
                    .delete(DBCol::StateTransitionData, &get_block_shard_id(block_hash, shard_id));
                potentially_deleted_count += 1;
            }
        }
        tracing::debug!(
            target: "state_transition_data",
            shard_id,
            start_height,
            potentially_deleted_count,
            "garbage collected state transition data for shard"
        );
        start_heights.insert(shard_id, last_final_block_height);
    }
    store_update.set_ser(DBCol::Misc, STATE_TRANSITION_START_HEIGHTS, &start_heights)?;
    store_update.commit()?;

    Ok(())
}

/// Calculates min height across all existing StateTransitionData entries for each shard
fn compute_start_heights(chain_store: &ChainStore) -> Result<StateTransitionStartHeights, Error> {
    let mut start_heights = HashMap::new();
    for res in chain_store.store().iter(DBCol::StateTransitionData) {
        let (block_hash, shard_id) = get_block_shard_id_rev(&res?.0).map_err(|err| {
            Error::StorageError(StorageError::StorageInconsistentState(format!(
                "Invalid StateTransitionData key: {err:?}"
            )))
        })?;
        let block_height = chain_store.get_block_height(&block_hash)?;
        start_heights
            .entry(shard_id)
            .and_modify(|height| *height = min(block_height, *height))
            .or_insert(block_height);
    }
    tracing::debug!(
        target: "state_transition_data",
        ?start_heights,
        "computed state transition data start heights"
    );
    Ok(start_heights)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use near_primitives::block_header::{BlockHeader, BlockHeaderInnerLite, BlockHeaderV4};
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::stateless_validation::StoredChunkStateTransitionData;
    use near_primitives::types::{BlockHeight, EpochId, ShardId};
    use near_primitives::utils::{get_block_shard_id, get_block_shard_id_rev, index_to_bytes};
    use near_store::db::STATE_TRANSITION_START_HEIGHTS;
    use near_store::test_utils::create_test_store;
    use near_store::{DBCol, Store};

    use super::{clear_before_last_final_block, StateTransitionStartHeights};
    use crate::ChainStore;

    #[test]
    fn initial_state_transition_data_gc() {
        let shard_id = 0;
        let block_at_1 = hash(&[1]);
        let block_at_2 = hash(&[2]);
        let block_at_3 = hash(&[3]);
        let final_height = 2;
        let store = create_test_store();
        for (hash, height) in [(block_at_1, 1), (block_at_2, 2), (block_at_3, 3)] {
            save_state_transition_data(&store, hash, height, shard_id);
        }
        clear_before_last_final_block(&create_chain_store(&store), &[final_height]).unwrap();
        check_start_heights(&store, vec![final_height]);
        check_existing_state_transition_data(
            &store,
            vec![(block_at_2, shard_id), (block_at_3, shard_id)],
        );
    }
    #[test]
    fn multiple_state_transition_data_gc() {
        let shard_id = 0;
        let store = create_test_store();
        let chain_store = create_chain_store(&store);
        save_state_transition_data(&store, hash(&[1]), 1, shard_id);
        save_state_transition_data(&store, hash(&[2]), 2, shard_id);
        clear_before_last_final_block(&chain_store, &[2]).unwrap();
        let block_at_3 = hash(&[3]);
        let final_height = 3;
        save_state_transition_data(&store, block_at_3, final_height, shard_id);
        clear_before_last_final_block(&chain_store, &[3]).unwrap();
        check_start_heights(&store, vec![final_height]);
        check_existing_state_transition_data(&store, vec![(block_at_3, shard_id)]);
    }

    #[track_caller]
    fn check_start_heights(store: &Store, expected: Vec<BlockHeight>) {
        let start_heights = store
            .get_ser::<StateTransitionStartHeights>(DBCol::Misc, STATE_TRANSITION_START_HEIGHTS)
            .unwrap()
            .unwrap();
        assert_eq!(
            start_heights,
            expected
                .into_iter()
                .enumerate()
                .map(|(i, h)| (i as ShardId, h))
                .collect::<HashMap<_, _>>()
        );
    }

    #[track_caller]
    fn check_existing_state_transition_data(store: &Store, expected: Vec<(CryptoHash, ShardId)>) {
        let mut remaining = expected.into_iter().collect::<HashSet<_>>();
        for entry in store.iter(DBCol::StateTransitionData) {
            let key = get_block_shard_id_rev(&entry.unwrap().0).unwrap();
            assert!(remaining.remove(&key), "unexpected StateTransitionData entry at {key:?}");
        }
        assert!(remaining.is_empty(), "missing StateTransitionData entries: {remaining:?}");
    }

    fn create_chain_store(store: &Store) -> ChainStore {
        ChainStore::new(store.clone(), 0, true)
    }

    fn save_state_transition_data(
        store: &Store,
        block_hash: CryptoHash,
        block_height: BlockHeight,
        shard_id: ShardId,
    ) {
        let epoch_id = EpochId::default();
        let blocks_per_height_key = index_to_bytes(block_height);
        let mut blocks_per_height: HashMap<EpochId, HashSet<CryptoHash>> = store
            .get_ser(DBCol::BlockPerHeight, blocks_per_height_key.as_ref())
            .unwrap()
            .unwrap_or_else(|| HashMap::default());
        blocks_per_height.entry(epoch_id).or_default().insert(block_hash);

        let mut store_update = store.store_update();
        store_update
            .set_ser(
                DBCol::StateTransitionData,
                &get_block_shard_id(&block_hash, shard_id),
                &StoredChunkStateTransitionData {
                    base_state: Default::default(),
                    receipts_hash: Default::default(),
                },
            )
            .unwrap();
        store_update
            .insert_ser(
                DBCol::BlockHeader,
                block_hash.as_bytes().as_ref(),
                &create_block_header(block_hash, block_height),
            )
            .unwrap();
        store_update
            .set_ser(DBCol::BlockPerHeight, blocks_per_height_key.as_ref(), &blocks_per_height)
            .unwrap();

        store_update.commit().unwrap();
    }

    // TODO(pugachag): currently there is no easy way to create BlockHeader
    // instance while only specifying a subset of fields. We need to create an
    // util for that, similar to TestBlockBuilder
    fn create_block_header(hash: CryptoHash, height: BlockHeight) -> BlockHeader {
        BlockHeader::BlockHeaderV4(Arc::new(BlockHeaderV4 {
            inner_lite: BlockHeaderInnerLite { height, ..BlockHeaderInnerLite::default() },
            hash,
            ..BlockHeaderV4::default()
        }))
    }
}
