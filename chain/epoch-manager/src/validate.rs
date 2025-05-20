use near_chain_primitives::Error;
use near_primitives::block::Tip;
use near_primitives::errors::EpochError;
use near_primitives::optimistic_block::OptimisticBlock;
use near_store::{HEAD_KEY, Store};

use crate::EpochManagerAdapter;

/// Validate whether Optimistic Block is relevant to be processed,
/// even if its epoch id is not known yet.
/// TODO: move to the same crate as `validate_chunk_relevant`.
pub fn validate_optimistic_block_relevant(
    epoch_manager_adapter: &dyn EpochManagerAdapter,
    block: &OptimisticBlock,
    store: &Store,
) -> Result<bool, Error> {
    let head = store.get_ser::<Tip>(near_store::DBCol::BlockMisc, HEAD_KEY)?;
    let Some(head) = head else {
        return Ok(true);
    };

    if block.height() <= head.height {
        return Err(Error::InvalidBlockHeight(block.height()));
    }

    // A heuristic to prevent block height to jump too fast towards BlockHeight::max and cause
    // overflow-related problems
    let epoch_config = epoch_manager_adapter.get_epoch_config(&head.epoch_id)?;
    if block.height() > head.height + epoch_config.epoch_length * 20 {
        return Err(Error::InvalidBlockHeight(block.height()));
    }

    if !epoch_manager_adapter.should_validate_signatures() {
        return Ok(true);
    }

    // A heuristic to check signature of the block even if prev block hash is not saved yet
    let epoch_ids =
        match epoch_manager_adapter.get_epoch_id_from_prev_block(&block.prev_block_hash()) {
            Ok(epoch_id) => vec![epoch_id],
            Err(EpochError::MissingBlock(_)) => {
                epoch_manager_adapter.possible_epochs_of_height_around_tip(&head, block.height())?
            }
            Err(err) => return Err(err.into()),
        };

    for epoch_id in epoch_ids {
        let validator = epoch_manager_adapter.get_block_producer_info(&epoch_id, block.height())?;
        if block.signature.verify(block.hash().as_bytes(), validator.public_key()) {
            return Ok(true);
        }
    }

    Ok(false)
}
