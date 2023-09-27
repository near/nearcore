use chrono::Utc;
use near_chain::types::{EpochManagerAdapter, LatestKnown};
use near_chain::{ChainStore, ChainStoreAccess, ChainStoreUpdate};
use near_primitives::block::Tip;
use near_primitives::utils::to_timestamp;

pub mod cli;

pub fn undo_block(
    chain_store: &mut ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
) -> anyhow::Result<()> {
    let current_head = chain_store.head()?;
    let current_head_hash = current_head.last_block_hash;
    let prev_block_hash = current_head.prev_block_hash;
    let prev_header = chain_store.get_block_header(&prev_block_hash)?;
    let prev_tip = Tip::from_header(&prev_header);
    let current_head_height = current_head.height;
    let prev_block_height = prev_tip.height;

    tracing::info!(target: "neard", ?prev_block_hash, ?current_head_hash, ?prev_block_height, ?current_head_height, "Trying to update head");

    // stop if it's already the final block
    if chain_store.final_head()?.height >= current_head.height {
        return Err(anyhow::anyhow!("Cannot revert past final block"));
    }

    let mut chain_store_update = ChainStoreUpdate::new(chain_store);

    chain_store_update.clear_head_block_data(epoch_manager)?;

    chain_store_update.save_head(&prev_tip)?;

    chain_store_update.commit()?;

    chain_store.save_latest_known(LatestKnown {
        height: prev_tip.height,
        seen: to_timestamp(Utc::now()),
    })?;

    let new_chain_store_head = chain_store.head()?;
    let new_chain_store_header_head = chain_store.header_head()?;
    let new_head_height = new_chain_store_head.height;
    let new_header_height = new_chain_store_header_head.height;

    tracing::info!(target: "neard", ?new_head_height, ?new_header_height, "The current chain store shows");
    Ok(())
}

pub fn undo_only_block_head(
    chain_store: &mut ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
) -> anyhow::Result<()> {
    let current_head = chain_store.head()?;
    let current_head_height = current_head.height;
    let current_header_head = chain_store.header_head()?;
    let current_header_height = current_header_head.height;

    let tail_height = chain_store.tail()?;
    let tail_header = chain_store.get_block_header_by_height(tail_height)?;
    let new_head = Tip::from_header(&tail_header);

    tracing::info!(target: "neard", ?tail_height, ?current_head_height, ?current_header_height, "Trying to update head");

    if current_head_height == tail_height {
        tracing::info!(target: "neard", "Body head is alreay at the oldest block.");
        return Ok(());
    }

    let mut chain_store_update = ChainStoreUpdate::new(chain_store);
    chain_store_update.clear_head_block_data(epoch_manager)?;
    chain_store_update.save_body_head(&new_head)?;
    chain_store_update.commit()?;

    let new_chain_store_head = chain_store.head()?;
    let new_chain_store_header_head = chain_store.header_head()?;
    let new_head_height = new_chain_store_head.height;
    let new_header_height = new_chain_store_header_head.height;

    tracing::info!(target: "neard", ?new_head_height, ?new_header_height, "The current chain store shows");
    Ok(())
}
