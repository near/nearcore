use crate::Block;
use crate::metrics::BLOCK_HEIGHT_SPICE_EXECUTION_HEAD;
use crate::spice::boundary::is_last_pre_spice_block;
use crate::spice::core::get_execution_result_from_store;
use crate::types::{RuntimeAdapter, Tip};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use std::sync::Arc;

/// Block-level finalize. Owns the entire commit lifecycle because the work
/// straddles the commit boundary, unlike `apply_chunk_postprocessing` which
/// takes `&mut StoreUpdate` and lets the caller commit.
///
/// **Pre-commit phase.** Forward-only writes to `spice_execution_head` and
/// `spice_final_execution_head`, both into one `StoreUpdate` so the heads
/// advance atomically. Forward-only setters make the writes idempotent. The
/// final execution head moves only to a fully certified block.
///
/// **Post-commit phase.** Flat-storage advance and memtrie GC. These touch
/// subsystems outside the spice `StoreUpdate` — flat storage commits its own
/// internal updates, memtrie GC mutates an in-memory cache. They must observe
/// the durably-written heads before they run, otherwise a reader between pre-
/// and post-commit would see an inconsistent split. Both ops are idempotent.
///
/// Calling twice on the same block reaches the same end state.
pub fn apply_block_postprocessing(
    runtime_adapter: &dyn RuntimeAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    chain_store: &ChainStoreAdapter,
    block: &Block,
) -> Result<(), Error> {
    let mut store_update = chain_store.store().store_update();
    let new_final = certified_final_execution_head(epoch_manager, chain_store, block.header())?;
    if let Some(new_final) = &new_final {
        store_update.chain_store_update().set_spice_final_execution_head(new_final);
    }
    store_update
        .chain_store_update()
        .set_spice_execution_head(&Tip::from_header(block.header()))?;
    BLOCK_HEIGHT_SPICE_EXECUTION_HEAD.set(block.header().height() as i64);
    store_update.commit();

    let final_head = match new_final {
        Some(new_final) => Arc::new(new_final),
        None => chain_store.spice_final_execution_head()?,
    };
    let shard_layout =
        epoch_manager.get_shard_layout_from_prev_block(block.header().prev_hash())?;
    update_flat_storage_head(runtime_adapter, &shard_layout, &final_head)?;
    gc_memtrie_roots(runtime_adapter, chain_store, &shard_layout, &final_head);
    Ok(())
}

/// The highest fully certified block above the current final execution head and at or
/// below `header`'s last final block, if any. Walks forward from the current head, so
/// each block is checked once it becomes final.
fn certified_final_execution_head(
    epoch_manager: &dyn EpochManagerAdapter,
    chain_store: &ChainStoreAdapter,
    header: &BlockHeader,
) -> Result<Option<Tip>, Error> {
    let last_final_hash = header.last_final_block();
    if last_final_hash == &CryptoHash::default() {
        return Ok(None);
    }
    let last_final_height = chain_store.get_block_header(last_final_hash)?.height();
    let mut head = chain_store.spice_final_execution_head()?.last_block_hash;
    let mut new_head = None;
    loop {
        // Final blocks are canonical, so the canonical successor leads to the last final block.
        let next_hash = match chain_store.get_next_block_hash(&head) {
            Ok(hash) => hash,
            Err(Error::DBNotFoundErr(_)) => return Ok(new_head),
            Err(err) => return Err(err),
        };
        let next = chain_store.get_block_header(&next_hash)?;
        if next.height() > last_final_height
            || !is_fully_certified(epoch_manager, chain_store, &next)?
        {
            return Ok(new_head);
        }
        head = *next.hash();
        new_head = Some(Tip::from_header(&next));
    }
}

/// Whether every chunk of `header`'s block is certified. Genesis and pre-spice blocks
/// other than the last one are certified by definition.
fn is_fully_certified(
    epoch_manager: &dyn EpochManagerAdapter,
    chain_store: &ChainStoreAdapter,
    header: &BlockHeader,
) -> Result<bool, Error> {
    if header.is_genesis()
        || (!header.is_spice() && !is_last_pre_spice_block(epoch_manager, header.hash())?)
    {
        return Ok(true);
    }
    let shard_layout = epoch_manager.get_shard_layout(header.epoch_id())?;
    Ok(shard_layout.shard_ids().all(|shard_id| {
        get_execution_result_from_store(chain_store, header.hash(), shard_id).is_some()
    }))
}

fn update_flat_storage_head(
    runtime_adapter: &dyn RuntimeAdapter,
    shard_layout: &ShardLayout,
    final_execution_head: &Tip,
) -> Result<(), Error> {
    // TODO(spice): Evaluate if using block before final_execution_head still
    // makes sense for spice. For now it's used mainly because it's used for
    // updating flat head without spice with the following reasoning: using
    // prev_block_hash should be required for `StateSnapshot` to be able to
    // make snapshot of flat storage at the epoch boundary.
    let new_flat_head = final_execution_head.prev_block_hash;
    // TODO(spice): handle state sync and resharding edge cases when updating
    // flat head.
    if new_flat_head == CryptoHash::default() {
        return Ok(());
    }
    let flat_storage_manager = runtime_adapter.get_flat_storage_manager();
    for shard_uid in shard_layout.shard_uids() {
        if flat_storage_manager.get_flat_storage_for_shard(shard_uid).is_none() {
            continue;
        }
        flat_storage_manager.update_flat_storage_for_shard(shard_uid, new_flat_head)?;
    }
    Ok(())
}

fn gc_memtrie_roots(
    runtime_adapter: &dyn RuntimeAdapter,
    chain_store: &ChainStoreAdapter,
    shard_layout: &ShardLayout,
    final_execution_head: &Tip,
) {
    let header = chain_store.get_block_header(&final_execution_head.last_block_hash).unwrap();
    let Some(prev_height) = header.prev_height() else {
        return;
    };
    let tries = runtime_adapter.get_tries();
    for shard_uid in shard_layout.shard_uids() {
        tries.delete_memtrie_roots_up_to_height(shard_uid, prev_height);
    }
}
