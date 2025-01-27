use near_chain_primitives::error::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::types::EpochId;
use near_store::{DBCol, Store, StoreUpdate};

use borsh::BorshDeserialize;

use crate::types::BlockHeader;
use crate::ChainStoreAccess;

fn get_state_sync_new_chunks(
    store: &Store,
    block_hash: &CryptoHash,
) -> Result<Option<Vec<u8>>, Error> {
    Ok(store.get_ser(DBCol::StateSyncNewChunks, block_hash.as_ref())?)
}

fn iter_state_sync_hashes_keys<'a>(
    store: &'a Store,
) -> impl Iterator<Item = Result<EpochId, std::io::Error>> + 'a {
    store
        .iter(DBCol::StateSyncHashes)
        .map(|item| item.and_then(|(k, _v)| EpochId::try_from_slice(&k)))
}

/// Saves new chunk info and returns whether there are at least 2 chunks per shard in the epoch for header.prev_hash()
fn save_epoch_new_chunks<T: ChainStoreAccess>(
    chain_store: &T,
    store_update: &mut StoreUpdate,
    header: &BlockHeader,
) -> Result<bool, Error> {
    let Some(mut num_new_chunks) =
        get_state_sync_new_chunks(&chain_store.store(), header.prev_hash())?
    else {
        // This might happen in the case of epoch sync where we save individual headers without having all
        // headers that belong to the epoch.
        return Ok(false);
    };

    // This shouldn't happen because block headers in the same epoch should have chunks masks
    // of the same length, but we log it here in case it happens for some reason. We return Ok because if this
    // happens, it's some bug in this state sync logic, because the chunk mask length of headers are checked when
    // they're verified. So in this case we shouldn't fail to commit this store update and store this block header
    if num_new_chunks.len() != header.chunk_mask().len() {
        tracing::error!(
            block_hash=%header.hash(), chunk_mask_len=%header.chunk_mask().len(), stored_len=%num_new_chunks.len(),
            "block header's chunk mask not of the same length as stored value in DBCol::StateSyncNewChunks",
        );
        return Ok(false);
    }

    let done = num_new_chunks.iter().all(|num_chunks| *num_chunks >= 2);

    for (num_new_chunks, new_chunk) in num_new_chunks.iter_mut().zip(header.chunk_mask().iter()) {
        // Only need to reach 2, so don't bother adding more than that
        if *new_chunk && *num_new_chunks < 2 {
            *num_new_chunks += 1;
        }
    }

    store_update.set_ser(DBCol::StateSyncNewChunks, header.hash().as_ref(), &num_new_chunks)?;
    Ok(done)
}

fn on_new_epoch(store_update: &mut StoreUpdate, header: &BlockHeader) -> Result<(), Error> {
    let num_new_chunks = vec![0u8; header.chunk_mask().len()];
    store_update.set_ser(DBCol::StateSyncNewChunks, header.hash().as_ref(), &num_new_chunks)?;
    Ok(())
}

fn remove_old_epochs(
    store: &Store,
    store_update: &mut StoreUpdate,
    header: &BlockHeader,
    prev_header: &BlockHeader,
) -> Result<(), Error> {
    for epoch_id in iter_state_sync_hashes_keys(store) {
        let epoch_id = epoch_id?;
        if &epoch_id != header.epoch_id() && &epoch_id != prev_header.epoch_id() {
            store_update.delete(DBCol::StateSyncHashes, epoch_id.as_ref());
        }
    }
    Ok(())
}

/// Helper to turn DBNotFoundErr() into None. We might get DBNotFoundErr() in the case of epoch sync
/// where we save individual headers without having all headers that belong to the epoch.
fn maybe_get_block_header<T: ChainStoreAccess>(
    chain_store: &T,
    block_hash: &CryptoHash,
) -> Result<Option<BlockHeader>, Error> {
    match chain_store.get_block_header(block_hash) {
        Ok(block_header) => Ok(Some(block_header)),
        // This might happen in the case of epoch sync where we save individual headers without having all
        // headers that belong to the epoch.
        Err(Error::DBNotFoundErr(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

fn has_enough_new_chunks(store: &Store, block_hash: &CryptoHash) -> Result<Option<bool>, Error> {
    let Some(num_new_chunks) = get_state_sync_new_chunks(store, block_hash)? else {
        // This might happen in the case of epoch sync where we save individual headers without having all
        // headers that belong to the epoch.
        return Ok(None);
    };
    Ok(Some(num_new_chunks.iter().all(|num_chunks| *num_chunks >= 2)))
}

/// Save num new chunks info and store the state sync hash if it has been found. We store it only
/// once it becomes final.
/// This should only be called if DBCol::StateSyncHashes does not yet have an entry for header.epoch_id().
/// The logic should still be correct if it is, but it's unnecessary and will waste a lot of time if called
/// on a header far away from the epoch start.
fn on_new_header<T: ChainStoreAccess>(
    chain_store: &T,
    store_update: &mut StoreUpdate,
    header: &BlockHeader,
) -> Result<(), Error> {
    let done = save_epoch_new_chunks(chain_store, store_update, header)?;
    if !done {
        return Ok(());
    }

    // Now check if the sync hash is known and finalized. The sync hash is the block after the first block with at least 2
    // chunks per shard in the epoch. Note that we cannot just check if the current header.last_final_block() is the sync
    // hash, because even though this function is called for each header, it is not guaranteed that we'll see every block
    // by checking header.last_final_block(), because it is possible for the final block to jump by more than one upon a new
    // head update. So here we iterate backwards until we find it, if it exists yet.

    let epoch_id = header.epoch_id();
    let last_final_hash = header.last_final_block();

    let Some(mut sync) = maybe_get_block_header(chain_store, last_final_hash)? else {
        return Ok(());
    };
    loop {
        let Some(sync_prev) = maybe_get_block_header(chain_store, sync.prev_hash())? else {
            return Ok(());
        };
        if sync_prev.epoch_id() != epoch_id
            || sync_prev.height() == chain_store.get_genesis_height()
        {
            return Ok(());
        }
        if has_enough_new_chunks(&chain_store.store(), sync_prev.hash())? != Some(true) {
            return Ok(());
        }

        let Some(sync_prev_prev) = maybe_get_block_header(chain_store, sync_prev.prev_hash())?
        else {
            return Ok(());
        };
        let Some(prev_prev_done) =
            has_enough_new_chunks(&chain_store.store(), sync_prev_prev.hash())?
        else {
            return Ok(());
        };

        if !prev_prev_done {
            // `sync_prev_prev` doesn't have enough new chunks, and `sync_prev` does, meaning `sync` is the first final
            // valid sync block
            store_update.set_ser(DBCol::StateSyncHashes, epoch_id.as_ref(), sync.hash())?;
            store_update.delete_all(DBCol::StateSyncNewChunks);
            return Ok(());
        }
        sync = sync_prev;
    }
}

/// Updates information in the DB related to calculating the correct "sync_hash" for this header's epoch,
/// if it hasn't already been found.
pub(crate) fn update_sync_hashes<T: ChainStoreAccess>(
    chain_store: &T,
    store_update: &mut StoreUpdate,
    header: &BlockHeader,
) -> Result<(), Error> {
    let sync_hash = chain_store.get_current_epoch_sync_hash(header.epoch_id())?;
    if sync_hash.is_some() || header.height() == chain_store.get_genesis_height() {
        return Ok(());
    }

    let prev_header = match chain_store.get_block_header(header.prev_hash()) {
        Ok(h) => h,
        // During epoch sync, we save headers whose prev headers might not exist, so we just do nothing in this case.
        // This means that we might not be able to state sync for this epoch, but for now this is not a problem.
        Err(Error::DBNotFoundErr(_)) => return Ok(()),
        Err(e) => return Err(e),
    };

    if prev_header.height() == chain_store.get_genesis_height() {
        return on_new_epoch(store_update, header);
    }
    if prev_header.epoch_id() != header.epoch_id() {
        // Here we remove any sync hashes stored for old epochs after saving [0,...,0] in the StateSyncNewChunks
        // column for this block. This means we will no longer remember sync hashes for these old epochs, which
        // should be fine as we only care to state sync to (and provide state parts for) the latest state
        on_new_epoch(store_update, header)?;
        return remove_old_epochs(&chain_store.store(), store_update, header, &prev_header);
    }

    on_new_header(chain_store, store_update, header)
}

///. Returns whether `block_hash` is the block that will appear immediately before the "sync_hash" block. That is,
/// whether it is going to be the prev_hash of the "sync_hash" block, when it is found.
///
/// `block_hash` is the prev_hash of the future "sync_hash" block iff it is the first block for which the
/// number of new chunks in the epoch in each shard is at least 2
///
/// This function can only return true before we save the "sync_hash" block to the `StateSyncHashes` column,
/// because it relies on data stored in the `StateSyncNewChunks` column, which is cleaned up after that.
///
/// This is used when making state snapshots, because in that case we don't need to wait for the "sync_hash"
/// block to be finalized to take a snapshot of the state as of its prev prev block
pub(crate) fn is_sync_prev_hash(
    store: &Store,
    block_hash: &CryptoHash,
    prev_hash: &CryptoHash,
) -> Result<bool, Error> {
    let Some(new_chunks) = get_state_sync_new_chunks(store, block_hash)? else {
        return Ok(false);
    };
    let done = new_chunks.iter().all(|num_chunks| *num_chunks >= 2);
    if !done {
        return Ok(false);
    }
    let Some(prev_new_chunks) = get_state_sync_new_chunks(store, prev_hash)? else {
        return Ok(false);
    };
    let prev_done = prev_new_chunks.iter().all(|num_chunks| *num_chunks >= 2);
    Ok(!prev_done)
}
