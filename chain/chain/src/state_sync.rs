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

fn iter_state_sync_new_chunks_keys<'a>(
    store: &'a Store,
) -> impl Iterator<Item = Result<CryptoHash, std::io::Error>> + 'a {
    store
        .iter(DBCol::StateSyncNewChunks)
        .map(|item| item.and_then(|(k, _v)| CryptoHash::try_from_slice(&k)))
}

fn iter_state_sync_hashes_keys<'a>(
    store: &'a Store,
) -> impl Iterator<Item = Result<EpochId, std::io::Error>> + 'a {
    store
        .iter(DBCol::StateSyncHashes)
        .map(|item| item.and_then(|(k, _v)| EpochId::try_from_slice(&k)))
}

fn save_epoch_new_chunks<T: ChainStoreAccess>(
    chain_store: &T,
    store_update: &mut StoreUpdate,
    header: &BlockHeader,
) -> Result<(), Error> {
    let Some(mut num_new_chunks) =
        get_state_sync_new_chunks(chain_store.store(), header.prev_hash())?
    else {
        // This might happen in the case of epoch sync where we save individual headers without having all
        // headers that belong to the epoch.
        return Ok(());
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
        return Ok(());
    }

    let done = num_new_chunks.iter().all(|num_chunks| *num_chunks >= 2);
    if done {
        // TODO(current_epoch_state_sync): this will not be correct if this block doesn't end up finalized on the main chain.
        // We should fix it by setting the sync hash when it's finalized, which requires making changes to how we take state snapshots.
        store_update.set_ser(DBCol::StateSyncHashes, header.epoch_id().as_ref(), header.hash())?;
        store_update.delete_all(DBCol::StateSyncNewChunks);
        return Ok(());
    }
    for (num_new_chunks, new_chunk) in num_new_chunks.iter_mut().zip(header.chunk_mask().iter()) {
        // Only need to reach 2, so don't bother adding more than that
        if *new_chunk && *num_new_chunks < 2 {
            *num_new_chunks += 1;
        }
    }

    store_update.set_ser(DBCol::StateSyncNewChunks, header.hash().as_ref(), &num_new_chunks)?;
    Ok(())
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

fn remove_old_blocks<T: ChainStoreAccess>(
    chain_store: &T,
    store_update: &mut StoreUpdate,
    header: &BlockHeader,
) -> Result<(), Error> {
    if header.last_final_block() == &CryptoHash::default() {
        return Ok(());
    }
    // We don't need to keep info for old blocks around. After a block is finalized, we don't need anything before it
    let last_final_header = match chain_store.get_block_header(header.last_final_block()) {
        Ok(h) => h,
        // This might happen in the case of epoch sync where we save individual headers without having all
        // headers that belong to the epoch.
        Err(Error::DBNotFoundErr(_)) => return Ok(()),
        Err(e) => return Err(e),
    };
    for block_hash in iter_state_sync_new_chunks_keys(chain_store.store()) {
        let block_hash = block_hash?;
        let old_header = chain_store.get_block_header(&block_hash)?;
        if old_header.height() < last_final_header.height() {
            store_update.delete(DBCol::StateSyncNewChunks, block_hash.as_ref());
        }
    }

    Ok(())
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
        // columnn for this block. This means we will no longer remember sync hashes for these old epochs, which
        // should be fine as we only care to state sync to (and provide state parts for) the latest state
        on_new_epoch(store_update, header)?;
        return remove_old_epochs(chain_store.store(), store_update, header, &prev_header);
    }

    save_epoch_new_chunks(chain_store, store_update, header)?;
    remove_old_blocks(chain_store, store_update, header)
}
