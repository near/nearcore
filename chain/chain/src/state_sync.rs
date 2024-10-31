use near_chain_primitives::error::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::types::EpochId;
use near_store::{DBCol, Store, StoreUpdate};

use borsh::BorshDeserialize;
use std::collections::BinaryHeap;

use crate::types::BlockHeader;
use crate::ChainStoreAccess;

// Used for storing block headers in a heap that returns the lowest heights first
#[derive(PartialEq, Eq)]
struct BlockHeaderByHeight<'a>(&'a BlockHeader);

impl<'a> PartialOrd for BlockHeaderByHeight<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for BlockHeaderByHeight<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.0.height().cmp(&self.0.height())
    }
}

pub(crate) fn get_current_epoch_sync_hash(
    store: &Store,
    epoch_id: &EpochId,
) -> Result<Option<CryptoHash>, Error> {
    Ok(store.get_ser(DBCol::StateSyncHashes, epoch_id.as_ref())?)
}

fn get_epoch_new_chunks(
    store: &Store,
    block_hash: &CryptoHash,
) -> Result<Option<Vec<usize>>, Error> {
    Ok(store.get_ser(DBCol::StateSyncNewChunks, block_hash.as_ref())?)
}

fn iter_epoch_new_chunks_keys<'a>(
    store: &'a Store,
) -> impl Iterator<Item = Result<CryptoHash, std::io::Error>> + 'a {
    store
        .iter(DBCol::StateSyncNewChunks)
        .map(|item| item.and_then(|(k, _v)| CryptoHash::try_from_slice(&k)))
}

fn save_epoch_new_chunks<T: ChainStoreAccess>(
    chain_store: &T,
    store_update: &mut StoreUpdate,
    header: &BlockHeader,
) -> Result<(), Error> {
    let Some(mut num_new_chunks) = get_epoch_new_chunks(chain_store.store(), header.prev_hash())?
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

    let mut done = true;
    for (shard_index, num_new_chunks) in num_new_chunks.iter_mut().enumerate() {
        let new_chunk = header.chunk_mask()[shard_index];
        if new_chunk {
            *num_new_chunks += 1;
        }
        if *num_new_chunks < 2 {
            done = false;
        }
    }
    if done {
        // TODO(current_epoch_state_sync): this will not be correct if this block doesn't end up finalized on the main chain.
        // We should fix it by setting the sync hash when it's finalized, which requires making changes to how we take state snapshots.
        store_update.set_ser(DBCol::StateSyncHashes, header.epoch_id().as_ref(), header.hash())?;
        store_update.delete_all(DBCol::StateSyncNewChunks);
        // TODO: remove old ones
        return Ok(());
    }

    store_update.set_ser(DBCol::StateSyncNewChunks, header.hash().as_ref(), &num_new_chunks)?;
    Ok(())
}

fn update_sync_hashes<T: ChainStoreAccess>(
    chain_store: &T,
    store_update: &mut StoreUpdate,
    header: &BlockHeader,
) -> Result<(), Error> {
    let sync_hash = get_current_epoch_sync_hash(chain_store.store(), header.epoch_id())?;
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

    if prev_header.height() == chain_store.get_genesis_height()
        || prev_header.epoch_id() != header.epoch_id()
    {
        let num_new_chunks = vec![0usize; header.chunk_mask().len()];
        store_update.set_ser(DBCol::StateSyncNewChunks, header.hash().as_ref(), &num_new_chunks)?;
        return Ok(());
    }

    save_epoch_new_chunks(chain_store, store_update, header)?;

    if header.last_final_block() != &CryptoHash::default() {
        // We don't need to keep info for old blocks around. After a block is finalized, we don't need anything before it
        let last_final_header = match chain_store.get_block_header(header.last_final_block()) {
            Ok(h) => h,
            // This might happen in the case of epoch sync where we save individual headers without having all
            // headers that belong to the epoch.
            Err(Error::DBNotFoundErr(_)) => return Ok(()),
            Err(e) => return Err(e),
        };

        for block_hash in iter_epoch_new_chunks_keys(chain_store.store()) {
            let block_hash = block_hash?;
            let old_header = chain_store.get_block_header(&block_hash)?;
            if old_header.height() < last_final_header.height() {
                store_update.delete(DBCol::StateSyncNewChunks, block_hash.as_ref());
            }
        }
    }
    Ok(())
}

pub(crate) struct SyncHashTracker<'a> {
    headers_by_height: BinaryHeap<BlockHeaderByHeight<'a>>,
}

impl<'a> SyncHashTracker<'a> {
    pub(crate) fn new() -> Self {
        Self { headers_by_height: BinaryHeap::new() }
    }

    pub(crate) fn push_header(&mut self, header: &'a BlockHeader) {
        self.headers_by_height.push(BlockHeaderByHeight(header));
    }

    pub(crate) fn update_store<T: ChainStoreAccess>(
        mut self,
        chain_store: &T,
        store_update: &mut StoreUpdate,
    ) -> Result<(), Error> {
        while let Some(BlockHeaderByHeight(header)) = self.headers_by_height.pop() {
            update_sync_hashes(chain_store, store_update, header)?;
        }
        Ok(())
    }
}
