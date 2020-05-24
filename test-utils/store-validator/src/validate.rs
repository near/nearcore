use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;

use borsh::BorshDeserialize;

use near_primitives::block::{Block, BlockHeader};
use near_primitives::borsh;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunk};
use near_primitives::types::{BlockHeight, EpochId};
use near_primitives::utils::index_to_bytes;
#[allow(unused)]
use near_store::{
    read_with_cache, ColBlock, ColBlockExtra, ColBlockHeader, ColBlockHeight, ColBlockMisc,
    ColBlockPerHeight, ColBlockRefCount, ColBlocksToCatchup, ColChallengedBlocks, ColChunkExtra,
    ColChunkPerHeightShard, ColChunks, ColEpochLightClientBlocks, ColIncomingReceipts,
    ColInvalidChunks, ColLastBlockWithNewChunk, ColNextBlockHashes, ColNextBlockWithNewChunk,
    ColOutgoingReceipts, ColPartialChunks, ColReceiptIdToShardId, ColState, ColStateChanges,
    ColStateDlInfos, ColStateHeaders, ColTransactionResult, ColTransactions, ColTrieChanges, DBCol,
    KeyForStateChanges, ShardTries, Store, StoreUpdate, Trie, TrieChanges, TrieIterator,
    WrappedTrieChanges, TAIL_KEY,
};

use crate::{ErrorMessage, StoreValidator};

macro_rules! get_parent_function_name {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        (&name[..name.len() - 3].split("::").last().unwrap()).to_string()
    }};
}

macro_rules! err {
    ($($x: tt),*) => (
        Err(ErrorMessage::new(get_parent_function_name!(), format!($($x),*)))
    )
}

macro_rules! unwrap_or_err {
    ($obj: expr, $($x: tt),*) => {
        match $obj {
            Ok(value) => value,
            Err(e) => {
                return Err(ErrorMessage::new(get_parent_function_name!(), format!("{}, error: {}", format!($($x),*), e)))
            }
        }
    };
}

// All validations start here

pub(crate) fn nothing(
    _sv: &StoreValidator,
    _key: &[u8],
    _value: &[u8],
) -> Result<(), ErrorMessage> {
    // Make sure that validation is executed
    Ok(())
}

pub(crate) fn block_header_validity(
    _sv: &StoreValidator,
    key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let block_hash =
        unwrap_or_err!(CryptoHash::try_from(key.as_ref()), "Can't deserialize Block Hash");
    let header =
        unwrap_or_err!(BlockHeader::try_from_slice(value), "Can't deserialize Block Header");
    if header.hash() != &block_hash {
        return err!("Invalid Block Header stored, hash = {:?}, header = {:?}", block_hash, header);
    }
    Ok(())
}

pub(crate) fn block_hash_validity(
    _sv: &StoreValidator,
    key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let block_hash =
        unwrap_or_err!(CryptoHash::try_from(key.as_ref()), "Can't deserialize Block Hash");
    let block = unwrap_or_err!(Block::try_from_slice(value), "Can't deserialize Block");
    if block.hash() != &block_hash {
        return err!("Invalid Block stored, hash = {:?}, block = {:?}", block_hash, block);
    }
    Ok(())
}

pub(crate) fn block_header_exists(
    sv: &StoreValidator,
    key: &[u8],
    _value: &[u8],
) -> Result<(), ErrorMessage> {
    let block_hash =
        unwrap_or_err!(CryptoHash::try_from(key.as_ref()), "Can't deserialize Block Hash");
    let header = unwrap_or_err!(
        sv.store.get_ser::<BlockHeader>(ColBlockHeader, block_hash.as_ref()),
        "Can't get Block Header from storage"
    );
    match header {
        Some(_) => Ok(()),
        None => err!("Block Header not found"),
    }
}

pub(crate) fn chunk_hash_validity(
    _sv: &StoreValidator,
    key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let chunk_hash =
        unwrap_or_err!(ChunkHash::try_from_slice(key.as_ref()), "Can't deserialize Chunk Hash");
    let shard_chunk =
        unwrap_or_err!(ShardChunk::try_from_slice(value), "Can't deserialize ShardChunk");
    if shard_chunk.chunk_hash != chunk_hash {
        return err!("Invalid ShardChunk stored");
    }
    Ok(())
}

pub(crate) fn block_of_chunk_exists(
    sv: &StoreValidator,
    _key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let shard_chunk =
        unwrap_or_err!(ShardChunk::try_from_slice(value), "Can't deserialize ShardChunk");
    let height = shard_chunk.header.height_included;
    let map = unwrap_or_err!(
        sv.store.get_ser::<HashMap<EpochId, HashSet<CryptoHash>>>(
            ColBlockPerHeight,
            &index_to_bytes(height),
        ),
        "Can't get Map from storage on height {:?}, no one is responsible for ShardChunk {:?}",
        height,
        shard_chunk
    );
    match map {
        Some(map) => {
            for (_, set) in map {
                for block_hash in set {
                    match sv.store.get_ser::<Block>(ColBlock, block_hash.as_ref()) {
                        Ok(Some(block)) => {
                            if block.chunks.contains(&shard_chunk.header) {
                                // Block for ShardChunk is found
                                return Ok(());
                            }
                        }
                        _ => {}
                    }
                }
            }
            err!("No Block on height {:?} accepts ShardChunk {:?}", height, shard_chunk)
        }
        None => err!(
            "Map is empty on height {:?}, no one is responsible for ShardChunk {:?}",
            height,
            shard_chunk
        ),
    }
}

pub(crate) fn block_height_cmp_tail(
    sv: &StoreValidator,
    _key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let tail = unwrap_or_err!(
        sv.store.get_ser::<BlockHeight>(ColBlockMisc, TAIL_KEY),
        "Can't get Tail from storage"
    )
    .unwrap_or(sv.config.genesis_height);
    let block = unwrap_or_err!(Block::try_from_slice(value), "Can't deserialize Block");
    if block.header.height() < tail && block.header.height() != sv.config.genesis_height {
        return err!("Invalid block height stored: {}, tail: {:?}", (block.header.height()), tail);
    }
    Ok(())
}

pub(crate) fn chunks_state_roots_in_trie(
    sv: &StoreValidator,
    _key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let shard_chunk: ShardChunk =
        unwrap_or_err!(ShardChunk::try_from_slice(value), "Can't deserialize ShardChunk");
    let shard_id = shard_chunk.header.inner.shard_id;
    let state_root = shard_chunk.header.inner.prev_state_root;
    let trie = sv.shard_tries.get_trie_for_shard(shard_id);
    let trie = TrieIterator::new(&trie, &state_root).unwrap();
    for item in trie {
        unwrap_or_err!(item, "Can't find ShardChunk {:?} in Trie", shard_chunk);
    }
    Ok(())
}
