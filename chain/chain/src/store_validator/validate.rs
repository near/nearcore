use std::collections::{HashMap, HashSet};

use near_primitives::block::{Block, BlockHeader, Tip};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunk};
use near_primitives::types::{BlockHeight, ChunkExtra, EpochId, ShardId};
use near_primitives::utils::{get_block_shard_id, index_to_bytes};
use near_store::{
    ColBlock, ColBlockHeader, ColBlockHeight, ColBlockMisc, ColBlockPerHeight, ColChunkExtra,
    ColChunkHashesByHeight, ColChunks, TrieChanges, TrieIterator, CHUNK_TAIL_KEY, HEADER_HEAD_KEY,
    HEAD_KEY, TAIL_KEY,
};

use crate::{ErrorMessage, StoreValidator};

macro_rules! get_parent_function_name {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        &name[..name.len() - 3].split("::").last().unwrap()
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
        };
    };
}

macro_rules! unwrap_or_err_db {
    ($obj: expr, $($x: tt),*) => {
        match $obj {
            Ok(Some(value)) => value,
            Err(e) => {
                return Err(ErrorMessage::new(get_parent_function_name!(), format!("{}, error: {}", format!($($x),*), e)))
            }
            _ => {
                return Err(ErrorMessage::new(get_parent_function_name!(), format!($($x),*)))
            }
        };
    };
}

// All validations start here

pub(crate) fn head_tail_validity<T, U>(
    sv: &mut StoreValidator,
    _key: &T,
    _value: &U,
) -> Result<(), ErrorMessage> {
    let tail = unwrap_or_err!(
        sv.store.get_ser::<BlockHeight>(ColBlockMisc, TAIL_KEY),
        "Can't get Tail from storage"
    )
    .unwrap_or(sv.config.genesis_height);
    let chunk_tail = unwrap_or_err!(
        sv.store.get_ser::<BlockHeight>(ColBlockMisc, CHUNK_TAIL_KEY),
        "Can't get Chunk Tail from storage"
    )
    .unwrap_or(sv.config.genesis_height);
    let head = unwrap_or_err_db!(
        sv.store.get_ser::<Tip>(ColBlockMisc, HEAD_KEY),
        "Can't get Head from storage"
    );
    let header_head = unwrap_or_err_db!(
        sv.store.get_ser::<Tip>(ColBlockMisc, HEADER_HEAD_KEY),
        "Can't get Header Head from storage"
    );
    sv.inner.head = head.height;
    sv.inner.header_head = header_head.height;
    sv.inner.tail = tail;
    sv.inner.chunk_tail = chunk_tail;
    sv.inner.is_misc_set = true;
    if chunk_tail > tail {
        return err!("chunk_tail > tail, {:?} > {:?}", chunk_tail, tail);
    }
    if tail > head.height {
        return err!("tail > head.height, {:?} > {:?}", tail, head);
    }
    if head.height > header_head.height {
        return err!("head.height > header_head.height, {:?} > {:?}", tail, head);
    }
    Ok(())
}

pub(crate) fn block_header_hash_validity(
    _sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    header: &BlockHeader,
) -> Result<(), ErrorMessage> {
    if header.hash() != block_hash {
        return err!("Invalid Block Header stored, hash = {:?}, header = {:?}", block_hash, header);
    }
    Ok(())
}

pub(crate) fn block_header_height_validity(
    sv: &mut StoreValidator,
    _block_hash: &CryptoHash,
    header: &BlockHeader,
) -> Result<(), ErrorMessage> {
    if !sv.inner.is_misc_set {
        return err!("Can't validate, is_misc_set == false");
    }
    let height = header.height();
    let head = sv.inner.header_head;
    if height > head {
        return err!("Invalid Block Header stored, Head = {:?}, header = {:?}", head, header);
    }
    Ok(())
}

pub(crate) fn block_hash_validity(
    _sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    block: &Block,
) -> Result<(), ErrorMessage> {
    if block.hash() != block_hash {
        return err!("Invalid Block stored, hash = {:?}, block = {:?}", block_hash, block);
    }
    Ok(())
}

pub(crate) fn block_height_validity(
    sv: &mut StoreValidator,
    _block_hash: &CryptoHash,
    block: &Block,
) -> Result<(), ErrorMessage> {
    if !sv.inner.is_misc_set {
        return err!("Can't validate, is_misc_set == false");
    }
    let height = block.header().height();
    let tail = sv.inner.tail;
    if height < tail && height != sv.config.genesis_height {
        sv.inner.block_heights_less_tail.push(*block.hash());
    }
    sv.inner.is_block_height_cmp_tail_prepared = true;

    let head = sv.inner.head;
    if height > head {
        return err!("Invalid Block stored, Head = {:?}, block = {:?}", head, block);
    }
    Ok(())
}

pub(crate) fn block_indexed_by_height(
    sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    block: &Block,
) -> Result<(), ErrorMessage> {
    let height = block.header().height();
    let block_hashes: HashSet<CryptoHash> = unwrap_or_err_db!(
        sv.store.get_ser::<HashMap<EpochId, HashSet<CryptoHash>>>(
            ColBlockPerHeight,
            &index_to_bytes(height)
        ),
        "Can't get HashMap for Height {:?} from ColBlockPerHeight",
        height
    )
    .values()
    .flatten()
    .cloned()
    .collect();
    if !block_hashes.contains(&block_hash) {
        return err!("Block {:?} is not found in ColBlockPerHeight", block);
    }
    Ok(())
}

pub(crate) fn block_header_exists(
    sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    _block: &Block,
) -> Result<(), ErrorMessage> {
    unwrap_or_err_db!(
        sv.store.get_ser::<BlockHeader>(ColBlockHeader, block_hash.as_ref()),
        "Can't get Block Header from storage"
    );
    Ok(())
}

pub(crate) fn chunk_hash_validity(
    _sv: &mut StoreValidator,
    chunk_hash: &ChunkHash,
    shard_chunk: &ShardChunk,
) -> Result<(), ErrorMessage> {
    if shard_chunk.chunk_hash != *chunk_hash {
        return err!("Invalid ShardChunk {:?} stored", shard_chunk);
    }
    Ok(())
}

pub(crate) fn chunk_tail_validity(
    sv: &mut StoreValidator,
    _chunk_hash: &ChunkHash,
    shard_chunk: &ShardChunk,
) -> Result<(), ErrorMessage> {
    if !sv.inner.is_misc_set {
        return err!("Can't validate, is_misc_set == false");
    }
    let chunk_tail = sv.inner.chunk_tail;
    let height = shard_chunk.header.inner.height_created;
    if height < chunk_tail {
        return err!(
            "Invalid ShardChunk stored, chunk_tail = {:?}, ShardChunk = {:?}",
            chunk_tail,
            shard_chunk
        );
    }
    Ok(())
}

pub(crate) fn chunk_indexed_by_height_created(
    sv: &mut StoreValidator,
    _chunk_hash: &ChunkHash,
    shard_chunk: &ShardChunk,
) -> Result<(), ErrorMessage> {
    let height = shard_chunk.header.inner.height_created;
    let chunk_hashes = unwrap_or_err_db!(
        sv.store.get_ser::<HashSet<ChunkHash>>(ColChunkHashesByHeight, &index_to_bytes(height)),
        "Can't get Chunks Set from storage on Height {:?}, no one is responsible for ShardChunk {:?}",
        height,
        shard_chunk
    );
    if !chunk_hashes.contains(&shard_chunk.chunk_hash) {
        return err!("Can't find ShardChunk {:?} on Height {:?}", shard_chunk, height);
    }
    Ok(())
}

pub(crate) fn block_chunks_exist(
    sv: &mut StoreValidator,
    _block_hash: &CryptoHash,
    block: &Block,
) -> Result<(), ErrorMessage> {
    for chunk_header in block.chunks().iter() {
        match &sv.me {
            Some(me) => {
                if sv.runtime_adapter.cares_about_shard(
                    Some(&me),
                    block.header().prev_hash(),
                    chunk_header.inner.shard_id,
                    true,
                ) || sv.runtime_adapter.will_care_about_shard(
                    Some(&me),
                    block.header().prev_hash(),
                    chunk_header.inner.shard_id,
                    true,
                ) {
                    unwrap_or_err_db!(
                        sv.store
                            .get_ser::<ShardChunk>(ColChunks, chunk_header.chunk_hash().as_ref()),
                        "Can't get Chunk {:?} from storage",
                        chunk_header
                    );
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn block_chunks_height_validity(
    _sv: &mut StoreValidator,
    _block_hash: &CryptoHash,
    block: &Block,
) -> Result<(), ErrorMessage> {
    for chunk_header in block.chunks().iter() {
        if chunk_header.inner.height_created > block.header().height() {
            return err!(
                "Invalid ShardChunk included, chunk_header = {:?}, block = {:?}",
                chunk_header,
                block
            );
        }
    }
    Ok(())
}

pub(crate) fn block_height_cmp_tail<T, U>(
    sv: &mut StoreValidator,
    _key: &T,
    _value: &U,
) -> Result<(), ErrorMessage> {
    if !sv.inner.is_misc_set {
        return err!("Can't validate, is_block_height_cmp_tail_prepared == false");
    }
    if sv.inner.block_heights_less_tail.len() < 2 {
        Ok(())
    } else {
        let len = sv.inner.block_heights_less_tail.len();
        let blocks = &sv.inner.block_heights_less_tail;
        err!("Found {:?} Blocks with height lower than Tail, {:?}", len, blocks)
    }
}

pub(crate) fn canonical_header_validity(
    sv: &mut StoreValidator,
    height: &BlockHeight,
    hash: &CryptoHash,
) -> Result<(), ErrorMessage> {
    let header = unwrap_or_err_db!(
        sv.store.get_ser::<BlockHeader>(ColBlockHeader, hash.as_ref()),
        "Can't get Block Header {:?} from ColBlockHeader",
        hash
    );
    if header.height() != *height {
        return err!("Block on Height {:?} doesn't have required Height, {:?}", height, header);
    }
    Ok(())
}

pub(crate) fn canonical_prev_block_validity(
    sv: &mut StoreValidator,
    height: &BlockHeight,
    hash: &CryptoHash,
) -> Result<(), ErrorMessage> {
    if *height != sv.config.genesis_height {
        let header = unwrap_or_err_db!(
            sv.store.get_ser::<BlockHeader>(ColBlockHeader, hash.as_ref()),
            "Can't get Block Header {:?} from ColBlockHeader",
            hash
        );
        let prev_hash = *header.prev_hash();
        let prev_header = unwrap_or_err_db!(
            sv.store.get_ser::<BlockHeader>(ColBlockHeader, prev_hash.as_ref()),
            "Can't get prev Block Header {:?} from ColBlockHeader",
            prev_hash
        );
        let prev_height = prev_header.height();
        let same_prev_hash = unwrap_or_err_db!(
            sv.store.get_ser::<CryptoHash>(ColBlockHeight, &index_to_bytes(prev_height)),
            "Can't get prev Block Hash from ColBlockHeight by Height, {:?}, {:?}",
            prev_height,
            prev_header
        );
        if prev_hash != same_prev_hash {
            return err!(
                "Prev Block Hashes in ColBlockHeight and ColBlockHeader at height {:?} are different, {:?}, {:?}",
                prev_height,
                prev_hash,
                same_prev_hash
            );
        }

        for cur_height in prev_height + 1..*height {
            let cur_hash = unwrap_or_err!(
                sv.store.get_ser::<CryptoHash>(ColBlockHeight, &index_to_bytes(cur_height)),
                "DB error while getting Block Hash from ColBlockHeight by Height {:?}",
                cur_height
            );
            if cur_hash.is_some() {
                return err!("Unexpected Block on the Canonical Chain is found between Heights {:?} and {:?}, {:?}", prev_height, height, cur_hash);
            }
        }
    }
    Ok(())
}

pub(crate) fn trie_changes_chunk_extra_exists(
    sv: &mut StoreValidator,
    (block_hash, shard_id): &(CryptoHash, ShardId),
    trie_changes: &TrieChanges,
) -> Result<(), ErrorMessage> {
    let new_root = trie_changes.new_root;
    // 1. Block with `block_hash` should be available
    let block = unwrap_or_err_db!(
        sv.store.get_ser::<Block>(ColBlock, block_hash.as_ref()),
        "Can't get Block from DB"
    );
    // 2. There should be ShardChunk with ShardId `shard_id`
    for chunk_header in block.chunks().iter() {
        if chunk_header.inner.shard_id == *shard_id {
            let chunk_hash = &chunk_header.hash;
            // 3. ShardChunk with `chunk_hash` should be available
            unwrap_or_err_db!(
                sv.store.get_ser::<ShardChunk>(ColChunks, chunk_hash.as_ref()),
                "Can't get Chunk from storage with ChunkHash {:?}",
                chunk_hash
            );
            // 4. Chunk Extra with `block_hash` and `shard_id` should be available
            let chunk_extra = unwrap_or_err_db!(
                sv.store.get_ser::<ChunkExtra>(
                    ColChunkExtra,
                    &get_block_shard_id(block_hash, *shard_id)
                ),
                "Can't get Chunk Extra from storage with key {:?} {:?}",
                block_hash,
                shard_id
            );
            let trie = sv.runtime_adapter.get_trie_for_shard(*shard_id);
            let trie_iterator = unwrap_or_err!(
                TrieIterator::new(&trie, &new_root),
                "Trie Node Missing for ShardChunk {:?}",
                chunk_header
            );
            // 5. ShardChunk `shard_chunk` should be available in Trie
            for item in trie_iterator {
                unwrap_or_err!(item, "Can't find ShardChunk {:?} in Trie", chunk_header);
            }
            // 6. Prev State Roots should be equal
            // TODO #2623: enable
            /*
            #[cfg(feature = "adversarial")]
            {
                let prev_state_root = chunk_header.inner.prev_state_root;
                let old_root = trie_changes.adv_get_old_root();
                if prev_state_root != old_root {
                    return err!(
                        "Prev State Root discrepancy, {:?} != {:?}, ShardChunk {:?}",
                        old_root,
                        prev_state_root,
                        chunk_header
                    );
                }
            }
            */
            // 7. State Roots should be equal
            let state_root = chunk_extra.state_root;
            return if state_root == new_root {
                Ok(())
            } else {
                err!(
                    "State Root discrepancy, {:?} != {:?}, ShardChunk {:?}",
                    new_root,
                    state_root,
                    chunk_header
                )
            };
        }
    }
    err!("ShardChunk is not included into Block {:?}", block)
}

pub(crate) fn chunk_of_height_exists(
    sv: &mut StoreValidator,
    height: &BlockHeight,
    chunk_hashes: &HashSet<ChunkHash>,
) -> Result<(), ErrorMessage> {
    for chunk_hash in chunk_hashes {
        let shard_chunk = unwrap_or_err_db!(
            sv.store.get_ser::<ShardChunk>(ColChunks, chunk_hash.as_ref()),
            "Can't get Chunk from storage with ChunkHash {:?}",
            chunk_hash
        );
        if shard_chunk.header.inner.height_created != *height {
            return err!("Invalid ShardChunk {:?} stored at Height {:?}", shard_chunk, height);
        }
    }
    Ok(())
}
