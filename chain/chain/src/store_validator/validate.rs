use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;

use borsh::BorshDeserialize;

use near_primitives::block::{Block, BlockHeader, Tip};
use near_primitives::borsh;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunk};
use near_primitives::types::{BlockHeight, EpochId};
use near_primitives::utils::index_to_bytes;
use near_store::{
    ColBlockHeader, ColBlockHeight, ColBlockMisc, ColBlockPerHeight, ColChunkHashesByHeight,
    ColChunks, CHUNK_TAIL_KEY, HEAD_KEY, TAIL_KEY,
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

pub(crate) fn head_tail_validity(
    sv: &mut StoreValidator,
    _key: &[u8],
    _value: &[u8],
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
    if chunk_tail > tail {
        return err!("chunk_tail > tail, {:?} > {:?}", chunk_tail, tail);
    }
    if tail > head.height {
        return err!("tail > head.height, {:?} > {:?}", tail, head);
    }
    Ok(())
}

pub(crate) fn block_header_validity(
    _sv: &mut StoreValidator,
    key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let block_hash =
        unwrap_or_err!(CryptoHash::try_from(key.as_ref()), "Can't deserialize Block Hash");
    let header =
        unwrap_or_err!(BlockHeader::try_from_slice(value), "Can't deserialize Block Header");
    if header.hash() != block_hash {
        return err!("Invalid Block Header stored, hash = {:?}, header = {:?}", block_hash, header);
    }
    Ok(())
}

pub(crate) fn block_hash_validity(
    sv: &mut StoreValidator,
    key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let block_hash =
        unwrap_or_err!(CryptoHash::try_from(key.as_ref()), "Can't deserialize Block Hash");
    let block = unwrap_or_err!(Block::try_from_slice(value), "Can't deserialize Block");

    // 1. Block Hash is valid
    if block.hash() != block_hash {
        return err!("Invalid Block stored, hash = {:?}, block = {:?}", block_hash, block);
    }

    // 2. Block is in ColBlockPerHeight
    let height = block.header.inner_lite.height;
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
    key: &[u8],
    _value: &[u8],
) -> Result<(), ErrorMessage> {
    let block_hash =
        unwrap_or_err!(CryptoHash::try_from(key.as_ref()), "Can't deserialize Block Hash");
    unwrap_or_err_db!(
        sv.store.get_ser::<BlockHeader>(ColBlockHeader, block_hash.as_ref()),
        "Can't get Block Header from storage"
    );
    Ok(())
}

pub(crate) fn chunk_basic_validity(
    _sv: &mut StoreValidator,
    key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let chunk_hash =
        unwrap_or_err!(ChunkHash::try_from_slice(key.as_ref()), "Can't deserialize Chunk Hash");
    let shard_chunk =
        unwrap_or_err!(ShardChunk::try_from_slice(value), "Can't deserialize ShardChunk");
    if shard_chunk.chunk_hash != chunk_hash {
        return err!("Invalid ShardChunk {:?} stored", shard_chunk);
    }
    Ok(())
}

pub(crate) fn block_chunks_exist(
    sv: &mut StoreValidator,
    _key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let block = unwrap_or_err!(Block::try_from_slice(value), "Can't deserialize Block");
    for chunk_header in block.chunks {
        match &sv.me {
            Some(me) => {
                if sv.runtime_adapter.cares_about_shard(
                    Some(&me),
                    &block.header.prev_hash,
                    chunk_header.inner.shard_id,
                    true,
                ) || sv.runtime_adapter.will_care_about_shard(
                    Some(&me),
                    &block.header.prev_hash,
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

pub(crate) fn block_height_cmp_tail(
    sv: &mut StoreValidator,
    _key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let tail = unwrap_or_err!(
        sv.store.get_ser::<BlockHeight>(ColBlockMisc, TAIL_KEY),
        "Can't get Tail from storage"
    )
    .unwrap_or(sv.config.genesis_height);
    let block = unwrap_or_err!(Block::try_from_slice(value), "Can't deserialize Block");
    if block.header.inner_lite.height < tail
        && block.header.inner_lite.height != sv.config.genesis_height
    {
        sv.block_heights_less_tail.push(block.hash());
    }
    Ok(())
}

pub(crate) fn block_height_cmp_tail_count(
    sv: &mut StoreValidator,
    _key: &[u8],
    _value: &[u8],
) -> Result<(), ErrorMessage> {
    if sv.block_heights_less_tail.len() < 2 {
        Ok(())
    } else {
        let len = sv.block_heights_less_tail.len();
        let blocks = &sv.block_heights_less_tail;
        err!("Found {:?} Blocks with height lower than Tail, {:?}", len, blocks)
    }
}

pub(crate) fn block_indexed_by_height(
    sv: &mut StoreValidator,
    key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let height: BlockHeight =
        unwrap_or_err!(BlockHeight::try_from_slice(key), "Can't deserialize Height");
    let hash = unwrap_or_err!(CryptoHash::try_from(value), "Can't deserialize Block Hash");

    // 1. Block Header exists
    let header = unwrap_or_err_db!(
        sv.store.get_ser::<BlockHeader>(ColBlockHeader, hash.as_ref()),
        "Can't get Block Header {:?} from ColBlockHeader",
        hash
    );

    // 2. Height is valid
    if header.inner_lite.height != height {
        return err!("Block on Height {:?} doesn't have required Height, {:?}", height, header);
    }

    // 3. If prev Block exists, it's also on the Canonical Chain
    if height != sv.config.genesis_height {
        let prev_hash = header.prev_hash;
        let prev_header = unwrap_or_err_db!(
            sv.store.get_ser::<BlockHeader>(ColBlockHeader, prev_hash.as_ref()),
            "Can't get prev Block Header {:?} from ColBlockHeader",
            prev_hash
        );
        let prev_height = prev_header.inner_lite.height;
        let same_prev_hash = unwrap_or_err_db!(
            sv.store.get_ser::<CryptoHash>(ColBlockHeight, &index_to_bytes(prev_height)),
            "Can't get prev Block Hash from ColBlockHeight by Height, {:?}, {:?}",
            prev_height,
            prev_header
        );
        if prev_hash != same_prev_hash {
            return err!(
                "Prev Block Hashes in ColBlockHeight and ColBlockHeader are different, {:?}, {:?}",
                prev_hash,
                same_prev_hash
            );
        }

        // 4. There are no Blocks in range (prev_height, height) on the Canonical Chain
        for cur_height in prev_height + 1..height {
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

pub(crate) fn chunk_state_roots_in_trie(
    _sv: &mut StoreValidator,
    _key: &[u8],
    _value: &[u8],
) -> Result<(), ErrorMessage> {
    // TODO enable after fixing #2623
    /*
    let shard_chunk: ShardChunk =
        unwrap_or_err!(ShardChunk::try_from_slice(value), "Can't deserialize ShardChunk");
    let shard_id = shard_chunk.header.inner.shard_id;
    let state_root = shard_chunk.header.inner.prev_state_root;
    let trie = sv.runtime_adapter.get_trie_for_shard(shard_id);
    let trie = unwrap_or_err!(
        TrieIterator::new(&trie, &state_root),
        "Trie Node Missing for ShardChunk {:?}",
        shard_chunk
    );
    for item in trie {
        unwrap_or_err!(item, "Can't find ShardChunk {:?} in Trie", shard_chunk);
    }
    */
    Ok(())
}

pub(crate) fn chunk_indexed_by_height_created(
    sv: &mut StoreValidator,
    _key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let shard_chunk: ShardChunk =
        unwrap_or_err!(ShardChunk::try_from_slice(value), "Can't deserialize ShardChunk");
    let height = shard_chunk.header.inner.height_created;
    let chunk_hashes = unwrap_or_err_db!(
        sv.store.get_ser::<HashSet<ChunkHash>>(ColChunkHashesByHeight, &index_to_bytes(height)),
        "Can't get Chunks Set from storage on Height {:?}, no one is responsible for ShardChunk {:?}",
        height,
        shard_chunk
    );
    if !chunk_hashes.contains(&shard_chunk.chunk_hash) {
        err!("Can't find ShardChunk {:?} on Height {:?}", shard_chunk, height)
    } else {
        Ok(())
    }
}

pub(crate) fn chunk_of_height_exists(
    sv: &mut StoreValidator,
    key: &[u8],
    value: &[u8],
) -> Result<(), ErrorMessage> {
    let height: BlockHeight =
        unwrap_or_err!(BlockHeight::try_from_slice(key), "Can't deserialize Height");
    let chunk_hashes: HashSet<ChunkHash> =
        unwrap_or_err!(HashSet::<ChunkHash>::try_from_slice(value), "Can't deserialize Set");
    for chunk_hash in chunk_hashes {
        let shard_chunk = unwrap_or_err_db!(
            sv.store.get_ser::<ShardChunk>(ColChunks, chunk_hash.as_ref()),
            "Can't get Chunk from storage with ChunkHash {:?}",
            chunk_hash
        );
        if shard_chunk.header.inner.height_created != height {
            return err!("Invalid ShardChunk {:?} stored at Height {:?}", shard_chunk, height);
        }
    }
    Ok(())
}
