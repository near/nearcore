use std::collections::{HashMap, HashSet};

use borsh::BorshSerialize;
use thiserror::Error;

use near_primitives::block::{Block, BlockHeader, Tip};
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunk, StateSyncInfo};
use near_primitives::syncing::{
    get_num_state_parts, ShardStateSyncResponseHeader, StateHeaderKey, StatePartKey,
};
use near_primitives::transaction::{ExecutionOutcomeWithIdAndProof, SignedTransaction};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, EpochId, ShardId};
use near_primitives::utils::{get_block_shard_id, index_to_bytes};
use near_store::{
    ColBlock, ColBlockHeader, ColBlockHeight, ColBlockInfo, ColBlockMisc, ColBlockPerHeight,
    ColChunkExtra, ColChunkHashesByHeight, ColChunks, ColHeaderHashesByHeight, ColOutcomeIds,
    ColStateHeaders, ColTransactionResult, DBCol, TrieChanges, TrieIterator, CHUNK_TAIL_KEY,
    FORK_TAIL_KEY, HEADER_HEAD_KEY, HEAD_KEY, NUM_COLS, SHOULD_COL_GC, TAIL_KEY,
};

use crate::StoreValidator;

#[derive(Error, Debug)]
pub enum StoreValidatorError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error("DB is corrupted")]
    DBCorruption(#[from] Box<dyn std::error::Error>),
    #[error("Function {func_name:?}: data is invalid, {reason:?}")]
    InvalidData { func_name: String, reason: String },
    #[error("Function {func_name:?}: data that expected to exist in DB is not found, {reason:?}")]
    DBNotFound { func_name: String, reason: String },
    #[error("Function {func_name:?}: {reason:?}, expected {expected:?}, found {found:?}")]
    Discrepancy { func_name: String, reason: String, expected: String, found: String },
    #[error("Function {func_name:?}: validation failed, {error:?}")]
    ValidationFailed { func_name: String, error: String },
}

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
        return Err(StoreValidatorError::ValidationFailed { func_name: get_parent_function_name!(), error: format!($($x),*) } );
    )
}

macro_rules! check_discrepancy {
    ($arg1: expr, $arg2: expr, $($x: tt),*) => {
        if $arg1 != $arg2 {
            return Err(StoreValidatorError::Discrepancy {
                func_name: get_parent_function_name!(),
                reason: format!($($x),*),
                expected: format!("{:?}", $arg1),
                found: format!("{:?}", $arg2),
            });
        }
    };
}

macro_rules! unwrap_or_err {
    ($obj: expr, $($x: tt),*) => {
        match $obj {
            Ok(value) => value,
            Err(e) => {
                return Err(StoreValidatorError::InvalidData {
                    func_name: get_parent_function_name!(),
                    reason: format!("{}, error: {}", format!($($x),*), e)
                })
            }
        };
    };
}

macro_rules! unwrap_or_err_db {
    ($obj: expr, $($x: expr),*) => {
        match $obj {
            Ok(Some(value)) => value,
            Err(e) => {
                return Err(StoreValidatorError::DBNotFound {
                    func_name: get_parent_function_name!(),
                    reason: format!("{}, error: {}", format!($($x),*), e)
                })
            }
            _ => {
                return Err(StoreValidatorError::DBNotFound {
                    func_name: get_parent_function_name!(),
                    reason: format!($($x),*)
                })
            }
        };
    };
}

// All validations start here

pub(crate) fn head_tail_validity(sv: &mut StoreValidator) -> Result<(), StoreValidatorError> {
    let mut tail = sv.config.genesis_height;
    let mut chunk_tail = sv.config.genesis_height;
    let mut fork_tail = sv.config.genesis_height;
    let tail_db = unwrap_or_err!(
        sv.store.get_ser::<BlockHeight>(ColBlockMisc, TAIL_KEY),
        "Can't get Tail from storage"
    );
    let chunk_tail_db = unwrap_or_err!(
        sv.store.get_ser::<BlockHeight>(ColBlockMisc, CHUNK_TAIL_KEY),
        "Can't get Chunk Tail from storage"
    );
    let fork_tail_db = unwrap_or_err!(
        sv.store.get_ser::<BlockHeight>(ColBlockMisc, FORK_TAIL_KEY),
        "Can't get Chunk Tail from storage"
    );
    if tail_db.is_none() && chunk_tail_db.is_some() || tail_db.is_some() && chunk_tail_db.is_none()
    {
        err!("Tail is {:?} and Chunk Tail is {:?}", tail_db, chunk_tail_db);
    }
    if tail_db.is_some() && fork_tail_db.is_none() {
        err!("Tail is {:?} but fork tail is None", tail_db);
    }
    if tail_db.is_some() {
        tail = tail_db.unwrap();
        chunk_tail = chunk_tail_db.unwrap();
        fork_tail = fork_tail_db.unwrap();
    }
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
    if chunk_tail > tail {
        err!("chunk_tail > tail, {:?} > {:?}", chunk_tail, tail);
    }
    if tail > head.height {
        err!("tail > head.height, {:?} > {:?}", tail, head);
    }
    if tail > fork_tail {
        err!("tail > fork_tail, {} > {}", tail, fork_tail);
    }
    if fork_tail > head.height {
        err!("fork tail > head.height, {} > {:?}", fork_tail, head);
    }
    if head.height > header_head.height {
        err!("head.height > header_head.height, {:?} > {:?}", tail, head);
    }
    Ok(())
}

pub(crate) fn block_header_hash_validity(
    _sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    header: &BlockHeader,
) -> Result<(), StoreValidatorError> {
    check_discrepancy!(header.hash(), block_hash, "Invalid Block Header stored");
    Ok(())
}

pub(crate) fn block_header_height_validity(
    sv: &mut StoreValidator,
    _block_hash: &CryptoHash,
    header: &BlockHeader,
) -> Result<(), StoreValidatorError> {
    let height = header.height();
    let head = sv.inner.header_head;
    if height > head {
        err!("Invalid Block Header stored, Head = {:?}, header = {:?}", head, header);
    }
    Ok(())
}

pub(crate) fn block_hash_validity(
    _sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    block: &Block,
) -> Result<(), StoreValidatorError> {
    check_discrepancy!(block.hash(), block_hash, "Invalid Block stored");
    Ok(())
}

pub(crate) fn block_height_validity(
    sv: &mut StoreValidator,
    _block_hash: &CryptoHash,
    block: &Block,
) -> Result<(), StoreValidatorError> {
    let height = block.header().height();
    let tail = sv.inner.tail;
    if height <= tail && height != sv.config.genesis_height {
        sv.inner.block_heights_less_tail.push(*block.hash());
    }
    let head = sv.inner.head;
    if height > head {
        err!("Invalid Block stored, Head = {:?}, block = {:?}", head, block);
    }
    Ok(())
}

pub(crate) fn block_indexed_by_height(
    sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    block: &Block,
) -> Result<(), StoreValidatorError> {
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
        err!("Block {:?} is not found in ColBlockPerHeight", block);
    }
    Ok(())
}

pub(crate) fn block_header_exists(
    sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    _block: &Block,
) -> Result<(), StoreValidatorError> {
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
) -> Result<(), StoreValidatorError> {
    check_discrepancy!(
        shard_chunk.chunk_hash(),
        *chunk_hash,
        "Invalid ShardChunk {:?} stored",
        shard_chunk
    );
    Ok(())
}

pub(crate) fn chunk_tail_validity(
    sv: &mut StoreValidator,
    _chunk_hash: &ChunkHash,
    shard_chunk: &ShardChunk,
) -> Result<(), StoreValidatorError> {
    let chunk_tail = sv.inner.chunk_tail;
    let height = shard_chunk.height_created();
    if height != sv.config.genesis_height && height < chunk_tail {
        err!(
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
) -> Result<(), StoreValidatorError> {
    let height = shard_chunk.height_created();
    let chunk_hashes = unwrap_or_err_db!(
        sv.store.get_ser::<HashSet<ChunkHash>>(ColChunkHashesByHeight, &index_to_bytes(height)),
        "Can't get Chunks Set from storage on Height {:?}, no one is responsible for ShardChunk {:?}",
        height,
        shard_chunk
    );
    if !chunk_hashes.contains(&shard_chunk.chunk_hash()) {
        err!("Can't find ShardChunk {:?} on Height {:?}", shard_chunk, height);
    }
    Ok(())
}

pub(crate) fn header_hash_indexed_by_height(
    sv: &mut StoreValidator,
    _hash: &CryptoHash,
    header: &BlockHeader,
) -> Result<(), StoreValidatorError> {
    let height = header.height();
    let _hashes = match sv
        .store
        .get_ser::<HashSet<CryptoHash>>(ColHeaderHashesByHeight, &index_to_bytes(height))
    {
        Ok(hashes) => hashes,
        Err(e) => err!("Storage error, {:?}", e),
    };
    // TODO #3488: enable
    // This check is disabled because currently we can accept Headers that below chunk_tail.
    // It creates a mess which records for ColHeaderHashesByHeight exist.
    // It will be resolved after #3488 is introduced by migration
    // that is removing Block Headers forcibly from the DB.

    /*if height < sv.inner.chunk_tail {
        // The data must be GCed
        if hashes.is_some() {
            err!(
                "ColHeaderHashesByHeight should be GCed, however for height {:?}, values {:?}",
                height,
                hashes
            )
        }
    } else {
        if hashes.is_none() || !hashes.unwrap().contains(&header.hash()) {
            err!("Can't find Header {:?} on Height {:?}", header, height);
        }
    }*/
    Ok(())
}

pub(crate) fn chunk_tx_exists(
    sv: &mut StoreValidator,
    _chunk_hash: &ChunkHash,
    shard_chunk: &ShardChunk,
) -> Result<(), StoreValidatorError> {
    for tx in shard_chunk.transactions().iter() {
        let tx_hash = tx.get_hash();
        sv.inner.tx_refcount.entry(tx_hash).and_modify(|x| *x += 1).or_insert(1);
    }
    for receipt in shard_chunk.receipts().iter() {
        sv.inner.receipt_refcount.entry(receipt.get_hash()).and_modify(|x| *x += 1).or_insert(1);
    }
    for tx in shard_chunk.transactions().iter() {
        let tx_hash = tx.get_hash();
        unwrap_or_err_db!(
            sv.store.get_ser::<SignedTransaction>(DBCol::ColTransactions, &tx_hash.as_ref()),
            "Can't get Tx from storage for Tx Hash {:?}",
            tx_hash
        );
    }
    Ok(())
}

pub(crate) fn block_chunks_exist(
    sv: &mut StoreValidator,
    _block_hash: &CryptoHash,
    block: &Block,
) -> Result<(), StoreValidatorError> {
    for chunk_header in block.chunks().iter() {
        if chunk_header.height_included() == block.header().height() {
            if let Some(me) = &sv.me {
                let cares_about_shard = sv.runtime_adapter.cares_about_shard(
                    Some(&me),
                    block.header().prev_hash(),
                    chunk_header.shard_id(),
                    true,
                );
                let will_care_about_shard = sv.runtime_adapter.will_care_about_shard(
                    Some(&me),
                    block.header().prev_hash(),
                    chunk_header.shard_id(),
                    true,
                );
                if cares_about_shard || will_care_about_shard {
                    unwrap_or_err_db!(
                        sv.store
                            .get_ser::<ShardChunk>(ColChunks, chunk_header.chunk_hash().as_ref()),
                        "Can't get Chunk {:?} from storage",
                        chunk_header
                    );
                    if cares_about_shard {
                        let block_shard_id =
                            get_block_shard_id(block.hash(), chunk_header.shard_id());
                        unwrap_or_err_db!(
                            sv.store.get_ser::<ChunkExtra>(ColChunkExtra, block_shard_id.as_ref()),
                            "Can't get chunk extra for chunk {:?} from storage",
                            chunk_header
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn block_chunks_height_validity(
    _sv: &mut StoreValidator,
    _block_hash: &CryptoHash,
    block: &Block,
) -> Result<(), StoreValidatorError> {
    for chunk_header in block.chunks().iter() {
        if chunk_header.height_created() > block.header().height() {
            err!(
                "Invalid ShardChunk included, chunk_header = {:?}, block = {:?}",
                chunk_header,
                block
            );
        }
    }
    Ok(())
}

pub(crate) fn block_info_exists(
    sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    _block: &Block,
) -> Result<(), StoreValidatorError> {
    unwrap_or_err_db!(
        sv.store.get_ser::<BlockInfo>(ColBlockInfo, block_hash.as_ref()),
        "Can't get BlockInfo from storage"
    );
    Ok(())
}

pub(crate) fn block_epoch_exists(
    _sv: &mut StoreValidator,
    _block_hash: &CryptoHash,
    _block: &Block,
) -> Result<(), StoreValidatorError> {
    // TODO #2893: why?
    /*
    unwrap_or_err_db!(
        sv.store.get_ser::<EpochInfo>(ColEpochInfo, block.header().epoch_id().as_ref()),
        "Can't get EpochInfo from storage"
    );
    */
    Ok(())
}

pub(crate) fn block_increase_refcount(
    sv: &mut StoreValidator,
    _block_hash: &CryptoHash,
    block: &Block,
) -> Result<(), StoreValidatorError> {
    if block.header().height() != sv.config.genesis_height {
        let prev_hash = block.header().prev_hash();
        sv.inner.block_refcount.entry(*prev_hash).and_modify(|x| *x += 1).or_insert(1);
    }
    Ok(())
}

pub(crate) fn canonical_header_validity(
    sv: &mut StoreValidator,
    height: &BlockHeight,
    hash: &CryptoHash,
) -> Result<(), StoreValidatorError> {
    let header = unwrap_or_err_db!(
        sv.store.get_ser::<BlockHeader>(ColBlockHeader, hash.as_ref()),
        "Can't get Block Header {:?} from ColBlockHeader",
        hash
    );
    if header.height() != *height {
        err!("Block on Height {:?} doesn't have required Height, {:?}", height, header);
    }
    Ok(())
}

pub(crate) fn canonical_prev_block_validity(
    sv: &mut StoreValidator,
    height: &BlockHeight,
    hash: &CryptoHash,
) -> Result<(), StoreValidatorError> {
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
        check_discrepancy!(
            prev_hash,
            same_prev_hash,
            "Prev Block Hashes in ColBlockHeight and ColBlockHeader at height {:?} are different",
            prev_height
        );

        for cur_height in prev_height + 1..*height {
            let cur_hash = unwrap_or_err!(
                sv.store.get_ser::<CryptoHash>(ColBlockHeight, &index_to_bytes(cur_height)),
                "DB error while getting Block Hash from ColBlockHeight by Height {:?}",
                cur_height
            );
            if cur_hash.is_some() {
                err!("Unexpected Block on the Canonical Chain is found between Heights {:?} and {:?}, {:?}", prev_height, height, cur_hash);
            }
        }
    }
    Ok(())
}

pub(crate) fn trie_changes_chunk_extra_exists(
    sv: &mut StoreValidator,
    (block_hash, shard_id): &(CryptoHash, ShardId),
    trie_changes: &TrieChanges,
) -> Result<(), StoreValidatorError> {
    let new_root = trie_changes.new_root;
    // 1. Block with `block_hash` should be available
    let block = unwrap_or_err_db!(
        sv.store.get_ser::<Block>(ColBlock, block_hash.as_ref()),
        "Can't get Block from DB"
    );
    // 2. There should be ShardChunk with ShardId `shard_id`
    for chunk_header in block.chunks().iter() {
        if chunk_header.shard_id() == *shard_id {
            let chunk_hash = chunk_header.chunk_hash();
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
            if chunk_header.height_included() == block.header().height() {
                check_discrepancy!(
                    chunk_header.prev_state_root(),
                    trie_changes.old_root,
                    "Prev State Root discrepancy, ShardChunk {:?}",
                    chunk_header
                );
            }
            if let Ok(Some(prev_chunk_extra)) = sv.store.get_ser::<ChunkExtra>(
                ColChunkExtra,
                &get_block_shard_id(block.header().prev_hash(), *shard_id),
            ) {
                check_discrepancy!(
                    prev_chunk_extra.state_root(),
                    &trie_changes.old_root,
                    "Prev State Root discrepancy, previous ChunkExtra {:?}",
                    prev_chunk_extra
                );
            }

            // 7. State Roots should be equal
            check_discrepancy!(
                chunk_extra.state_root(),
                &new_root,
                "State Root discrepancy, ShardChunk {:?}",
                chunk_header
            );
            return Ok(());
        }
    }
    err!("ShardChunk is not included into Block {:?}", block)
}

pub(crate) fn chunk_of_height_exists(
    sv: &mut StoreValidator,
    height: &BlockHeight,
    chunk_hashes: &HashSet<ChunkHash>,
) -> Result<(), StoreValidatorError> {
    for chunk_hash in chunk_hashes {
        let shard_chunk = unwrap_or_err_db!(
            sv.store.get_ser::<ShardChunk>(ColChunks, chunk_hash.as_ref()),
            "Can't get Chunk from storage with ChunkHash {:?}",
            chunk_hash
        );
        check_discrepancy!(
            shard_chunk.height_created(),
            *height,
            "Invalid ShardChunk {:?} stored",
            shard_chunk
        );
    }
    Ok(())
}

pub(crate) fn header_hash_of_height_exists(
    sv: &mut StoreValidator,
    height: &BlockHeight,
    header_hashes: &HashSet<CryptoHash>,
) -> Result<(), StoreValidatorError> {
    for hash in header_hashes {
        let header = unwrap_or_err_db!(
            sv.store.get_ser::<BlockHeader>(ColBlockHeader, hash.as_ref()),
            "Can't get Header from storage with Hash {:?}",
            hash
        );
        check_discrepancy!(header.height(), *height, "Invalid Header {:?} stored", header);
    }
    Ok(())
}

pub(crate) fn outcome_by_outcome_id_exists(
    sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    outcome_ids: &Vec<CryptoHash>,
) -> Result<(), StoreValidatorError> {
    for outcome_id in outcome_ids {
        let outcomes = unwrap_or_err_db!(
            sv.store.get_ser::<Vec<ExecutionOutcomeWithIdAndProof>>(
                ColTransactionResult,
                outcome_id.as_ref()
            ),
            "Can't get TransactionResult from storage with Outcome id {:?}",
            outcome_id
        );
        if outcomes.iter().find(|outcome| &outcome.block_hash == block_hash).is_none() {
            panic!("Invalid TransactionResult {:?} stored", outcomes);
        }
    }
    Ok(())
}

pub(crate) fn outcome_id_block_exists(
    sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    _outcome_ids: &Vec<CryptoHash>,
) -> Result<(), StoreValidatorError> {
    unwrap_or_err_db!(
        sv.store.get_ser::<Block>(ColBlock, block_hash.as_ref()),
        "Can't get Block from DB"
    );
    Ok(())
}

pub(crate) fn outcome_indexed_by_block_hash(
    sv: &mut StoreValidator,
    outcome_id: &CryptoHash,
    outcomes: &Vec<ExecutionOutcomeWithIdAndProof>,
) -> Result<(), StoreValidatorError> {
    for outcome in outcomes {
        let block = unwrap_or_err_db!(
            sv.store.get_ser::<Block>(ColBlock, outcome.block_hash.as_ref()),
            "Can't get Block {} from DB",
            outcome.block_hash
        );
        let mut outcome_ids = vec![];
        for chunk_header in block.chunks().iter() {
            if chunk_header.height_included() == block.header().height() {
                if let Some(me) = &sv.me {
                    if sv.runtime_adapter.cares_about_shard(
                        Some(&me),
                        block.header().prev_hash(),
                        chunk_header.shard_id(),
                        true,
                    ) || sv.runtime_adapter.will_care_about_shard(
                        Some(&me),
                        block.header().prev_hash(),
                        chunk_header.shard_id(),
                        true,
                    ) {
                        outcome_ids.extend(unwrap_or_err_db!(
                            sv.store.get_ser::<Vec<CryptoHash>>(
                                ColOutcomeIds,
                                &get_block_shard_id(block.hash(), chunk_header.shard_id())
                            ),
                            "Can't get Outcome ids by Block Hash"
                        ));
                    }
                }
            }
        }
        if !outcome_ids.contains(outcome_id) {
            println!("outcome ids: {:?}, block: {:?}", outcome_ids, block);
            err!("Outcome id {:?} is not found in ColOutcomeIds", outcome_id);
        }
    }
    Ok(())
}

pub(crate) fn state_sync_info_valid(
    _sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    state_sync_info: &StateSyncInfo,
) -> Result<(), StoreValidatorError> {
    check_discrepancy!(
        state_sync_info.epoch_tail_hash,
        *block_hash,
        "Invalid StateSyncInfo stored"
    );
    Ok(())
}

pub(crate) fn state_sync_info_block_exists(
    sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    _state_sync_info: &StateSyncInfo,
) -> Result<(), StoreValidatorError> {
    unwrap_or_err_db!(
        sv.store.get_ser::<Block>(ColBlock, block_hash.as_ref()),
        "Can't get Block from DB"
    );
    Ok(())
}

pub(crate) fn chunk_extra_block_exists(
    sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    _chunk_extra: &ChunkExtra,
) -> Result<(), StoreValidatorError> {
    unwrap_or_err_db!(
        sv.store.get_ser::<Block>(ColBlock, block_hash.as_ref()),
        "Can't get Block from DB"
    );
    Ok(())
}

pub(crate) fn block_info_block_header_exists(
    sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    _block_info: &BlockInfo,
) -> Result<(), StoreValidatorError> {
    // fake block info for pre-genesis block
    if *block_hash == CryptoHash::default() {
        return Ok(());
    }
    unwrap_or_err_db!(
        sv.store.get_ser::<BlockHeader>(ColBlockHeader, block_hash.as_ref()),
        "Can't get Block Header from DB"
    );
    Ok(())
}

pub(crate) fn epoch_validity(
    sv: &mut StoreValidator,
    epoch_id: &EpochId,
    _epoch_info: &EpochInfo,
) -> Result<(), StoreValidatorError> {
    check_discrepancy!(sv.runtime_adapter.epoch_exists(epoch_id), true, "Invalid EpochInfo stored");
    Ok(())
}

pub(crate) fn last_block_chunk_included(
    sv: &mut StoreValidator,
    shard_id: &ShardId,
    block_hash: &CryptoHash,
) -> Result<(), StoreValidatorError> {
    let block = unwrap_or_err_db!(
        sv.store.get_ser::<Block>(ColBlock, block_hash.as_ref()),
        "Can't get Block from DB"
    );
    for chunk_header in block.chunks().iter() {
        if chunk_header.shard_id() == *shard_id {
            // TODO #2893: Some Chunks missing
            /*
            unwrap_or_err_db!(
                sv.store.get_ser::<ShardChunk>(ColChunks, chunk_header.chunk_hash().as_ref()),
                "Can't get Chunk {:?} from storage",
                chunk_header
            );
            */
            return Ok(());
        }
    }
    err!("ShardChunk is not included into Block {:?}", block)
}

pub(crate) fn gc_col_count(
    sv: &mut StoreValidator,
    col: &DBCol,
    count: &u64,
) -> Result<(), StoreValidatorError> {
    if SHOULD_COL_GC[*col as usize] {
        sv.inner.gc_col[*col as usize] = *count;
    } else {
        if *count > 0 {
            err!("DBCol is cleared by mistake")
        }
    }
    Ok(())
}

pub(crate) fn tx_refcount(
    sv: &mut StoreValidator,
    tx_hash: &CryptoHash,
    refcount: &u64,
) -> Result<(), StoreValidatorError> {
    let expected = sv.inner.tx_refcount.get(tx_hash).map(|&rc| rc).unwrap_or_default();
    if *refcount != expected {
        err!("Invalid tx refcount, expected {:?}, found {:?}", expected, refcount)
    } else {
        sv.inner.tx_refcount.remove(tx_hash);
        return Ok(());
    }
}

pub(crate) fn receipt_refcount(
    sv: &mut StoreValidator,
    receipt_id: &CryptoHash,
    refcount: &u64,
) -> Result<(), StoreValidatorError> {
    let expected = sv.inner.receipt_refcount.get(receipt_id).map(|&rc| rc).unwrap_or_default();
    if *refcount != expected {
        err!("Invalid receipt refcount, expected {:?}, found {:?}", expected, refcount)
    } else {
        sv.inner.receipt_refcount.remove(receipt_id);
        return Ok(());
    }
}

pub(crate) fn block_refcount(
    sv: &mut StoreValidator,
    block_hash: &CryptoHash,
    refcount: &u64,
) -> Result<(), StoreValidatorError> {
    if let Some(found) = sv.inner.block_refcount.get(block_hash) {
        if refcount != found {
            err!("Invalid Block Refcount, expected {:?}, found {:?}", refcount, found)
        } else {
            sv.inner.block_refcount.remove(block_hash);
            return Ok(());
        }
    }
    let header = unwrap_or_err_db!(
        sv.store.get_ser::<BlockHeader>(ColBlockHeader, block_hash.as_ref()),
        "Can't get Block Header from DB"
    );
    check_discrepancy!(
        header.height(),
        sv.config.genesis_height,
        "Unexpected Block Refcount found"
    );
    // This is Genesis Block
    check_discrepancy!(*refcount, 1, "Invalid Genesis Block Refcount {:?}", refcount);
    sv.inner.genesis_blocks.push(*block_hash);
    Ok(())
}

pub(crate) fn state_header_block_exists(
    sv: &mut StoreValidator,
    key: &StateHeaderKey,
    _header: &ShardStateSyncResponseHeader,
) -> Result<(), StoreValidatorError> {
    unwrap_or_err_db!(
        sv.store.get_ser::<Block>(ColBlock, key.1.as_ref()),
        "Can't get Block from DB"
    );
    Ok(())
}

pub(crate) fn state_part_header_exists(
    sv: &mut StoreValidator,
    key: &StatePartKey,
    _part: &Vec<u8>,
) -> Result<(), StoreValidatorError> {
    let StatePartKey(block_hash, shard_id, part_id) = *key;
    let state_header_key = unwrap_or_err!(
        StateHeaderKey(shard_id, block_hash).try_to_vec(),
        "Can't serialize StateHeaderKey"
    );
    let header = unwrap_or_err_db!(
        sv.store.get_ser::<ShardStateSyncResponseHeader>(ColStateHeaders, &state_header_key),
        "Can't get StateHeaderKey from DB"
    );
    let num_parts = get_num_state_parts(header.state_root_node().memory_usage);
    if part_id >= num_parts {
        err!("Invalid part_id {:?}, num_parts {:?}", part_id, num_parts)
    }
    Ok(())
}

// Final checks

pub(crate) fn block_height_cmp_tail_final(
    sv: &mut StoreValidator,
) -> Result<(), StoreValidatorError> {
    if sv.inner.block_heights_less_tail.len() >= 2 {
        let len = sv.inner.block_heights_less_tail.len();
        let blocks = &sv.inner.block_heights_less_tail;
        err!("Found {:?} Blocks with height lower than Tail, {:?}", len, blocks)
    }
    Ok(())
}

pub(crate) fn gc_col_count_final(sv: &mut StoreValidator) -> Result<(), StoreValidatorError> {
    let mut zeroes = 0;
    for count in sv.inner.gc_col.iter() {
        if *count == 0 {
            zeroes += 1;
        }
    }
    // 1. All zeroes case is acceptable
    if zeroes == NUM_COLS {
        return Ok(());
    }
    let mut gc_col_count = 0;
    for gc_col in SHOULD_COL_GC.iter() {
        if *gc_col == true {
            gc_col_count += 1;
        }
    }
    // 2. All columns are GCed case is acceptable
    if zeroes == NUM_COLS - gc_col_count {
        return Ok(());
    }
    // TODO #2861 build a graph of dependencies or make it better in another way
    err!("Suspicious, look into GC values manually")
}

pub(crate) fn tx_refcount_final(sv: &mut StoreValidator) -> Result<(), StoreValidatorError> {
    let len = sv.inner.tx_refcount.len();
    if len > 0 {
        for tx_refcount in sv.inner.tx_refcount.iter() {
            err!("Found {:?} Txs that are not counted, i.e. {:?}", len, tx_refcount);
        }
    }
    Ok(())
}

pub(crate) fn receipt_refcount_final(sv: &mut StoreValidator) -> Result<(), StoreValidatorError> {
    let len = sv.inner.receipt_refcount.len();
    if len > 0 {
        for receipt_refcount in sv.inner.receipt_refcount.iter() {
            err!("Found {:?} receipts that are not counted, i.e. {:?}", len, receipt_refcount);
        }
    }
    Ok(())
}

pub(crate) fn block_refcount_final(sv: &mut StoreValidator) -> Result<(), StoreValidatorError> {
    if sv.inner.block_refcount.len() > 1 {
        let len = sv.inner.block_refcount.len();
        for block_refcount in sv.inner.block_refcount.iter() {
            err!("Found {:?} Blocks that are not counted, i.e. {:?}", len, block_refcount);
        }
    }
    if sv.inner.genesis_blocks.len() > 1 {
        let len = sv.inner.genesis_blocks.len();
        for tail_block in sv.inner.genesis_blocks.iter() {
            err!("Found {:?} Genesis Blocks, i.e. {:?}", len, tail_block);
        }
    }
    Ok(())
}
