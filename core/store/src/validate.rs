use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::Arc;

use borsh::BorshDeserialize;

use near_chain_configs::GenesisConfig;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunk};
use near_primitives::types::{BlockHeight, EpochId};
use near_primitives::utils::index_to_bytes;

#[allow(unused)]
use crate::{
    read_with_cache, ColBlock, ColBlockExtra, ColBlockHeader, ColBlockHeight, ColBlockMisc,
    ColBlockPerHeight, ColBlockRefCount, ColBlocksToCatchup, ColChallengedBlocks, ColChunkExtra,
    ColChunkPerHeightShard, ColChunks, ColEpochLightClientBlocks, ColIncomingReceipts,
    ColInvalidChunks, ColLastBlockWithNewChunk, ColNextBlockHashes, ColNextBlockWithNewChunk,
    ColOutgoingReceipts, ColPartialChunks, ColReceiptIdToShardId, ColState, ColStateChanges,
    ColStateDlInfos, ColStateHeaders, ColTransactionResult, ColTransactions, ColTrieChanges, DBCol,
    KeyForStateChanges, ShardTries, Store, StoreUpdate, Trie, TrieChanges, TrieIterator,
    WrappedTrieChanges, TAIL_KEY,
};

macro_rules! get_parent_function_name {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        &name[..name.len() - 3]
    }};
}

macro_rules! err(($x:expr) => (Err(format!("{}: {}", get_parent_function_name!(), $x))));

macro_rules! unwrap_or_err {
    ($obj: expr, $ret: expr) => {
        match $obj {
            Ok(value) => value,
            Err(err) => {
                return err!(err);
            }
        }
    };
}

// All validations start here
//

fn nothing(_sv: &StoreValidator, _key: &[u8], _value: &[u8]) -> Result<(), String> {
    // Make sure that validation is executed
    Ok(())
}

fn block_header_validity(_sv: &StoreValidator, key: &[u8], value: &[u8]) -> Result<(), String> {
    let block_hash = CryptoHash::try_from(key.as_ref()).unwrap();
    match BlockHeader::try_from_slice(value) {
        Ok(header) => {
            if header.hash() != block_hash {
                err!(format!("Invalid Block Header hash stored, {:?}", block_hash))
            } else {
                Ok(())
            }
        }
        Err(e) => err!(format!("Can't get Block Header from storage, {:?}, {:?}", block_hash, e)),
    }
}

fn block_hash_validity(_sv: &StoreValidator, key: &[u8], value: &[u8]) -> Result<(), String> {
    let block_hash = CryptoHash::try_from(key.as_ref()).unwrap();
    match Block::try_from_slice(value) {
        Ok(block) => {
            if block.hash() != block_hash {
                err!(format!("Invalid Block hash stored, {:?}", block_hash))
            } else {
                Ok(())
            }
        }
        Err(e) => err!(format!("Can't get Block Header from storage, {:?}, {:?}", block_hash, e)),
    }
}

fn block_header_exists(sv: &StoreValidator, key: &[u8], _value: &[u8]) -> Result<(), String> {
    let block_hash = CryptoHash::try_from(key.as_ref()).unwrap();
    match sv.store.get_ser::<BlockHeader>(ColBlockHeader, block_hash.as_ref()) {
        Ok(Some(_header)) => Ok(()),
        Ok(None) => err!(format!("Block Header not found, {:?}", block_hash)),
        Err(e) => err!(format!("Can't get Block Header from storage, {:?}, {:?}", block_hash, e)),
    }
}

fn chunk_hash_validity(_sv: &StoreValidator, key: &[u8], value: &[u8]) -> Result<(), String> {
    let chunk_hash = ChunkHash::try_from_slice(key.as_ref()).unwrap();
    match ShardChunk::try_from_slice(value) {
        Ok(shard_chunk) => {
            if shard_chunk.chunk_hash != chunk_hash {
                err!(format!("Invalid ShardChunk hash stored, {:?}", chunk_hash))
            } else {
                Ok(())
            }
        }
        Err(e) => err!(format!("Can't get ShardChunk from storage, {:?}, {:?}", chunk_hash, e)),
    }
}

fn block_of_chunk_exists(sv: &StoreValidator, _key: &[u8], value: &[u8]) -> Result<(), String> {
    let shard_chunk =
        unwrap_or_err!(ShardChunk::try_from_slice(value), "Can't deserialize ShardChunk");
    let height = shard_chunk.header.height_included;
    if height == sv.config.genesis_height {
        return Ok(());
    }
    match sv.store.get_ser::<HashMap<EpochId, HashSet<CryptoHash>>>(
        ColBlockPerHeight,
        &index_to_bytes(height),
    ) {
        Ok(Some(map)) => {
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
            err!(format!("No Block on height {:?} accepts ShardChunk {:?}", height, shard_chunk))
        }
        Ok(None) => err!(format!("Map not found on height {:?}, no one is responsible for ShardChunk {:?}", height, shard_chunk)),
        Err(e) => err!(format!("Can't get Map from storage on height {:?}, no one is responsible for ShardChunk {:?}, {:?}", height, shard_chunk, e)),
    }
}

fn block_height_cmp_tail(sv: &StoreValidator, key: &[u8], value: &[u8]) -> Result<(), String> {
    let block_hash = CryptoHash::try_from(key.as_ref()).unwrap();
    let tail = match sv.store.get_ser::<BlockHeight>(ColBlockMisc, TAIL_KEY) {
        Ok(Some(tail)) => tail,
        Ok(None) => sv.config.genesis_height,
        Err(_) => return err!("Can't get Tail from storage"),
    };
    match Block::try_from_slice(value) {
        Ok(block) => {
            if block.header.inner_lite.height < tail
                && block.header.inner_lite.height != sv.config.genesis_height
            {
                err!(format!(
                    "Invalid block height stored: {:?}, tail: {:?}",
                    block.header.inner_lite.height, tail
                ))
            } else {
                Ok(())
            }
        }
        Err(e) => {
            err!(format!("Can't get Block from storage, {:?}, {:?}", block_hash.to_string(), e))
        }
    }
}

fn chunks_state_roots_in_trie(
    sv: &StoreValidator,
    _key: &[u8],
    value: &[u8],
) -> Result<(), String> {
    let shard_chunk: ShardChunk =
        unwrap_or_err!(ShardChunk::try_from_slice(value), "Can't deserialize ShardChunk");
    let shard_id = shard_chunk.header.inner.shard_id;
    let state_root = shard_chunk.header.inner.prev_state_root;
    let trie = sv.shard_tries.get_trie_for_shard(shard_id);
    let trie = TrieIterator::new(&trie, &state_root).unwrap();
    for item in trie {
        unwrap_or_err!(item, format!("Can't find ShardChunk {} in Trie", shard_chunk));
    }
    Ok(())
}

//
// All validations end here

#[derive(Debug)]
pub struct ErrorMessage {
    pub col: DBCol,
    pub msg: String,
}

impl ErrorMessage {
    fn new(col: DBCol, msg: String) -> Self {
        Self { col, msg }
    }
}

pub struct StoreValidator {
    config: GenesisConfig,
    shard_tries: ShardTries,
    store: Arc<Store>,

    pub errors: Vec<ErrorMessage>,
    tests: u64,
}

impl StoreValidator {
    pub fn new(config: GenesisConfig, shard_tries: ShardTries, store: Arc<Store>) -> Self {
        StoreValidator {
            config,
            shard_tries: shard_tries.clone(),
            store: store.clone(),
            errors: vec![],
            tests: 0,
        }
    }
    pub fn is_failed(&self) -> bool {
        self.tests == 0 || self.errors.len() > 0
    }
    pub fn num_failed(&self) -> u64 {
        self.errors.len() as u64
    }
    pub fn tests_done(&self) -> u64 {
        self.tests
    }
    pub fn validate(&mut self) {
        self.check(&nothing, &[0], &[0], ColBlockMisc);
        for (key, value) in self.store.clone().iter(ColBlockHeader) {
            // Block Header Hash is valid
            self.check(&block_header_validity, &key, &value, ColBlockHeader);
        }
        for (key, value) in self.store.clone().iter(ColBlock) {
            // Block Hash is valid
            self.check(&block_hash_validity, &key, &value, ColBlock);
            // Block Header for current Block exists
            self.check(&block_header_exists, &key, &value, ColBlock);
            // Block Height is greater or equal to tail, or to Genesis Height
            self.check(&block_height_cmp_tail, &key, &value, ColBlock);
        }
        for (key, value) in self.store.clone().iter(ColChunks) {
            // Chunk Hash is valid
            self.check(&chunk_hash_validity, &key, &value, ColChunks);
            // Block for current Chunk exists
            self.check(&block_of_chunk_exists, &key, &value, ColChunks);
            // There is a State Root in the Trie
            self.check(&chunks_state_roots_in_trie, &key, &value, ColChunks);
        }
        for shard_id in 0..self.shard_tries.tries.len() {
            println!("{}", shard_id);
            // TODO ??
        }
    }

    fn check(
        &mut self,
        f: &dyn Fn(&StoreValidator, &[u8], &[u8]) -> Result<(), String>,
        key: &[u8],
        value: &[u8],
        col: DBCol,
    ) {
        let result = f(self, key, value);
        self.tests += 1;
        match result {
            Ok(_) => {}
            Err(msg) => self.errors.push(ErrorMessage::new(col, msg)),
        }
    }
}
