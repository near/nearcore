//! Several specializations of the storage over the general-purpose key-value storage used by the
//! generic BlockChain and by specific BeaconChain/ShardChain.
use crate::Storage;
use primitives::hash::CryptoHash;
use primitives::traits::{Decode, Encode};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

pub mod beacon;

type StorageResult<T> = io::Result<Option<T>>;

/// Uniquely identifies the chain.
#[derive(Clone)]
pub enum ChainId {
    BeaconChain,
    ShardChain(u32),
}

impl Into<u32> for ChainId {
    fn into(self) -> u32 {
        match self {
            ChainId::BeaconChain => 0u32,
            ChainId::ShardChain(i) => i + 1,
        }
    }
}

impl From<u32> for ChainId {
    fn from(id: u32) -> Self {
        if id == 0 {
            ChainId::BeaconChain
        } else {
            ChainId::ShardChain(id - 1)
        }
    }
}

/// Column that stores the mapping: genesis_hash -> best_block_hash.
const COL_BEST_BLOCK: u32 = 0;
const COL_HEADERS: u32 = 1;
const COL_BLOCKS: u32 = 2;
const COL_BLOCK_INDICES: u32 = 3;

pub struct BlockChainStorage<SignedHeader, SignedBlock> {
    chain_id: ChainId,
    storage: Arc<Storage>,
    genesis_hash: CryptoHash,

    best_block_hash: HashMap<Vec<u8>, CryptoHash>,
    headers: HashMap<Vec<u8>, SignedHeader>,
    blocks: HashMap<Vec<u8>, SignedBlock>,
    block_indices: HashMap<Vec<u8>, CryptoHash>,
}

/// Specific block chain storages like beacon chain storage and shard chain storage should implement
/// this trait to allow them to be used in specific beacon chain and shard chain. Rust way of doing
/// polymorphism.
pub trait StorageContainer {
    /// Returns reference to the internal generic BlockChain storage.
    fn blockchain_storage_mut<SignedHeader, SignedBlock>(
        &mut self,
    ) -> &mut BlockChainStorage<SignedHeader, SignedBlock>;
}

impl<SignedHeader, SignedBlock> BlockChainStorage<SignedHeader, SignedBlock>
where
    SignedHeader: Clone + Encode + Decode,
    SignedBlock: Clone + Encode + Decode,
{
    /// Converts the relative index of the column to the absolute. Absolute indices of columns of
    /// different `BlockChainStorage`'s do not overlap.
    fn abs_col(&self, col: u32) -> u32 {
        // We use pairing function to map chain id and column id to integer in such way that if we
        // later add more columns or chains (shards) the old storage be compatible with the new
        // code -- it will map to the same columns and chains in the new code.
        let id: u32 = self.chain_id.clone().into();
        (id + col) * (id + col + 1) / 2 + col
    }

    pub fn new(storage: Arc<Storage>, chain_id: ChainId, genesis_hash: CryptoHash) -> Self {
        Self {
            storage,
            chain_id,
            genesis_hash,
            best_block_hash: Default::default(),
            headers: Default::default(),
            blocks: Default::default(),
            block_indices: Default::default(),
        }
    }

    #[inline]
    pub fn best_block_hash(&mut self) -> StorageResult<&CryptoHash> {
        read_with_cache(
            self.storage.as_ref(),
            self.abs_col(COL_BEST_BLOCK),
            &mut self.best_block_hash,
            self.genesis_hash.as_ref(),
        )
    }

    #[inline]
    pub fn set_best_block_hash(&mut self, value: CryptoHash) -> io::Result<()> {
        write_with_cache(
            self.storage.as_ref(),
            self.abs_col(COL_BEST_BLOCK),
            &mut self.best_block_hash,
            self.genesis_hash.as_ref(),
            value,
        )
    }

    #[inline]
    pub fn header(&mut self, hash: &CryptoHash) -> StorageResult<&SignedHeader> {
        read_with_cache(
            self.storage.as_ref(),
            self.abs_col(COL_HEADERS),
            &mut self.headers,
            hash.as_ref(),
        )
    }

    #[inline]
    pub fn set_header(&mut self, hash: &CryptoHash, header: SignedHeader) -> io::Result<()> {
        write_with_cache(
            self.storage.as_ref(),
            self.abs_col(COL_HEADERS),
            &mut self.headers,
            hash.as_ref(),
            header,
        )
    }

    #[inline]
    pub fn block(&mut self, hash: &CryptoHash) -> StorageResult<&SignedBlock> {
        read_with_cache(
            self.storage.as_ref(),
            self.abs_col(COL_BLOCKS),
            &mut self.blocks,
            hash.as_ref(),
        )
    }

    #[inline]
    pub fn set_block(&mut self, hash: &CryptoHash, block: SignedBlock) -> io::Result<()> {
        write_with_cache(
            self.storage.as_ref(),
            self.abs_col(COL_BLOCKS),
            &mut self.blocks,
            hash.as_ref(),
            block,
        )
    }

    #[inline]
    pub fn hash_by_index(&mut self, index: u64) -> StorageResult<&CryptoHash> {
        read_with_cache(
            self.storage.as_ref(),
            self.abs_col(COL_BLOCK_INDICES),
            &mut self.block_indices,
            index_to_bytes(&index),
        )
    }

    #[inline]
    pub fn set_hash_by_index(&mut self, index: u64, hash: CryptoHash) -> io::Result<()> {
        write_with_cache(
            self.storage.as_ref(),
            self.abs_col(COL_BLOCK_INDICES),
            &mut self.block_indices,
            index_to_bytes(&index),
            hash,
        )
    }
}

/// Provides a view on the bytes that constitute the u64 index.
fn index_to_bytes(index: &u64) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(
            index as *const u64 as *const u8,
            std::mem::size_of::<u64>() / std::mem::size_of::<u8>(),
        )
    }
}

fn write_with_cache<T: Clone + Encode>(
    storage: &Storage,
    col: u32,
    cache: &mut HashMap<Vec<u8>, T>,
    key: &[u8],
    value: T,
) -> io::Result<()> {
    let data = Encode::encode(&value)?;
    let mut db_transaction = storage.transaction();
    db_transaction.put(Some(col), key, &data);
    storage.write(db_transaction)?;
    // If it has reached here then it is safe to put in cache.
    cache.insert(key.to_vec(), value);
    Ok(())
}

#[allow(dead_code)]
fn extend_with_cache<T: Clone + Encode>(
    storage: &Storage,
    col: u32,
    cache: &mut HashMap<Vec<u8>, T>,
    values: HashMap<Vec<u8>, T>,
) -> io::Result<()> {
    let mut db_transaction = storage.transaction();
    let mut cache_to_extend = vec![];
    for (key, value) in values {
        let data = Encode::encode(&value)?;
        cache_to_extend.push((key.clone(), value));
        db_transaction.put(Some(col), &key, &data);
    }
    storage.write(db_transaction)?;
    // If it has reached here then it is safe to put in cache.
    cache.extend(cache_to_extend);
    Ok(())
}

fn read_with_cache<'a, T: Decode + 'a>(
    storage: &Storage,
    col: u32,
    cache: &'a mut HashMap<Vec<u8>, T>,
    key: &[u8],
) -> StorageResult<&'a T> {
    match cache.entry(key.to_vec()) {
        Entry::Occupied(entry) => Ok(Some(entry.into_mut())),
        Entry::Vacant(entry) => {
            if let Some(data) = storage.get(Some(col), key)? {
                let result = Decode::decode(data.as_ref())?;
                Ok(Some(entry.insert(result)))
            } else {
                Ok(None)
            }
        }
    }
}
