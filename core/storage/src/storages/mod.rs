//! Several specializations of the storage over the general-purpose key-value storage used by the
//! generic BlockChain and by specific BeaconChain/ShardChain.
use crate::KeyValueDB;
use lru::LruCache;
use primitives::block_traits::SignedBlock;
use primitives::block_traits::SignedHeader;
use primitives::hash::CryptoHash;
use primitives::serialize::{Decode, Encode};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

pub mod beacon;
pub mod shard;

type StorageResult<T> = io::Result<Option<T>>;

/// Uniquely identifies the chain.
#[derive(Clone)]
pub enum ChainId {
    BeaconChain,
    ShardChain(u32),
}

impl From<ChainId> for u32 {
    fn from(id: ChainId) -> u32 {
        match id {
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

// Columns that are used both by beacon and shard chain.
/// Column that stores the mapping: genesis hash -> best block hash.
const COL_BEST_BLOCK: u32 = 0;
/// Column that stores the mapping: header hash -> header.
const COL_HEADERS: u32 = 1;
/// Column that stores the mapping: block header hash -> block.
const COL_BLOCKS: u32 = 2;
/// Column that stores the indices of the current chain through the mapping: block index -> header
/// hash.
const COL_BLOCK_INDICES: u32 = 3;

// Columns that are used by the shard chain only.
const COL_STATE: u32 = 4;
const COL_TRANSACTION_RESULTS: u32 = 5;
const COL_TRANSACTION_ADDRESSES: u32 = 6;
const COL_RECEIPT_BLOCK: u32 = 7;
const COL_TX_NONCE: u32 = 8;

// Columns used by the beacon chain only.
const COL_PROPOSAL: u32 = 9;
const COL_PARTICIPATION: u32 = 10;
const COL_PROCESSED_BLOCKS: u32 = 11;
const COL_THRESHOLD: u32 = 12;
const COL_ACCEPTED_AUTHORITY: u32 = 13;

/// Number of columns per chain.
pub const NUM_COLS: u32 = 14;

/// Error that occurs when we try operating with genesis-specific columns, without setting the
/// genesis in advance.
const MISSING_GENESIS_ERR: &str = "Genesis is not set.";

/// lru cache size
const CACHE_SIZE: usize = 20;

pub struct BlockChainStorage<H, B> {
    chain_id: ChainId,
    storage: Arc<KeyValueDB>,
    genesis_hash: Option<CryptoHash>,
    // keyed by hash
    best_block_hash: LruCache<Vec<u8>, CryptoHash>,
    // keyed by hash
    headers: LruCache<Vec<u8>, H>,
    // keyed by hash
    blocks: LruCache<Vec<u8>, B>,
    // keyed by index
    block_indices: LruCache<Vec<u8>, CryptoHash>,
}

/// Specific block chain storages like beacon chain storage and shard chain storage should implement
/// this trait to allow them to be used in specific beacon chain and shard chain. Rust way of doing
/// polymorphism.
pub trait GenericStorage<H, B> {
    /// Returns reference to the internal generic BlockChain storage.
    fn blockchain_storage_mut(&mut self) -> &mut BlockChainStorage<H, B>;
}

impl<H, B> BlockChainStorage<H, B>
where
    H: SignedHeader,
    B: SignedBlock<SignedHeader = H>,
{
    /// Encodes a slice of bytes into a vector by adding a prefix that corresponds to the chain id.
    pub fn enc_slice(&self, slice: &[u8]) -> Vec<u8> {
        let id: u32 = self.chain_id.clone().into();
        let mut res = vec![];
        res.extend_from_slice(chain_id_to_bytes(&id));
        res.extend_from_slice(slice);
        res
    }

    /// Encodes hash by adding a prefix that corresponds to the chain id.
    pub fn enc_hash(&self, hash: &CryptoHash) -> [u8; 36] {
        let id: u32 = self.chain_id.clone().into();
        let mut res = [0; 36];
        res[..4].copy_from_slice(chain_id_to_bytes(&id));
        res[4..].copy_from_slice(hash.as_ref());
        res
    }

    /// Encodes block index by adding a prefix that corresponds to the chain id.
    fn enc_index(&self, index: u64) -> [u8; 12] {
        let id: u32 = self.chain_id.clone().into();
        let mut res = [0; 12];
        res[..4].copy_from_slice(chain_id_to_bytes(&id));
        res[4..].copy_from_slice(index_to_bytes(&index));
        res
    }

    pub fn new(storage: Arc<KeyValueDB>, chain_id: ChainId) -> Self {
        Self {
            storage,
            chain_id,
            genesis_hash: None,
            best_block_hash: LruCache::new(CACHE_SIZE),
            headers: LruCache::new(CACHE_SIZE),
            blocks: LruCache::new(CACHE_SIZE),
            block_indices: LruCache::new(CACHE_SIZE),
        }
    }

    pub fn genesis_hash(&self) -> &CryptoHash {
        self.genesis_hash.as_ref().expect(MISSING_GENESIS_ERR)
    }

    pub fn set_genesis(&mut self, genesis: B) -> io::Result<()> {
        // check whether we already have a genesis. If we do, then
        // the genesis must match the existing one in storage
        if let Some(genesis_hash) = self.genesis_hash {
            if genesis_hash != genesis.block_hash() {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid genesis"));
            }
        }
        self.genesis_hash = Some(genesis.block_hash());
        if self.block(&genesis.block_hash())?.is_none() {
            // Only add genesis block if it was not added before. It might have been added before
            // if we have launched on the existing storage.
            self.add_block(genesis)
        } else {
            Ok(())
        }
    }

    pub fn add_block(&mut self, block: B) -> io::Result<()> {
        self.set_best_block_hash(block.block_hash())?;
        self.set_hash_by_index(block.index(), block.block_hash())?;
        self.set_header(&block.block_hash(), block.header())?;
        self.set_block(&block.block_hash(), block)
    }

    pub fn add_header(&mut self, header: B::SignedHeader) -> io::Result<()> {
        self.set_best_block_hash(header.block_hash())?;
        self.set_hash_by_index(header.index(), header.block_hash())?;
        self.set_header(&header.block_hash(), header)
    }

    #[inline]
    pub fn best_block_hash(&mut self) -> StorageResult<&CryptoHash> {
        let key = self.enc_hash(self.genesis_hash.as_ref().expect(MISSING_GENESIS_ERR));
        read_with_cache(self.storage.as_ref(), COL_BEST_BLOCK, &mut self.best_block_hash, &key)
    }

    #[inline]
    pub fn set_best_block_hash(&mut self, value: CryptoHash) -> io::Result<()> {
        let key = self.enc_hash(self.genesis_hash.as_ref().expect(MISSING_GENESIS_ERR));
        write_with_cache(
            self.storage.as_ref(),
            COL_BEST_BLOCK,
            &mut self.best_block_hash,
            &key,
            value,
        )
    }

    #[inline]
    pub fn header(&mut self, hash: &CryptoHash) -> StorageResult<&H> {
        let key = self.enc_hash(hash);
        read_with_cache(self.storage.as_ref(), COL_HEADERS, &mut self.headers, &key)
    }

    #[inline]
    pub fn set_header(&mut self, hash: &CryptoHash, header: H) -> io::Result<()> {
        let key = self.enc_hash(hash);
        write_with_cache(self.storage.as_ref(), COL_HEADERS, &mut self.headers, &key, header)
    }

    #[inline]
    pub fn block(&mut self, hash: &CryptoHash) -> StorageResult<&B> {
        let key = self.enc_hash(hash);
        read_with_cache(self.storage.as_ref(), COL_BLOCKS, &mut self.blocks, &key)
    }

    #[inline]
    pub fn set_block(&mut self, hash: &CryptoHash, block: B) -> io::Result<()> {
        let key = self.enc_hash(hash);
        write_with_cache(self.storage.as_ref(), COL_BLOCKS, &mut self.blocks, &key, block)
    }

    #[inline]
    pub fn best_block(&mut self) -> StorageResult<&B> {
        let best_hash = *self.best_block_hash().unwrap().unwrap();
        self.block(&best_hash)
    }

    #[inline]
    pub fn hash_by_index(&mut self, index: u64) -> StorageResult<&CryptoHash> {
        // Check to make sure the requested index is not larger than the index of the best block.
        let best_block_index = match self.best_block_hash()?.cloned() {
            None => return Ok(None),
            Some(best_hash) => match self.block(&best_hash)? {
                None => return Ok(None),
                Some(block) => block.index(),
            },
        };
        if best_block_index < index {
            return Ok(None);
        }
        let key = self.enc_index(index);
        read_with_cache(self.storage.as_ref(), COL_BLOCK_INDICES, &mut self.block_indices, &key)
    }

    #[inline]
    pub fn set_hash_by_index(&mut self, index: u64, hash: CryptoHash) -> io::Result<()> {
        let key = self.enc_index(index);
        write_with_cache(
            self.storage.as_ref(),
            COL_BLOCK_INDICES,
            &mut self.block_indices,
            &key,
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

/// Provides a view on the bytes that constitute the chain id.
fn chain_id_to_bytes(index: &u32) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(
            index as *const u32 as *const u8,
            std::mem::size_of::<u32>() / std::mem::size_of::<u8>(),
        )
    }
}

fn write_with_cache<T: Clone + Encode>(
    storage: &KeyValueDB,
    col: u32,
    cache: &mut LruCache<Vec<u8>, T>,
    key: &[u8],
    value: T,
) -> io::Result<()> {
    let data = Encode::encode(&value)?;
    let mut db_transaction = storage.transaction();
    db_transaction.put(Some(col), key, &data);
    storage.write(db_transaction)?;
    // If it has reached here then it is safe to put in cache.
    cache.put(key.to_vec(), value);
    Ok(())
}

fn extend_with_cache<T: Clone + Encode>(
    storage: &KeyValueDB,
    col: u32,
    cache: &mut LruCache<Vec<u8>, T>,
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
    for (key, value) in cache_to_extend {
        cache.put(key, value);
    }
    Ok(())
}

fn read_with_cache<'a, T: Decode + Clone + 'a>(
    storage: &KeyValueDB,
    col: u32,
    cache: &'a mut LruCache<Vec<u8>, T>,
    key: &[u8],
) -> StorageResult<&'a T> {
    let key_vec = key.to_vec();
    if cache.contains(&key_vec) {
        let value = cache.get(&key_vec).unwrap();
        Ok(Some(value))
    } else if let Some(data) = storage.get(Some(col), key)? {
        let result = Decode::decode(data.as_ref())?;
        cache.put(key_vec.clone(), result);
        Ok(cache.get(&key_vec))
    } else {
        Ok(None)
    }
}

/// prune column based on index
fn prune_index<T>(
    storage: &KeyValueDB,
    col: u32,
    cache: &mut LruCache<Vec<u8>, T>,
    filter: &Fn(u64) -> bool,
) -> io::Result<()> {
    let get_u64_from_key = |k: &[u8]| {
        let mut buf: [u8; 8] = [0; 8];
        buf.copy_from_slice(&k[4..]);
        u64::from_le_bytes(buf)
    };
    let mut db_transaction = storage.transaction();
    for (k, _) in storage.iter(Some(col)) {
        let key = get_u64_from_key(&k);
        if !filter(key) {
            db_transaction.delete(Some(col), &k);
        }
    }
    storage.write(db_transaction)?;

    // LruCache does not have `retain`, so have to sort to this
    // https://github.com/jeromefroe/lru-rs/issues/31
    let keys: Vec<_> = cache.iter().map(|(k, _)| k.clone()).collect();
    for key in keys {
        if !filter(get_u64_from_key(&key)) {
            cache.pop(&key);
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_cache_read_and_write() {
        let db = kvdb_memorydb::create(NUM_COLS);
        let mut cache = LruCache::new(8);
        for i in 0..8 {
            write_with_cache(&db, 0, &mut cache, &[i as u8], i).unwrap();
        }
        for i in (8..12).rev() {
            write_with_cache(&db, 0, &mut cache, &[i as u8], i).unwrap();
        }
        let values: Vec<_> = cache.into_iter().map(|(_, v)| *v as u32).collect();
        assert_eq!(values, vec![8, 9, 10, 11, 7, 6, 5, 4]);
        for i in 0..12 {
            let result = read_with_cache(&db, 0, &mut cache, &[i as u8]);
            assert_eq!(*result.unwrap().unwrap(), i);
        }
        let values: Vec<u32> = cache.into_iter().map(|(_, v)| *v as u32).collect();
        assert_eq!(values, vec![11, 10, 9, 8, 7, 6, 5, 4]);
    }
}
