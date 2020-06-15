use std::cmp;
use std::collections::HashMap;
use std::io;
use std::sync::RwLock;

use rocksdb::{
    BlockBasedOptions, ColumnFamily, ColumnFamilyDescriptor, Direction, IteratorMode, Options,
    ReadOptions, WriteBatch, DB,
};
use strum_macros::EnumIter;

use near_primitives::version::DbVersion;

#[derive(Debug, Clone, PartialEq)]
pub struct DBError(rocksdb::Error);

impl std::fmt::Display for DBError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for DBError {}

impl From<rocksdb::Error> for DBError {
    fn from(err: rocksdb::Error) -> Self {
        DBError(err)
    }
}

impl Into<io::Error> for DBError {
    fn into(self) -> io::Error {
        io::Error::new(io::ErrorKind::Other, self)
    }
}

#[derive(PartialEq, Debug, Copy, Clone, EnumIter)]
pub enum DBCol {
    /// Column to indicate which version of database this is.
    ColDbVersion = 0,
    ColBlockMisc = 1,
    ColBlock = 2,
    ColBlockHeader = 3,
    ColBlockHeight = 4,
    ColState = 5,
    ColChunkExtra = 6,
    ColTransactionResult = 7,
    ColOutgoingReceipts = 8,
    ColIncomingReceipts = 9,
    ColPeers = 10,
    ColEpochInfo = 11,
    ColBlockInfo = 12,
    ColChunks = 13,
    ColPartialChunks = 14,
    /// Blocks for which chunks need to be applied after the state is downloaded for a particular epoch
    ColBlocksToCatchup = 15,
    /// Blocks for which the state is being downloaded
    ColStateDlInfos = 16,
    ColChallengedBlocks = 17,
    ColStateHeaders = 18,
    ColInvalidChunks = 19,
    ColBlockExtra = 20,
    /// Store hash of a block per each height, to detect double signs.
    ColBlockPerHeight = 21,
    ColStateParts = 22,
    ColEpochStart = 23,
    /// Map account_id to announce_account
    ColAccountAnnouncements = 24,
    /// Next block hashes in the sequence of the canonical chain blocks
    ColNextBlockHashes = 25,
    /// `LightClientBlock`s corresponding to the last final block of each completed epoch
    ColEpochLightClientBlocks = 26,
    ColReceiptIdToShardId = 27,
    ColNextBlockWithNewChunk = 28,
    ColLastBlockWithNewChunk = 29,
    /// Map each saved peer on disk with its component id.
    ColPeerComponent = 30,
    /// Map component id with all edges in this component.
    ColComponentEdges = 31,
    /// Biggest nonce used.
    LastComponentNonce = 32,
    /// Transactions
    ColTransactions = 33,
    ColChunkPerHeightShard = 34,
    /// Changes to key-values that we have recorded.
    ColStateChanges = 35,
    ColBlockRefCount = 36,
    ColTrieChanges = 37,
    /// Merkle tree of block hashes
    ColBlockMerkleTree = 38,
    ColChunkHashesByHeight = 39,
    /// Block ordinals.
    ColBlockOrdinal = 40,
}

// Do not move this line from enum DBCol
const NUM_COLS: usize = 41;

impl std::fmt::Display for DBCol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let desc = match self {
            Self::ColDbVersion => "db version",
            Self::ColBlockMisc => "miscellaneous block data",
            Self::ColBlock => "block data",
            Self::ColBlockHeader => "block header data",
            Self::ColBlockHeight => "block height",
            Self::ColState => "blockchain state",
            Self::ColChunkExtra => "extra information of trunk",
            Self::ColTransactionResult => "transaction results",
            Self::ColOutgoingReceipts => "outgoing receipts",
            Self::ColIncomingReceipts => "incoming receipts",
            Self::ColPeers => "peer information",
            Self::ColEpochInfo => "epoch information",
            Self::ColBlockInfo => "block information",
            Self::ColChunks => "chunks",
            Self::ColPartialChunks => "partial chunks",
            Self::ColBlocksToCatchup => "blocks need to apply chunks",
            Self::ColStateDlInfos => "blocks downloading",
            Self::ColChallengedBlocks => "challenged blocks",
            Self::ColStateHeaders => "state headers",
            Self::ColInvalidChunks => "invalid chunks",
            Self::ColBlockExtra => "extra block information",
            Self::ColBlockPerHeight => "hash of block per height",
            Self::ColStateParts => "state parts",
            Self::ColEpochStart => "epoch start",
            Self::ColAccountAnnouncements => "account announcements",
            Self::ColNextBlockHashes => "next block hash",
            Self::ColEpochLightClientBlocks => "epoch light client block",
            Self::ColReceiptIdToShardId => "receipt id to shard id",
            Self::ColNextBlockWithNewChunk => "next block with new chunk",
            Self::ColLastBlockWithNewChunk => "last block with new chunk",
            Self::ColPeerComponent => "peer components",
            Self::ColComponentEdges => "component edges",
            Self::LastComponentNonce => "last component nonce",
            Self::ColTransactions => "transactions",
            Self::ColChunkPerHeightShard => "hash of chunk per height and shard_id",
            Self::ColStateChanges => "key value changes",
            Self::ColBlockRefCount => "refcount per block",
            Self::ColTrieChanges => "trie changes",
            Self::ColBlockMerkleTree => "block merkle tree",
            Self::ColChunkHashesByHeight => "chunk hashes indexed by height_created",
            Self::ColBlockOrdinal => "block ordinal",
        };
        write!(formatter, "{}", desc)
    }
}

pub const HEAD_KEY: &[u8; 4] = b"HEAD";
pub const TAIL_KEY: &[u8; 4] = b"TAIL";
pub const CHUNK_TAIL_KEY: &[u8; 10] = b"CHUNK_TAIL";
pub const SYNC_HEAD_KEY: &[u8; 9] = b"SYNC_HEAD";
pub const HEADER_HEAD_KEY: &[u8; 11] = b"HEADER_HEAD";
pub const LATEST_KNOWN_KEY: &[u8; 12] = b"LATEST_KNOWN";
pub const LARGEST_TARGET_HEIGHT_KEY: &[u8; 21] = b"LARGEST_TARGET_HEIGHT";
pub const VERSION_KEY: &[u8; 7] = b"VERSION";

pub struct DBTransaction {
    pub ops: Vec<DBOp>,
}

pub enum DBOp {
    Insert { col: DBCol, key: Vec<u8>, value: Vec<u8> },
    Delete { col: DBCol, key: Vec<u8> },
}

impl DBTransaction {
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, col: DBCol, key: K, value: V) {
        self.ops.push(DBOp::Insert {
            col,
            key: key.as_ref().to_owned(),
            value: value.as_ref().to_owned(),
        });
    }

    pub fn delete<K: AsRef<[u8]>>(&mut self, col: DBCol, key: K) {
        self.ops.push(DBOp::Delete { col, key: key.as_ref().to_owned() });
    }
}

pub struct RocksDB {
    db: DB,
    cfs: Vec<*const ColumnFamily>,
}

// DB was already Send+Sync. cf and read_options are const pointers using only functions in
// this file and safe to share across threads.
unsafe impl Send for RocksDB {}
unsafe impl Sync for RocksDB {}

pub struct TestDB {
    db: RwLock<Vec<HashMap<Vec<u8>, Vec<u8>>>>,
}

pub trait Database: Sync + Send {
    fn transaction(&self) -> DBTransaction {
        DBTransaction { ops: Vec::new() }
    }
    fn get(&self, col: DBCol, key: &[u8]) -> Result<Option<Vec<u8>>, DBError>;
    fn iter<'a>(&'a self, column: DBCol) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a>;
    fn iter_prefix<'a>(
        &'a self,
        col: DBCol,
        key_prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a>;
    fn write(&self, batch: DBTransaction) -> Result<(), DBError>;
}

impl Database for RocksDB {
    fn get(&self, col: DBCol, key: &[u8]) -> Result<Option<Vec<u8>>, DBError> {
        let read_options = rocksdb_read_options();
        unsafe { Ok(self.db.get_cf_opt(&*self.cfs[col as usize], key, &read_options)?) }
    }

    fn iter<'a>(&'a self, col: DBCol) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        let read_options = rocksdb_read_options();
        unsafe {
            let cf_handle = &*self.cfs[col as usize];
            let iterator = self.db.iterator_cf_opt(cf_handle, read_options, IteratorMode::Start);
            Box::new(iterator)
        }
    }

    fn iter_prefix<'a>(
        &'a self,
        col: DBCol,
        key_prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        // NOTE: There is no Clone implementation for ReadOptions, so we cannot really reuse
        // `self.read_options` here.
        let mut read_options = rocksdb_read_options();
        read_options.set_prefix_same_as_start(true);
        unsafe {
            let cf_handle = &*self.cfs[col as usize];
            // This implementation is copied from RocksDB implementation of `prefix_iterator_cf` since
            // there is no `prefix_iterator_cf_opt` method.
            let iterator = self
                .db
                .iterator_cf_opt(
                    cf_handle,
                    read_options,
                    IteratorMode::From(key_prefix, Direction::Forward),
                )
                .take_while(move |(key, _value)| key.starts_with(key_prefix));
            Box::new(iterator)
        }
    }

    fn write(&self, transaction: DBTransaction) -> Result<(), DBError> {
        let mut batch = WriteBatch::default();
        for op in transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => unsafe {
                    batch.put_cf(&*self.cfs[col as usize], key, value);
                },
                DBOp::Delete { col, key } => unsafe {
                    batch.delete_cf(&*self.cfs[col as usize], key);
                },
            }
        }
        Ok(self.db.write(batch)?)
    }
}

impl Database for TestDB {
    fn get(&self, col: DBCol, key: &[u8]) -> Result<Option<Vec<u8>>, DBError> {
        Ok(self.db.read().unwrap()[col as usize].get(key).cloned())
    }

    fn iter<'a>(&'a self, col: DBCol) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        let iterator = self.db.read().unwrap()[col as usize]
            .clone()
            .into_iter()
            .map(|(k, v)| (k.into_boxed_slice(), v.into_boxed_slice()));
        Box::new(iterator)
    }

    fn iter_prefix<'a>(
        &'a self,
        col: DBCol,
        key_prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        Box::new(self.iter(col).filter(move |(key, _value)| key.starts_with(key_prefix)))
    }

    fn write(&self, transaction: DBTransaction) -> Result<(), DBError> {
        let mut db = self.db.write().unwrap();
        for op in transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => db[col as usize].insert(key, value),
                DBOp::Delete { col, key } => db[col as usize].remove(&key),
            };
        }
        Ok(())
    }
}

fn rocksdb_read_options() -> ReadOptions {
    let mut read_options = ReadOptions::default();
    read_options.set_verify_checksums(false);
    read_options
}

/// DB level options
fn rocksdb_options() -> Options {
    let mut opts = Options::default();

    opts.create_missing_column_families(true);
    opts.create_if_missing(true);
    opts.set_use_fsync(false);
    opts.set_max_open_files(512);
    opts.set_keep_log_file_num(1);
    opts.set_bytes_per_sync(1048576);
    opts.set_write_buffer_size(1024 * 1024 * 512 / 2);
    opts.set_max_bytes_for_level_base(1024 * 1024 * 512 / 2);
    opts.increase_parallelism(cmp::max(1, num_cpus::get() as i32 / 2));
    opts.set_max_total_wal_size(1 * 1024 * 1024 * 1024);

    return opts;
}

fn rocksdb_block_based_options() -> BlockBasedOptions {
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_size(1024 * 16);
    let cache_size = 1024 * 1024 * 512 / 3;
    block_opts.set_lru_cache(cache_size);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_bloom_filter(10, true);
    block_opts
}

fn rocksdb_column_options() -> Options {
    let mut opts = Options::default();
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.set_block_based_table_factory(&rocksdb_block_based_options());
    opts.optimize_level_style_compaction(1024 * 1024 * 128);
    opts.set_target_file_size_base(1024 * 1024 * 64);
    opts.set_compression_per_level(&[]);
    opts
}

impl RocksDB {
    /// Returns version of the database state on disk.
    pub fn get_version<P: AsRef<std::path::Path>>(path: P) -> Result<DbVersion, DBError> {
        let db = RocksDB::new_read_only(path)?;
        db.get(DBCol::ColDbVersion, VERSION_KEY).map(|result| {
            serde_json::from_slice(
                &result
                    .expect("Failed to find version in first column. Database must be corrupted."),
            )
            .expect("Failed to parse version. Database must be corrupted.")
        })
    }

    fn new_read_only<P: AsRef<std::path::Path>>(path: P) -> Result<Self, DBError> {
        let options = Options::default();
        let cf_names: Vec<_> = vec!["col0".to_string()];
        let db = DB::open_cf_for_read_only(&options, path, cf_names.iter(), false)?;
        let cfs =
            cf_names.iter().map(|n| db.cf_handle(n).unwrap() as *const ColumnFamily).collect();
        Ok(Self { db, cfs })
    }

    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self, DBError> {
        let options = rocksdb_options();
        let cf_names: Vec<_> = (0..NUM_COLS).map(|col| format!("col{}", col)).collect();
        let cf_descriptors = cf_names
            .iter()
            .map(|cf_name| ColumnFamilyDescriptor::new(cf_name, rocksdb_column_options()));
        let db = DB::open_cf_descriptors(&options, path, cf_descriptors)?;
        let cfs =
            cf_names.iter().map(|n| db.cf_handle(n).unwrap() as *const ColumnFamily).collect();
        Ok(Self { db, cfs })
    }
}

impl TestDB {
    pub fn new() -> Self {
        let db: Vec<_> = (0..NUM_COLS).map(|_| HashMap::new()).collect();
        Self { db: RwLock::new(db) }
    }
}
