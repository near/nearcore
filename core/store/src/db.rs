#[cfg(not(feature = "single_thread_rocksdb"))]
use std::cmp;
use std::collections::HashMap;
use std::io;
use std::sync::RwLock;

use borsh::{BorshDeserialize, BorshSerialize};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, Direction, Env, IteratorMode,
    Options, ReadOptions, WriteBatch, DB,
};
use strum::EnumIter;
use tracing::warn;

use near_primitives::version::DbVersion;

use crate::db::refcount::merge_refcounted_records;

use std::path::Path;
use std::sync::atomic::Ordering;

pub(crate) mod refcount;
pub(crate) mod v6_to_v7;

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

/// This enum holds the information about the columns that we use within the RocksDB storage.
/// You can think about our storage as 2-dimensional table (with key and column as indexes/coordinates).
// TODO(mm-near): add info about the RC in the columns.
#[derive(PartialEq, Debug, Copy, Clone, EnumIter, BorshDeserialize, BorshSerialize, Hash, Eq)]
pub enum DBCol {
    /// Column to indicate which version of database this is.
    /// - *Rows*: single row [VERSION_KEY]
    /// - *Content type*: The version of the database (u32), serialized as JSON.
    ColDbVersion = 0,
    /// Column that store Misc cells.
    /// - *Rows*: multiple, for example "GENESIS_JSON_HASH", "HEAD_KEY", [LATEST_KNOWN_KEY] etc.
    /// - *Content type*: cell specific.
    ColBlockMisc = 1,
    /// Column that stores Block content.
    /// - *Rows*: block hash (CryptHash)
    /// - *Content type*: [near_primitives::block::Block]
    ColBlock = 2,
    /// Column that stores Block headers.
    /// - *Rows*: block hash (CryptoHash)
    /// - *Content type*: [near_primitives::block_header::BlockHeader]
    ColBlockHeader = 3,
    /// Column that stores mapping from block height to block hash.
    /// - *Rows*: height (u64)
    /// - *Content type*: block hash (CryptoHash)
    ColBlockHeight = 4,
    /// Column that stores the Trie state.
    /// - *Rows*: trie_node_or_value_hash (CryptoHash)
    /// - *Content type*: Serializd RawTrieNodeWithSize or value ()
    ColState = 5,
    /// Mapping from BlockChunk to ChunkExtra
    /// - *Rows*: BlockChunk (block_hash, shard_uid)
    /// - *Content type*: [near_primitives::types::ChunkExtra]
    ColChunkExtra = 6,
    /// Mapping from transaction outcome id (CryptoHash) to list of outcome ids with proofs.
    /// - *Rows*: outcome id (CryptoHash)
    /// - *Content type*: Vec of [near_primitives::transactions::ExecutionOutcomeWithIdAndProof]
    ColTransactionResult = 7,
    /// Mapping from Block + Shard to list of outgoing receipts.
    /// - *Rows*: block + shard
    /// - *Content type*: Vec of [near_primitives::receipt::Receipt]
    ColOutgoingReceipts = 8,
    /// Mapping from Block + Shard to list of incoming receipt proofs.
    /// Each proof might prove multiple receipts.
    /// - *Rows*: (block, shard)
    /// - *Content type*: Vec of [near_primitives::sharding::ReceiptProof]
    ColIncomingReceipts = 9,
    /// Info about the peers that we are connected to. Mapping from peer_id to KnownPeerState.
    /// - *Rows*: peer_id (PublicKey)
    /// - *Content type*: [network_primitives::types::KnownPeerState]
    ColPeers = 10,
    /// Mapping from EpochId to EpochInfo
    /// - *Rows*: EpochId (CryptoHash)
    /// - *Content type*: [near_primitives::epoch_manager::EpochInfo]
    ColEpochInfo = 11,
    /// Mapping from BlockHash to BlockInfo
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: [near_primitives::epoch_manager::BlockInfo]
    ColBlockInfo = 12,
    /// Mapping from ChunkHash to ShardChunk.
    /// - *Rows*: ChunkHash (CryptoHash)
    /// - *Content type*: [near_primitives::sharding::ShardChunk]
    ColChunks = 13,
    /// Storage for  PartialEncodedChunk.
    /// - *Rows*: ChunkHash (CryptoHash)
    /// - *Content type*: [near_primitives::sharding::PartialEncodedChunk]
    ColPartialChunks = 14,
    /// Blocks for which chunks need to be applied after the state is downloaded for a particular epoch
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: Vec of BlockHash (CryptoHash)
    ColBlocksToCatchup = 15,
    /// Blocks for which the state is being downloaded
    /// - *Rows*: First block of the epoch (CryptoHash)
    /// - *Content type*: StateSyncInfo
    ColStateDlInfos = 16,
    /// Blocks that were ever challenged.
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: 'true' (bool)
    ColChallengedBlocks = 17,
    /// Contains all the Shard State Headers.
    /// - *Rows*: StateHeaderKey (ShardId || BlockHash)
    /// - *Content type*: ShardStateSyncResponseHeader
    ColStateHeaders = 18,
    /// Contains all the invalid chunks (that we had trouble decoding or verifying).
    /// - *Rows*: ShardChunkHeader object
    /// - *Content type*: EncodedShardChunk
    ColInvalidChunks = 19,
    /// Contains 'BlockExtra' information that is computed after block was processed.
    /// Currently it stores only challenges results.
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: BlockExtra
    ColBlockExtra = 20,
    /// Store hash of all block per each height, to detect double signs.
    /// - *Rows*: int (height of the block)
    /// - *Content type*: Map: EpochId -> Set of BlockHash(CryptoHash)
    ColBlockPerHeight = 21,
    /// Contains State parts that we've received.
    /// - *Rows*: StatePartKey (BlockHash || ShardId || PartId (u64))
    /// - *Content type*: state part (bytes)
    ColStateParts = 22,
    /// Contains mapping from epoch_id to epoch start (first block height of the epoch)
    /// - *Rows*: EpochId (CryptoHash)  -- TODO: where does the epoch_id come from? it looks like blockHash..
    /// - *Content type*: BlockHeight (int)
    ColEpochStart = 23,
    /// Map account_id to announce_account (which peer has announced which account in the current epoch). // TODO: explain account annoucement
    /// - *Rows*: AccountId (str)
    /// - *Content type*: AnnounceAccount
    ColAccountAnnouncements = 24,
    /// Next block hashes in the sequence of the canonical chain blocks.
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: next block: BlockHash (CryptoHash)
    ColNextBlockHashes = 25,
    /// `LightClientBlock`s corresponding to the last final block of each completed epoch.
    /// - *Rows*: EpochId (CryptoHash)
    /// - *Content type*: LightClientBlockView
    ColEpochLightClientBlocks = 26,
    /// Mapping from Receipt id to Shard id
    /// - *Rows*: ReceiptId (CryptoHash)
    /// - *Content type*: Shard Id || ref_count (u64 || u64)
    ColReceiptIdToShardId = 27,
    // Deprecated.
    _ColNextBlockWithNewChunk = 28,
    // Deprecated.
    _ColLastBlockWithNewChunk = 29,
    /// Network storage:
    ///   When given edge is removed (or we didn't get any ping from it for a while), we remove it from our 'in memory'
    ///   view and persist into storage.
    ///
    ///   This is done, so that we prevent the attack, when someone tries to introduce the edge/peer again into the network,
    ///   but with the 'old' nonce.
    ///
    ///   When we write things to storage, we do it in groups (here they are called 'components') - this naming is a little bit
    ///   unfortunate, as the peers/edges that we persist don't need to be connected or form any other 'component' (in a graph theory sense).
    ///
    ///   Each such component gets a new identifier (here called 'nonce').
    ///
    ///   We store this info in the three columns below:
    ///     - LastComponentNonce: keeps info on what is the next identifier (nonce) that can be used.
    ///     - PeerComponent: keep information on mapping from the peer to the last component that it belonged to (so that if a new peer shows
    ///         up we know which 'component' to load)
    ///     - ComponentEdges: keep the info about the edges that were connecting these peers that were removed.

    /// Map each saved peer on disk with its component id (a.k.a. nonce).
    /// - *Rows*: peer_id
    /// - *Column type*:  (nonce) u64
    ColPeerComponent = 30,
    /// Map component id  (a.k.a. nonce) with all edges in this component.
    /// These are all the edges that were purged and persisted to disk at the same time.
    /// - *Rows*: nonce
    /// - *Column type*: `Vec<near_network::routing::Edge>`
    ColComponentEdges = 31,
    /// Biggest component id (a.k.a nonce) used.
    /// - *Rows*: single row (empty row name)
    /// - *Column type*: (nonce) u64
    ColLastComponentNonce = 32,
    /// Map of transactions
    /// - *Rows*: transaction hash
    /// - *Column type*: SignedTransaction
    ColTransactions = 33,
    /// Mapping from a given (Height, ShardId) to the Chunk hash.
    /// - *Rows*: (Height || ShardId) - (u64 || u64)
    /// - *Column type*: ChunkHash (CryptoHash)
    ColChunkPerHeightShard = 34,
    /// Changes to state (Trie) that we have recorded.
    /// - *Rows*: BlockHash || TrieKey (TrieKey is written via custom to_vec)
    /// - *Column type*: TrieKey, new value and reason for change (RawStateChangesWithTrieKey)
    ColStateChanges = 35,
    /// Mapping from Block to its refcount. (Refcounts are used in handling chain forks)
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Column type*: refcount (u64)
    ColBlockRefCount = 36,
    /// Changes to Trie that we recorded during given block/shard processing.
    /// - *Rows*: BlockHash || ShardId
    /// - *Column type*: old root, new root, list of insertions, list of deletions (TrieChanges)
    ColTrieChanges = 37,
    /// Mapping from a block hash to a merkle tree of block hashes that are in the chain before it.
    /// - *Rows*: BlockHash
    /// - *Column type*: PartialMerkleTree - MerklePath to the leaf + number of leaves in the whole tree.
    ColBlockMerkleTree = 38,
    /// Mapping from height to the set of Chunk Hashes that were included in the block at that height.
    /// - *Rows*: height (u64)
    /// - *Column type*: Vec<ChunkHash (CryptoHash)>
    ColChunkHashesByHeight = 39,
    /// Mapping from block ordinal number (number of the block in the chain) to the BlockHash.
    /// - *Rows*: ordinal (u64)
    /// - *Column type*: BlockHash (CryptoHash)
    ColBlockOrdinal = 40,
    /// GC Count for each column - number of times we did the GarbageCollection on the column.
    /// - *Rows*: column id (byte)
    /// - *Column type*: u64
    ColGCCount = 41,
    /// All Outcome ids by block hash and shard id. For each shard it is ordered by execution order.
    /// TODO: seems that it has only 'transaction ids' there (not sure if intentional)
    /// - *Rows*: BlockShardId (BlockHash || ShardId) - 40 bytes
    /// - *Column type*: Vec <OutcomeId (CryptoHash)>
    ColOutcomeIds = 42,
    /// Deprecated
    _ColTransactionRefCount = 43,
    /// Heights of blocks that have been processed.
    /// - *Rows*: height (u64)
    /// - *Column type*: empty
    ColProcessedBlockHeights = 44,
    /// Mapping from receipt hash to Receipt.
    /// - *Rows*: receipt (CryptoHash)
    /// - *Column type*: Receipt
    ColReceipts = 45,
    /// Precompiled machine code of the contract, used by StoreCompiledContractCache.
    /// - *Rows*: ContractCacheKey or code hash (not sure)
    /// - *Column type*: near-vm-runner CacheRecord
    ColCachedContractCode = 46,
    /// Epoch validator information used for rpc purposes.
    /// - *Rows*: epoch id (CryptoHash)
    /// - *Column type*: EpochSummary
    ColEpochValidatorInfo = 47,
    /// Header Hashes indexed by Height.
    /// - *Rows*: height (u64)
    /// - *Column type*: Vec<HeaderHashes (CryptoHash)>
    ColHeaderHashesByHeight = 48,
    /// State changes made by a chunk, used for splitting states
    /// - *Rows*: BlockShardId (BlockHash || ShardId) - 40 bytes
    /// - *Column type*: StateChangesForSplitStates
    ColStateChangesForSplitStates = 49,
}

// Do not move this line from enum DBCol
pub const NUM_COLS: usize = 50;

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
            Self::_ColNextBlockWithNewChunk => "next block with new chunk (deprecated)",
            Self::_ColLastBlockWithNewChunk => "last block with new chunk (deprecated)",
            Self::ColPeerComponent => "peer components",
            Self::ColComponentEdges => "component edges",
            Self::ColLastComponentNonce => "last component nonce",
            Self::ColTransactions => "transactions",
            Self::ColChunkPerHeightShard => "hash of chunk per height and shard_id",
            Self::ColStateChanges => "key value changes",
            Self::ColBlockRefCount => "refcount per block",
            Self::ColTrieChanges => "trie changes",
            Self::ColBlockMerkleTree => "block merkle tree",
            Self::ColChunkHashesByHeight => "chunk hashes indexed by height_created",
            Self::ColBlockOrdinal => "block ordinal",
            Self::ColGCCount => "gc count",
            Self::ColOutcomeIds => "outcome ids",
            Self::_ColTransactionRefCount => "refcount per transaction (deprecated)",
            Self::ColProcessedBlockHeights => "processed block heights",
            Self::ColReceipts => "receipts",
            Self::ColCachedContractCode => "cached code",
            Self::ColEpochValidatorInfo => "epoch validator info",
            Self::ColHeaderHashesByHeight => "header hashes indexed by their height",
            Self::ColStateChangesForSplitStates => {
                "state changes indexed by block hash and shard id"
            }
        };
        write!(formatter, "{}", desc)
    }
}

impl DBCol {
    pub fn is_rc(&self) -> bool {
        IS_COL_RC[*self as usize]
    }
}

// List of columns for which GC should be implemented

pub static SHOULD_COL_GC: [bool; NUM_COLS] = {
    let mut col_gc = [true; NUM_COLS];
    col_gc[DBCol::ColDbVersion as usize] = false; // DB version is unrelated to GC
    col_gc[DBCol::ColBlockMisc as usize] = false;
    // TODO #3488 remove
    col_gc[DBCol::ColBlockHeader as usize] = false; // header sync needs headers
    col_gc[DBCol::ColGCCount as usize] = false; // GC count it self isn't GCed
    col_gc[DBCol::ColBlockHeight as usize] = false; // block sync needs it + genesis should be accessible
    col_gc[DBCol::ColPeers as usize] = false; // Peers is unrelated to GC
    col_gc[DBCol::ColBlockMerkleTree as usize] = false;
    col_gc[DBCol::ColAccountAnnouncements as usize] = false;
    col_gc[DBCol::ColEpochLightClientBlocks as usize] = false;
    col_gc[DBCol::ColPeerComponent as usize] = false; // Peer related info doesn't GC
    col_gc[DBCol::ColLastComponentNonce as usize] = false;
    col_gc[DBCol::ColComponentEdges as usize] = false;
    col_gc[DBCol::ColBlockOrdinal as usize] = false;
    col_gc[DBCol::ColEpochInfo as usize] = false; // https://github.com/nearprotocol/nearcore/pull/2952
    col_gc[DBCol::ColEpochValidatorInfo as usize] = false; // https://github.com/nearprotocol/nearcore/pull/2952
    col_gc[DBCol::ColEpochStart as usize] = false; // https://github.com/nearprotocol/nearcore/pull/2952
    col_gc[DBCol::ColCachedContractCode as usize] = false;
    col_gc
};

// List of columns for which GC may not be executed even in fully operational node

pub static SKIP_COL_GC: [bool; NUM_COLS] = {
    let mut col_gc = [false; NUM_COLS];
    // A node may never restarted
    col_gc[DBCol::ColStateHeaders as usize] = true;
    // True until #2515
    col_gc[DBCol::ColStateParts as usize] = true;
    col_gc
};

// List of reference counted columns

pub static IS_COL_RC: [bool; NUM_COLS] = {
    let mut col_rc = [false; NUM_COLS];
    col_rc[DBCol::ColState as usize] = true;
    col_rc[DBCol::ColTransactions as usize] = true;
    col_rc[DBCol::ColReceipts as usize] = true;
    col_rc[DBCol::ColReceiptIdToShardId as usize] = true;
    col_rc
};

pub const HEAD_KEY: &[u8; 4] = b"HEAD";
pub const TAIL_KEY: &[u8; 4] = b"TAIL";
pub const CHUNK_TAIL_KEY: &[u8; 10] = b"CHUNK_TAIL";
pub const FORK_TAIL_KEY: &[u8; 9] = b"FORK_TAIL";
pub const HEADER_HEAD_KEY: &[u8; 11] = b"HEADER_HEAD";
pub const FINAL_HEAD_KEY: &[u8; 10] = b"FINAL_HEAD";
pub const LATEST_KNOWN_KEY: &[u8; 12] = b"LATEST_KNOWN";
pub const LARGEST_TARGET_HEIGHT_KEY: &[u8; 21] = b"LARGEST_TARGET_HEIGHT";
pub const VERSION_KEY: &[u8; 7] = b"VERSION";
pub const GENESIS_JSON_HASH_KEY: &[u8; 17] = b"GENESIS_JSON_HASH";
pub const GENESIS_STATE_ROOTS_KEY: &[u8; 19] = b"GENESIS_STATE_ROOTS";

pub struct DBTransaction {
    pub ops: Vec<DBOp>,
}

pub enum DBOp {
    Insert { col: DBCol, key: Vec<u8>, value: Vec<u8> },
    UpdateRefcount { col: DBCol, key: Vec<u8>, value: Vec<u8> },
    Delete { col: DBCol, key: Vec<u8> },
    DeleteAll { col: DBCol },
}

impl DBTransaction {
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, col: DBCol, key: K, value: V) {
        self.ops.push(DBOp::Insert {
            col,
            key: key.as_ref().to_owned(),
            value: value.as_ref().to_owned(),
        });
    }

    pub fn update_refcount<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        col: DBCol,
        key: K,
        value: V,
    ) {
        self.ops.push(DBOp::UpdateRefcount {
            col,
            key: key.as_ref().to_owned(),
            value: value.as_ref().to_owned(),
        });
    }

    pub fn delete<K: AsRef<[u8]>>(&mut self, col: DBCol, key: K) {
        self.ops.push(DBOp::Delete { col, key: key.as_ref().to_owned() });
    }

    pub fn delete_all(&mut self, col: DBCol) {
        self.ops.push(DBOp::DeleteAll { col });
    }
}

pub struct RocksDB {
    db: DB,
    cfs: Vec<*const ColumnFamily>,

    check_free_space_counter: std::sync::atomic::AtomicU16,
    check_free_space_interval: u16,
    free_space_threshold: bytesize::ByteSize,
}

// DB was already Send+Sync. cf and read_options are const pointers using only functions in
// this file and safe to share across threads.
unsafe impl Send for RocksDB {}
unsafe impl Sync for RocksDB {}

/// Options for configuring [`RocksDB`](RocksDB).
///
/// ```rust
/// use near_store::db::RocksDBOptions;
///
/// let rocksdb = RocksDBOptions::default()
///     .check_free_space_interval(256)
///     .free_disk_space_threshold(bytesize::ByteSize::mb(10))
///     .read_only("/db/path");
/// ```
pub struct RocksDBOptions {
    cf_names: Option<Vec<String>>,
    cf_descriptors: Option<Vec<ColumnFamilyDescriptor>>,

    rocksdb_options: Option<Options>,
    check_free_space_interval: u16,
    free_space_threshold: bytesize::ByteSize,
    warn_treshold: bytesize::ByteSize,
}

/// Sets [`RocksDBOptions::check_free_space_interval`] to 256,
/// [`RocksDBOptions::free_disk_space_threshold`] to 16 MB and
/// [`RocksDBOptions::free_disk_space_warn_threshold`] to 256 MB.
impl Default for RocksDBOptions {
    fn default() -> Self {
        RocksDBOptions {
            cf_names: None,
            cf_descriptors: None,
            rocksdb_options: None,
            check_free_space_interval: 256,
            free_space_threshold: bytesize::ByteSize::mb(16),
            warn_treshold: bytesize::ByteSize::mb(256),
        }
    }
}

impl RocksDBOptions {
    /// Once the disk space is below the `free_disk_space_warn_threshold`, RocksDB will emit an warning message every [`interval`](RocksDBOptions::check_free_space_interval) write.
    pub fn free_disk_space_warn_threshold(mut self, warn_treshold: bytesize::ByteSize) -> Self {
        self.warn_treshold = warn_treshold;
        self
    }

    pub fn cf_names(mut self, cf_names: Vec<String>) -> Self {
        self.cf_names = Some(cf_names);
        self
    }
    pub fn cf_descriptors(mut self, cf_descriptors: Vec<ColumnFamilyDescriptor>) -> Self {
        self.cf_descriptors = Some(cf_descriptors);
        self
    }

    pub fn rocksdb_options(mut self, rocksdb_options: Options) -> Self {
        self.rocksdb_options = Some(rocksdb_options);
        self
    }

    /// After n writes, the free memory in the database's data directory is checked.
    pub fn check_free_space_interval(mut self, interval: u16) -> Self {
        self.check_free_space_interval = interval;
        self
    }

    /// Free space threshold. If the directory has fewer available bytes left, writing will not be
    /// allowed to ensure recoverability.
    pub fn free_disk_space_threshold(mut self, threshold: bytesize::ByteSize) -> Self {
        self.free_space_threshold = threshold;
        self
    }

    /// Opens a read only database.
    pub fn read_only<P: AsRef<std::path::Path>>(self, path: P) -> Result<RocksDB, DBError> {
        let options = self.rocksdb_options.unwrap_or_default();
        let cf_names: Vec<_> = self.cf_names.unwrap_or_else(|| vec!["col0".to_string()]);
        let db = DB::open_cf_for_read_only(&options, path, cf_names.iter(), false)?;
        let cfs =
            cf_names.iter().map(|n| db.cf_handle(n).unwrap() as *const ColumnFamily).collect();
        Ok(RocksDB {
            db,
            cfs,
            check_free_space_interval: self.check_free_space_interval,
            check_free_space_counter: std::sync::atomic::AtomicU16::new(0),
            free_space_threshold: self.free_space_threshold,
        })
    }

    /// Opens the database in read/write mode.
    pub fn read_write<P: AsRef<std::path::Path>>(self, path: P) -> Result<RocksDB, DBError> {
        use strum::IntoEnumIterator;
        let options = self.rocksdb_options.unwrap_or_else(rocksdb_options);
        let cf_names = self
            .cf_names
            .unwrap_or_else(|| DBCol::iter().map(|col| format!("col{}", col as usize)).collect());
        let cf_descriptors = self.cf_descriptors.unwrap_or_else(|| {
            DBCol::iter()
                .map(|col| {
                    ColumnFamilyDescriptor::new(
                        format!("col{}", col as usize),
                        rocksdb_column_options(col),
                    )
                })
                .collect()
        });
        let db = DB::open_cf_descriptors(&options, path, cf_descriptors)?;
        #[cfg(feature = "single_thread_rocksdb")]
        {
            // These have to be set after open db
            let mut env = Env::default().unwrap();
            env.set_bottom_priority_background_threads(0);
            env.set_high_priority_background_threads(0);
            env.set_low_priority_background_threads(0);
            env.set_background_threads(0);
            println!("Disabled all background threads in rocksdb");
        }
        let cfs =
            cf_names.iter().map(|n| db.cf_handle(n).unwrap() as *const ColumnFamily).collect();
        Ok(RocksDB {
            db,
            cfs,
            check_free_space_interval: self.check_free_space_interval,
            check_free_space_counter: std::sync::atomic::AtomicU16::new(0),
            free_space_threshold: self.free_space_threshold,
        })
    }
}

pub struct TestDB {
    db: RwLock<Vec<HashMap<Vec<u8>, Vec<u8>>>>,
}

pub trait Database: Sync + Send {
    fn transaction(&self) -> DBTransaction {
        DBTransaction { ops: Vec::new() }
    }
    fn get(&self, col: DBCol, key: &[u8]) -> Result<Option<Vec<u8>>, DBError>;
    fn iter<'a>(&'a self, column: DBCol) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a>;
    fn iter_without_rc_logic<'a>(
        &'a self,
        column: DBCol,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a>;
    fn iter_prefix<'a>(
        &'a self,
        col: DBCol,
        key_prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a>;
    fn write(&self, batch: DBTransaction) -> Result<(), DBError>;
    fn as_rocksdb(&self) -> Option<&RocksDB> {
        None
    }
}

impl Database for RocksDB {
    fn get(&self, col: DBCol, key: &[u8]) -> Result<Option<Vec<u8>>, DBError> {
        let read_options = rocksdb_read_options();
        let result = self.db.get_cf_opt(unsafe { &*self.cfs[col as usize] }, key, &read_options)?;
        Ok(RocksDB::get_with_rc_logic(col, result))
    }

    fn iter_without_rc_logic<'a>(
        &'a self,
        col: DBCol,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        let read_options = rocksdb_read_options();
        unsafe {
            let cf_handle = &*self.cfs[col as usize];
            let iterator = self.db.iterator_cf_opt(cf_handle, read_options, IteratorMode::Start);
            Box::new(iterator)
        }
    }

    fn iter<'a>(&'a self, col: DBCol) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        let read_options = rocksdb_read_options();
        unsafe {
            let cf_handle = &*self.cfs[col as usize];
            let iterator = self.db.iterator_cf_opt(cf_handle, read_options, IteratorMode::Start);
            RocksDB::iter_with_rc_logic(col, iterator)
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
            RocksDB::iter_with_rc_logic(col, iterator)
        }
    }

    fn write(&self, transaction: DBTransaction) -> Result<(), DBError> {
        if let Err(check) = self.pre_write_check() {
            if check.is_io() {
                warn!("unable to verify remaing disk space: {:?}, continueing write without verifying (this may result in unrecoverable data loss if disk space is exceeded", check)
            } else {
                panic!("{:?}", check)
            }
        }

        let mut batch = WriteBatch::default();
        for op in transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => unsafe {
                    batch.put_cf(&*self.cfs[col as usize], key, value);
                },
                DBOp::UpdateRefcount { col, key, value } => unsafe {
                    assert!(col.is_rc());
                    batch.merge_cf(&*self.cfs[col as usize], key, value);
                },
                DBOp::Delete { col, key } => unsafe {
                    batch.delete_cf(&*self.cfs[col as usize], key);
                },
                DBOp::DeleteAll { col } => {
                    let cf_handle = unsafe { &*self.cfs[col as usize] };
                    let opt_first = self.db.iterator_cf(cf_handle, IteratorMode::Start).next();
                    let opt_last = self.db.iterator_cf(cf_handle, IteratorMode::End).next();
                    assert_eq!(opt_first.is_some(), opt_last.is_some());
                    if let (Some((min_key, _)), Some((max_key, _))) = (opt_first, opt_last) {
                        batch.delete_range_cf(cf_handle, &min_key, &max_key);
                        // delete_range_cf deletes ["begin_key", "end_key"), so need one more delete
                        batch.delete_cf(cf_handle, max_key)
                    }
                }
            }
        }
        Ok(self.db.write(batch)?)
    }

    fn as_rocksdb(&self) -> Option<&RocksDB> {
        Some(self)
    }
}

impl Database for TestDB {
    fn get(&self, col: DBCol, key: &[u8]) -> Result<Option<Vec<u8>>, DBError> {
        let result = self.db.read().unwrap()[col as usize].get(key).cloned();
        Ok(RocksDB::get_with_rc_logic(col, result))
    }

    fn iter<'a>(&'a self, col: DBCol) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        let iterator = self.iter_without_rc_logic(col);
        RocksDB::iter_with_rc_logic(col, iterator)
    }

    fn iter_without_rc_logic<'a>(
        &'a self,
        col: DBCol,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
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
        RocksDB::iter_with_rc_logic(
            col,
            self.iter(col).filter(move |(key, _value)| key.starts_with(key_prefix)),
        )
    }

    fn write(&self, transaction: DBTransaction) -> Result<(), DBError> {
        let mut db = self.db.write().unwrap();
        for op in transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => {
                    db[col as usize].insert(key, value);
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    let mut val = db[col as usize].get(&key).cloned().unwrap_or_default();
                    merge_refcounted_records(&mut val, &value);
                    if !val.is_empty() {
                        db[col as usize].insert(key, val);
                    } else {
                        db[col as usize].remove(&key);
                    }
                }
                DBOp::Delete { col, key } => {
                    db[col as usize].remove(&key);
                }
                DBOp::DeleteAll { col } => db[col as usize].clear(),
            };
        }
        Ok(())
    }
}

/// DB level options
fn rocksdb_options() -> Options {
    let mut opts = Options::default();

    opts.create_missing_column_families(true);
    opts.create_if_missing(true);
    opts.set_use_fsync(false);
    opts.set_max_open_files(512);
    opts.set_keep_log_file_num(1);
    opts.set_bytes_per_sync(bytesize::MIB);
    opts.set_write_buffer_size(256 * bytesize::MIB as usize);
    opts.set_max_bytes_for_level_base(256 * bytesize::MIB);
    #[cfg(not(feature = "single_thread_rocksdb"))]
    {
        opts.increase_parallelism(cmp::max(1, num_cpus::get() as i32 / 2));
        opts.set_max_total_wal_size(bytesize::GIB);
    }
    #[cfg(feature = "single_thread_rocksdb")]
    {
        opts.set_disable_auto_compactions(true);
        opts.set_max_background_jobs(0);
        opts.set_stats_dump_period_sec(0);
        opts.set_stats_persist_period_sec(0);
        opts.set_level_zero_slowdown_writes_trigger(-1);
        opts.set_level_zero_file_num_compaction_trigger(-1);
        opts.set_level_zero_stop_writes_trigger(100000000);
    }

    opts
}

fn rocksdb_read_options() -> ReadOptions {
    let mut read_options = ReadOptions::default();
    read_options.set_verify_checksums(false);
    read_options
}

fn rocksdb_block_based_options(cache_size: usize) -> BlockBasedOptions {
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_size(16 * bytesize::KIB as usize);
    // We create block_cache for each of 47 columns, so the total cache size is 32 * 47 = 1504mb
    block_opts.set_block_cache(&Cache::new_lru_cache(cache_size).unwrap());
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_bloom_filter(10, true);
    block_opts
}

// TODO(#5213) Use ByteSize package to represent sizes.
fn choose_cache_size(col: DBCol) -> usize {
    match col {
        DBCol::ColState => 512 * 1024 * 1024,
        _ => 32 * 1024 * 1024,
    }
}

fn rocksdb_column_options(col: DBCol) -> Options {
    let mut opts = Options::default();
    opts.set_level_compaction_dynamic_level_bytes(true);
    let cache_size = choose_cache_size(col);
    opts.set_block_based_table_factory(&rocksdb_block_based_options(cache_size));
    opts.optimize_level_style_compaction(128 * bytesize::MIB as usize);
    opts.set_target_file_size_base(64 * bytesize::MIB);
    opts.set_compression_per_level(&[]);
    if col.is_rc() {
        opts.set_merge_operator("refcount merge", RocksDB::refcount_merge, RocksDB::refcount_merge);
        opts.set_compaction_filter("empty value filter", RocksDB::empty_value_compaction_filter);
    }
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
        RocksDBOptions::default().read_only(path)
    }

    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self, DBError> {
        RocksDBOptions::default().read_write(path)
    }

    /// Checks if there is enough memory left to perform a write. Not having enough memory left can
    /// lead to difficult to recover from state, thus a PreWriteCheckErr is pretty much
    /// unrecoverable in most cases.
    fn pre_write_check(&self) -> Result<(), PreWriteCheckErr> {
        let counter = self.check_free_space_counter.fetch_add(1, Ordering::Relaxed);
        if self.check_free_space_interval >= counter {
            return Ok(());
        }
        self.check_free_space_counter.swap(0, Ordering::Relaxed);

        let available = available_space(self.db.path())?;

        if available < 16_u64 * self.free_space_threshold {
            warn!("remaining disk space is running low ({} left)", available);
        }

        if available < self.free_space_threshold {
            Err(PreWriteCheckErr::LowDiskSpace(available))
        } else {
            Ok(())
        }
    }
}

fn available_space<P: AsRef<Path> + std::fmt::Debug>(
    path: P,
) -> std::io::Result<bytesize::ByteSize> {
    let available = fs2::available_space(path)?;
    Ok(bytesize::ByteSize::b(available))
}

#[derive(Debug, thiserror::Error)]
pub enum PreWriteCheckErr {
    #[error("error checking filesystem: {0}")]
    IO(#[from] std::io::Error),
    #[error("low disk memory ({0} available)")]
    LowDiskSpace(bytesize::ByteSize),
}

impl PreWriteCheckErr {
    pub fn is_io(&self) -> bool {
        matches!(self, PreWriteCheckErr::IO(_))
    }

    pub fn is_low_disk_space(&self) -> bool {
        matches!(self, PreWriteCheckErr::LowDiskSpace(_))
    }
}

impl Drop for RocksDB {
    fn drop(&mut self) {
        if cfg!(feature = "single_thread_rocksdb") {
            // RocksDB with only one thread stuck on wait some condition var
            // Turn on additional threads to proceed
            let mut env = Env::default().unwrap();
            env.set_background_threads(4);
        }
        self.db.cancel_all_background_work(true);
    }
}

impl TestDB {
    pub fn new() -> Self {
        let db: Vec<_> = (0..NUM_COLS).map(|_| HashMap::new()).collect();
        Self { db: RwLock::new(db) }
    }
}

#[cfg(test)]
mod tests {
    use crate::db::DBCol::ColState;
    use crate::db::{rocksdb_read_options, DBError, Database, RocksDB};
    use crate::{create_store, DBCol};

    impl RocksDB {
        #[cfg(not(feature = "single_thread_rocksdb"))]
        fn compact(&self, col: DBCol) {
            self.db.compact_range_cf(
                unsafe { &*self.cfs[col as usize] },
                Option::<&[u8]>::None,
                Option::<&[u8]>::None,
            );
        }

        fn get_no_empty_filtering(
            &self,
            col: DBCol,
            key: &[u8],
        ) -> Result<Option<Vec<u8>>, DBError> {
            let read_options = rocksdb_read_options();
            let result =
                self.db.get_cf_opt(unsafe { &*self.cfs[col as usize] }, key, &read_options)?;
            Ok(result)
        }
    }

    #[test]
    fn test_prewrite_check() {
        let tmp_dir = tempfile::Builder::new().prefix("_test_prewrite_check").tempdir().unwrap();
        let store = RocksDB::new(tmp_dir).unwrap();
        store.pre_write_check().unwrap()
    }

    #[test]
    fn test_clear_column() {
        let tmp_dir = tempfile::Builder::new().prefix("_test_clear_column").tempdir().unwrap();
        let store = create_store(tmp_dir.path());
        assert_eq!(store.get(ColState, &[1]).unwrap(), None);
        {
            let mut store_update = store.store_update();
            store_update.update_refcount(ColState, &[1], &[1], 1);
            store_update.update_refcount(ColState, &[2], &[2], 1);
            store_update.update_refcount(ColState, &[3], &[3], 1);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(ColState, &[1]).unwrap(), Some(vec![1]));
        {
            let mut store_update = store.store_update();
            store_update.delete_all(ColState);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(ColState, &[1]).unwrap(), None);
    }

    #[test]
    fn rocksdb_merge_sanity() {
        let tmp_dir = tempfile::Builder::new().prefix("_test_snapshot_sanity").tempdir().unwrap();
        let store = create_store(tmp_dir.path());
        let ptr = (&*store.storage) as *const (dyn Database + 'static);
        let rocksdb = unsafe { &*(ptr as *const RocksDB) };
        assert_eq!(store.get(ColState, &[1]).unwrap(), None);
        {
            let mut store_update = store.store_update();
            store_update.update_refcount(ColState, &[1], &[1], 1);
            store_update.commit().unwrap();
        }
        {
            let mut store_update = store.store_update();
            store_update.update_refcount(ColState, &[1], &[1], 1);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(ColState, &[1]).unwrap(), Some(vec![1]));
        assert_eq!(
            rocksdb.get_no_empty_filtering(ColState, &[1]).unwrap(),
            Some(vec![1, 2, 0, 0, 0, 0, 0, 0, 0])
        );
        {
            let mut store_update = store.store_update();
            store_update.update_refcount(ColState, &[1], &[1], -1);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(ColState, &[1]).unwrap(), Some(vec![1]));
        assert_eq!(
            rocksdb.get_no_empty_filtering(ColState, &[1]).unwrap(),
            Some(vec![1, 1, 0, 0, 0, 0, 0, 0, 0])
        );
        {
            let mut store_update = store.store_update();
            store_update.update_refcount(ColState, &[1], &[1], -1);
            store_update.commit().unwrap();
        }
        // Refcount goes to 0 -> get() returns None
        assert_eq!(store.get(ColState, &[1]).unwrap(), None);
        // Internally there is an empty value
        assert_eq!(rocksdb.get_no_empty_filtering(ColState, &[1]).unwrap(), Some(vec![]));

        #[cfg(not(feature = "single_thread_rocksdb"))]
        {
            // single_thread_rocksdb makes compact hang forever
            rocksdb.compact(ColState);
            rocksdb.compact(ColState);

            // After compaction the empty value disappears
            assert_eq!(rocksdb.get_no_empty_filtering(ColState, &[1]).unwrap(), None);
            assert_eq!(store.get(ColState, &[1]).unwrap(), None);
        }
    }
}
