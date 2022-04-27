use borsh::{BorshDeserialize, BorshSerialize};
use std::fmt;
use strum::{EnumCount, IntoEnumIterator};

/// This enum holds the information about the columns that we use within the RocksDB storage.
/// You can think about our storage as 2-dimensional table (with key and column as indexes/coordinates).
#[derive(
    PartialEq,
    Debug,
    Copy,
    Clone,
    Hash,
    Eq,
    BorshDeserialize,
    BorshSerialize,
    strum::EnumCount,
    strum::EnumIter,
)]
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

impl DBCol {
    /// Whethere this column is reference-counted.
    /// This means, that we're storing additional 8 bytes at the end of the payload with the current RC value.
    /// For such columns you must not use set, set_ser or delete, but 'update_refcount' instead.
    //
    /// Under the hood, we're using our custom merge operator (refcount_merge) to properly 'join' the refcounted cells.
    /// WARNING: this means that the 'value' for a given key must never change.
    /// Example:
    ///   update_refcount("foo", "bar", 1);
    ///   // good - after this call, the RC will be equal to 3.
    ///   update_refcount("foo", "bar", 2);
    ///   // bad - the value is still 'bar'.
    ///   update_refcount("foo", "baz", 1);
    ///   // ok - the value will be removed now. (as rc == 0)
    ///   update_refcount("foo", "", -3)
    ///
    /// Quick note on negative refcounts:
    ///   if we have a key that ends up having a negative refcount, we have to store this value (negative ref) in the database.
    /// Example:
    ///   update_refcount("a", "b", 1)
    ///   update_refcount("a", -3)
    ///   // Now we have the entry in the database that has "a", empty value and refcount value of -2,
    pub fn is_rc(&self) -> bool {
        RC_COLUMNS[*self as usize]
    }
    /// Whether this column is garbage collected.
    pub fn is_gc(&self) -> bool {
        !NO_GC_COLUMNS[*self as usize]
    }
    /// Whether GC for this column is possible, but optional.
    pub fn is_gc_optional(&self) -> bool {
        OPTIONAL_GC_COLUMNS[*self as usize]
    }
    /// All garbage-collected columns.
    pub fn all_gc_columns() -> impl Iterator<Item = DBCol> {
        DBCol::iter().filter(|col| col.is_gc())
    }
}

const NO_GC_COLUMNS: [bool; DBCol::COUNT] = col_set(&[
    DBCol::ColDbVersion, // DB version is unrelated to GC
    DBCol::ColBlockMisc,
    // TODO #3488 remove
    DBCol::ColBlockHeader, // header sync needs headers
    DBCol::ColGCCount,     // GC count it self isn't GCed
    DBCol::ColBlockHeight, // block sync needs it + genesis should be accessible
    DBCol::ColPeers,       // Peers is unrelated to GC
    DBCol::ColBlockMerkleTree,
    DBCol::ColAccountAnnouncements,
    DBCol::ColEpochLightClientBlocks,
    DBCol::ColPeerComponent, // Peer related info doesn't GC
    DBCol::ColLastComponentNonce,
    DBCol::ColComponentEdges,
    DBCol::ColBlockOrdinal,
    DBCol::ColEpochInfo, // https://github.com/nearprotocol/nearcore/pull/2952
    DBCol::ColEpochValidatorInfo, // https://github.com/nearprotocol/nearcore/pull/2952
    DBCol::ColEpochStart, // https://github.com/nearprotocol/nearcore/pull/2952
    DBCol::ColCachedContractCode,
]);

const OPTIONAL_GC_COLUMNS: [bool; DBCol::COUNT] = col_set(&[
    // A node may never restarted
    DBCol::ColStateHeaders,
    // True until #2515
    DBCol::ColStateParts,
]);

const RC_COLUMNS: [bool; DBCol::COUNT] = col_set(&[
    DBCol::ColState,
    DBCol::ColTransactions,
    DBCol::ColReceipts,
    DBCol::ColReceiptIdToShardId,
]);

const fn col_set(cols: &[DBCol]) -> [bool; DBCol::COUNT] {
    let mut res = [false; DBCol::COUNT];
    let mut i = 0;
    while i < cols.len() {
        res[cols[i] as usize] = true;
        i += 1;
    }
    res
}

impl fmt::Display for DBCol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        write!(f, "{}", desc)
    }
}
