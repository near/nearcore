use std::fmt;

/// This enum holds the information about the columns that we use within the
/// RocksDB storage.
///
/// You can think about our storage as 2-dimensional table (with key and column
/// as indexes/coordinates).
///
/// Note that the names of the variants in this enumeration correspond to the
/// name of the RocksDB column families.  As such, it is *not* safe to rename
/// a variant.
///
/// The only exception is adding an underscore at the beginning of the name to
/// indicate that the column has been deprecated.  Deprecated columns are not
/// used except for the database migration code which needs to deal with the
/// deprecation.  Make sure to add `#[strum(serialize = "OriginalName")]`
/// attribute in front of the variant when you deprecate a column.
#[derive(
    PartialEq, Copy, Clone, Debug, Hash, Eq, enum_map::Enum, strum::EnumIter, strum::IntoStaticStr,
)]
pub enum DBCol {
    /// Column to indicate which version of database this is.
    /// - *Rows*: single row `"VERSION"`
    /// - *Content type*: The version of the database (u32), serialized as JSON.
    DbVersion,
    /// Column that stores miscellaneous block-related cells.
    /// - *Rows*: multiple, for example `"GENESIS_JSON_HASH"`, `"HEAD_KEY"`, `"LATEST_KNOWN_KEY"` etc.
    /// - *Content type*: cell specific.
    BlockMisc,
    /// Column that stores Block content.
    /// - *Rows*: block hash (CryptHash)
    /// - *Content type*: [near_primitives::block::Block]
    Block,
    /// Column that stores Block headers.
    /// - *Rows*: block hash (CryptoHash)
    /// - *Content type*: [near_primitives::block_header::BlockHeader]
    BlockHeader,
    /// Column that stores mapping from block height to block hash on the current canonical chain.
    /// (if you want to see all the blocks that we got for a given height, for example due to double signing etc,
    /// look at BlockPerHeight column).
    /// - *Rows*: height (u64)
    /// - *Content type*: block hash (CryptoHash)
    BlockHeight,
    /// Column that stores the Trie state.
    /// - *Rows*: trie_node_or_value_hash (CryptoHash)
    /// - *Content type*: Serializd RawTrieNodeWithSize or value ()
    State,
    /// Mapping from BlockChunk to ChunkExtra
    /// - *Rows*: BlockChunk (block_hash, shard_uid)
    /// - *Content type*: [near_primitives::types::chunk_extra::ChunkExtra]
    ChunkExtra,
    /// Deprecated.
    #[strum(serialize = "TransactionResult")]
    _TransactionResult,
    /// Mapping from Block + Shard to list of outgoing receipts.
    /// - *Rows*: block + shard
    /// - *Content type*: Vec of [near_primitives::receipt::Receipt]
    OutgoingReceipts,
    /// Mapping from Block + Shard to list of incoming receipt proofs.
    /// Each proof might prove multiple receipts.
    /// - *Rows*: (block, shard)
    /// - *Content type*: Vec of [near_primitives::sharding::ReceiptProof]
    IncomingReceipts,
    /// Deprecated.
    #[strum(serialize = "Peers")]
    _Peers,
    /// List of recent outbound TIER2 connections. We'll attempt to re-establish
    /// these connections after node restart or upon disconnection.
    /// - *Rows*: single row (empty row name)
    /// - *Content type*: Vec of [network_primitives::types::ConnectionInfo]
    RecentOutboundConnections,
    /// Mapping from EpochId to EpochInfo
    /// - *Rows*: EpochId (CryptoHash)
    /// - *Content type*: [near_primitives::epoch_manager::epoch_info::EpochInfo]
    EpochInfo,
    /// Mapping from BlockHash to BlockInfo
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: [near_primitives::epoch_manager::block_info::BlockInfo]
    BlockInfo,
    /// Mapping from ChunkHash to ShardChunk.
    /// - *Rows*: ChunkHash (CryptoHash)
    /// - *Content type*: [near_primitives::sharding::ShardChunk]
    Chunks,
    /// Storage for  PartialEncodedChunk.
    /// - *Rows*: ChunkHash (CryptoHash)
    /// - *Content type*: [near_primitives::sharding::PartialEncodedChunk]
    PartialChunks,
    /// Blocks for which chunks need to be applied after the state is downloaded for a particular epoch
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: Vec of BlockHash (CryptoHash)
    BlocksToCatchup,
    /// Blocks for which the state is being downloaded
    /// - *Rows*: First block of the epoch (CryptoHash)
    /// - *Content type*: StateSyncInfo
    StateDlInfos,
    /// Blocks that were ever challenged.
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: 'true' (bool)
    ChallengedBlocks,
    /// Contains all the Shard State Headers.
    /// - *Rows*: StateHeaderKey (ShardId || BlockHash)
    /// - *Content type*: ShardStateSyncResponseHeader
    StateHeaders,
    /// Contains all the invalid chunks (that we had trouble decoding or verifying).
    /// - *Rows*: ShardChunkHeader object
    /// - *Content type*: EncodedShardChunk
    InvalidChunks,
    /// Contains 'BlockExtra' information that is computed after block was processed.
    /// Currently it stores only challenges results.
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: BlockExtra
    BlockExtra,
    /// Store hash of all block per each height, to detect double signs.
    /// In most cases, it is better to get the value from BlockHeight column instead (which
    /// keeps the hash of the block from canonical chain)
    /// - *Rows*: int (height of the block)
    /// - *Content type*: Map: EpochId -> Set of BlockHash(CryptoHash)
    BlockPerHeight,
    /// Contains State parts that we've received.
    /// - *Rows*: StatePartKey (BlockHash || ShardId || PartId (u64))
    /// - *Content type*: state part (bytes)
    StateParts,
    /// Contains mapping from epoch_id to epoch start (first block height of the epoch)
    /// - *Rows*: EpochId (CryptoHash)  -- TODO: where does the epoch_id come from? it looks like blockHash..
    /// - *Content type*: BlockHeight (int)
    EpochStart,
    /// Map account_id to announce_account (which peer has announced which account in the current epoch). // TODO: explain account announcement
    /// - *Rows*: AccountId (str)
    /// - *Content type*: AnnounceAccount
    AccountAnnouncements,
    /// Next block hashes in the sequence of the canonical chain blocks.
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: next block: BlockHash (CryptoHash)
    NextBlockHashes,
    /// `LightClientBlock`s corresponding to the last final block of each completed epoch.
    /// - *Rows*: EpochId (CryptoHash)
    /// - *Content type*: LightClientBlockView
    EpochLightClientBlocks,
    /// Mapping from Receipt id to destination Shard Id, i.e, the shard that this receipt is sent to.
    /// - *Rows*: ReceiptId (CryptoHash)
    /// - *Content type*: Shard Id || ref_count (u64 || u64)
    ReceiptIdToShardId,
    // Deprecated.
    #[strum(serialize = "NextBlockWithNewChunk")]
    _NextBlockWithNewChunk,
    // Deprecated.
    #[strum(serialize = "LastBlockWithNewChunk")]
    _LastBlockWithNewChunk,
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
    PeerComponent,
    /// Map component id  (a.k.a. nonce) with all edges in this component.
    /// These are all the edges that were purged and persisted to disk at the same time.
    /// - *Rows*: nonce
    /// - *Column type*: `Vec<near_network::routing::Edge>`
    ComponentEdges,
    /// Biggest component id (a.k.a nonce) used.
    /// - *Rows*: single row (empty row name)
    /// - *Column type*: (nonce) u64
    LastComponentNonce,
    /// Map of transactions
    /// - *Rows*: transaction hash
    /// - *Column type*: SignedTransaction
    Transactions,
    /// Deprecated.
    #[strum(serialize = "ChunkPerHeightShard")]
    _ChunkPerHeightShard,
    /// Changes to state (Trie) that we have recorded.
    /// - *Rows*: BlockHash || TrieKey (TrieKey is written via custom to_vec)
    /// - *Column type*: TrieKey, new value and reason for change (RawStateChangesWithTrieKey)
    StateChanges,
    /// Mapping from Block to its refcount (number of blocks that use this block as a parent). (Refcounts are used in handling chain forks).
    /// In following example:
    ///     1 -> 2 -> 3 -> 5
    ///           \ --> 4
    /// The block '2' will have a refcount equal to 2.
    ///
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Column type*: refcount (u64)
    BlockRefCount,
    /// Changes to Trie that we recorded during given block/shard processing.
    /// - *Rows*: BlockHash || ShardId
    /// - *Column type*: old root, new root, list of insertions, list of deletions (TrieChanges)
    TrieChanges,
    /// Mapping from a block hash to a merkle tree of block hashes that are in the chain before it.
    /// - *Rows*: BlockHash
    /// - *Column type*: PartialMerkleTree - MerklePath to the leaf + number of leaves in the whole tree.
    BlockMerkleTree,
    /// Mapping from height to the set of Chunk Hashes that were included in the block at that height.
    /// - *Rows*: height (u64)
    /// - *Column type*: Vec<ChunkHash (CryptoHash)>
    ChunkHashesByHeight,
    /// Mapping from block ordinal number (number of the block in the chain) to the BlockHash.
    /// Note: that it can be different than BlockHeight - if we have skipped some heights when creating the blocks.
    ///       for example in chain 1->3, the second block has height 3, but ordinal 2.
    /// - *Rows*: ordinal (u64)
    /// - *Column type*: BlockHash (CryptoHash)
    BlockOrdinal,
    /// Deprecated.
    #[strum(serialize = "GCCount")]
    _GCCount,
    /// All Outcome ids by block hash and shard id. For each shard it is ordered by execution order.
    /// - *Rows*: BlockShardId (BlockHash || ShardId) - 40 bytes
    /// - *Column type*: Vec <OutcomeId (CryptoHash)>
    OutcomeIds,
    /// Deprecated
    #[strum(serialize = "TransactionRefCount")]
    _TransactionRefCount,
    /// Heights of blocks that have been processed.
    /// - *Rows*: height (u64)
    /// - *Column type*: empty
    ProcessedBlockHeights,
    /// Mapping from receipt hash to Receipt. Note that this doesn't store _all_
    /// receipts. Some receipts are ephemeral and get processed after creation
    /// without getting into the database at all.
    /// - *Rows*: receipt (CryptoHash)
    /// - *Column type*: Receipt
    Receipts,
    /// Precompiled machine code of the contract, used by StoreCompiledContractCache.
    /// - *Rows*: ContractCacheKey or code hash (not sure)
    /// - *Column type*: near-vm-runner CacheRecord
    CachedContractCode,
    /// Epoch validator information used for rpc purposes.
    /// - *Rows*: epoch id (CryptoHash)
    /// - *Column type*: EpochSummary
    EpochValidatorInfo,
    /// Header Hashes indexed by Height.
    /// - *Rows*: height (u64)
    /// - *Column type*: Vec<HeaderHashes (CryptoHash)>
    HeaderHashesByHeight,
    /// State changes made by a chunk, used for splitting states
    /// - *Rows*: BlockShardId (BlockHash || ShardId) - 40 bytes
    /// - *Column type*: StateChangesForSplitStates
    StateChangesForSplitStates,
    /// Transaction or receipt outcome, by outcome ID (transaction or receipt hash) and block
    /// hash. Multiple outcomes may be stored for the same outcome ID in case of forks.
    /// *Rows*: OutcomeId (CryptoHash) || BlockHash (CryptoHash)
    /// *Column type*: ExecutionOutcomeWithProof
    TransactionResultForBlock,
    /// Flat state contents. Used to get `ValueRef` by trie key faster than doing a trie lookup.
    /// - *Rows*: `shard_uid` + trie key (Vec<u8>)
    /// - *Column type*: FlatStateValue
    FlatState,
    /// Changes for flat state delta. Stores how flat state should be updated for the given shard and block.
    /// - *Rows*: `KeyForFlatStateDelta { shard_uid, block_hash }`
    /// - *Column type*: `FlatStateChanges`
    FlatStateChanges,
    /// Metadata for flat state delta.
    /// - *Rows*: `KeyForFlatStateDelta { shard_uid, block_hash }`
    /// - *Column type*: `FlatStateDeltaMetadata`
    FlatStateDeltaMetadata,
    /// Flat storage status for the corresponding shard.
    /// - *Rows*: `shard_uid`
    /// - *Column type*: `FlatStorageStatus`
    FlatStorageStatus,
    /// Column to persist pieces of miscellaneous small data. Should only be used to store
    /// constant or small (for example per-shard) amount of data.
    /// - *Rows*: arbitrary string, see `crate::db::FLAT_STATE_VALUES_INLINING_MIGRATION_STATUS_KEY` for example
    /// - *Column type*: arbitrary bytes
    Misc,
}

/// Defines different logical parts of a db key.
/// To access a column you can use a concatenation of several key types.
/// This is needed to define DBCol::key_type.
/// Update this enum and DBCol::key_type accordingly when creating a new column.
/// Currently only used in cold storage continuous migration.
#[derive(PartialEq, Copy, Clone, Debug, Hash, Eq, strum::EnumIter)]
pub enum DBKeyType {
    /// Empty row name. Used in DBCol::LastComponentNonce and DBCol::RecentOutboundConnections
    Empty,
    /// Set of predetermined strings. Used, for example, in DBCol::BlockMisc
    StringLiteral,
    BlockHash,
    /// Hash of the previous block. Logically different from BlockHash. Used fro DBCol::NextBlockHashes.
    PreviousBlockHash,
    BlockHeight,
    BlockOrdinal,
    ShardId,
    ShardUId,
    ChunkHash,
    EpochId,
    Nonce,
    PeerId,
    AccountId,
    TrieNodeOrValueHash,
    TrieKey,
    ReceiptHash,
    TransactionHash,
    OutcomeId,
    ContractCacheKey,
    PartId,
    ColumnId,
}

impl DBCol {
    /// Whether data in this column is effectively immutable.
    ///
    /// Data in such columns is never overwriten, though it can be deleted by gc
    /// eventually. Specifically, for a given key:
    ///
    /// * It's OK to insert a new value.
    /// * It's also OK to insert a value which is equal to the one already
    ///   stored.
    /// * Inserting a different value would crash the node in debug, but not in
    ///   release.
    /// * GC (and only GC) is allowed to remove any value.
    ///
    /// In some sense, insert-only column acts as an rc-column, where rc is
    /// always one.
    pub const fn is_insert_only(&self) -> bool {
        match self {
            DBCol::Block
            | DBCol::BlockHeader
            | DBCol::BlockExtra
            | DBCol::BlockInfo
            | DBCol::Chunks
            | DBCol::InvalidChunks
            | DBCol::PartialChunks
            | DBCol::TransactionResultForBlock => true,
            _ => false,
        }
    }

    /// Whether this column is reference-counted.
    ///
    /// A reference-counted column is one where we store additional 8-byte value
    /// at the end of the payload with the current reference counter value.  For
    /// such columns you must not use `set`, `set_ser` or `delete` operations,
    /// but 'increment_refcount' and `decrement_refcount` instead.
    ///
    /// Under the hood, we’re using custom merge operator (see
    /// [`crate::db::RocksDB::refcount_merge`]) to properly ‘join’ the
    /// refcounted cells.  This means that the 'value' for a given key must
    /// never change.
    ///
    /// Example:
    ///
    /// ```ignore
    /// increment_refcount("foo", "bar");
    /// // good - after this call, the RC will be equal to 3.
    /// increment_refcount_by("foo", "bar", 2);
    /// // bad - the value is still 'bar'.
    /// increment_refcount("foo", "baz");
    /// // ok - the value will be removed now. (as rc == 0)
    /// decrement_refcount_by("foo", "", 3)
    /// ```
    ///
    /// Quick note on negative refcounts: if we have a key that ends up having
    /// a negative refcount, we have to store this value (negative ref) in the
    /// database.
    ///
    /// Example:
    ///
    /// ```ignore
    /// increment_refcount("a", "b");
    /// decrement_refcount_by("a", 3);
    /// // Now we have the entry in the database with key "a", empty payload and
    /// // refcount value of -2,
    /// ```
    pub const fn is_rc(&self) -> bool {
        match self {
            DBCol::State | DBCol::Transactions | DBCol::Receipts | DBCol::ReceiptIdToShardId => {
                true
            }
            _ => false,
        }
    }

    /// Whether this column should be copied to the cold storage.
    ///
    /// This doesn't include DbVersion and BlockMisc columns which are present
    /// in the cold database but rather than being copied from hot database are
    /// maintained separately.
    pub const fn is_cold(&self) -> bool {
        // Explicitly list all columns so that if new one is added it'll need to
        // be added here as well.
        match self {
            // DBVersion and BlockMisc are maintained separately in the cold
            // storage, they should not be copied from hot.
            DBCol::DbVersion | DBCol::BlockMisc => false,
            // Most of the GC-ed columns should be copied to the cold storage.
            DBCol::Block
            | DBCol::BlockExtra
            | DBCol::BlockInfo
            // TODO can be reconstruction from BlockHeight instead of saving to cold storage.
            | DBCol::BlockPerHeight
            | DBCol::ChunkExtra
            // TODO can be changed to reconstruction from Block instead of saving to cold storage.
            | DBCol::ChunkHashesByHeight
            | DBCol::Chunks
            | DBCol::IncomingReceipts
            | DBCol::NextBlockHashes
            | DBCol::OutcomeIds
            | DBCol::OutgoingReceipts
            // TODO can be changed to reconstruction on request instead of saving in cold storage.
            | DBCol::PartialChunks
            | DBCol::ReceiptIdToShardId
            | DBCol::Receipts
            | DBCol::State
            | DBCol::StateChanges
            // TODO StateChangesForSplitStates is not GC-ed, why is it here?
            | DBCol::StateChangesForSplitStates
            | DBCol::StateHeaders
            | DBCol::TransactionResultForBlock
            | DBCol::Transactions => true,

            // TODO
            DBCol::ChallengedBlocks => false,
            DBCol::Misc => false,
            // BlockToCatchup is only needed while syncing and it is not immutable.
            DBCol::BlocksToCatchup => false,
            // BlockRefCount is only needed when handling forks and it is not immutable.
            DBCol::BlockRefCount => false,
            // InvalidChunks is only needed at head when accepting new chunks.
            DBCol::InvalidChunks => false,
            // StateParts is only needed while syncing.
            DBCol::StateParts => false,
            // TrieChanges is only needed for GC.
            DBCol::TrieChanges => false,
            // StateDlInfos is only needed when syncing and it is not immutable.
            DBCol::StateDlInfos => false,
            // TODO
            DBCol::ProcessedBlockHeights => false,
            // HeaderHashesByHeight is only needed for GC.
            DBCol::HeaderHashesByHeight => false,

            // Columns that are not GC-ed need not be copied to the cold storage.
            DBCol::BlockHeader
            | DBCol::_GCCount
            | DBCol::BlockHeight
            | DBCol::_Peers
            | DBCol::RecentOutboundConnections
            | DBCol::BlockMerkleTree
            | DBCol::AccountAnnouncements
            | DBCol::EpochLightClientBlocks
            | DBCol::PeerComponent
            | DBCol::LastComponentNonce
            | DBCol::ComponentEdges
            | DBCol::EpochInfo
            | DBCol::EpochStart
            | DBCol::EpochValidatorInfo
            | DBCol::BlockOrdinal
            | DBCol::_ChunkPerHeightShard
            | DBCol::_NextBlockWithNewChunk
            | DBCol::_LastBlockWithNewChunk
            | DBCol::_TransactionRefCount
            | DBCol::_TransactionResult
            // | DBCol::StateChangesForSplitStates
            | DBCol::CachedContractCode
            | DBCol::FlatState
            | DBCol::FlatStateChanges
            | DBCol::FlatStateDeltaMetadata
            | DBCol::FlatStorageStatus => false,
        }
    }

    /// Whether this column exists in cold storage.
    pub(crate) const fn is_in_colddb(&self) -> bool {
        matches!(*self, DBCol::DbVersion | DBCol::BlockMisc) || self.is_cold()
    }

    /// Vector of DBKeyType s concatenation of which results in key for the column.
    pub fn key_type(&self) -> &'static [DBKeyType] {
        match self {
            DBCol::DbVersion => &[DBKeyType::StringLiteral],
            DBCol::BlockMisc => &[DBKeyType::StringLiteral],
            DBCol::Misc => &[DBKeyType::StringLiteral],
            DBCol::Block => &[DBKeyType::BlockHash],
            DBCol::BlockHeader => &[DBKeyType::BlockHash],
            DBCol::BlockHeight => &[DBKeyType::BlockHeight],
            DBCol::State => &[DBKeyType::ShardUId, DBKeyType::TrieNodeOrValueHash],
            DBCol::ChunkExtra => &[DBKeyType::BlockHash, DBKeyType::ShardUId],
            DBCol::_TransactionResult => &[DBKeyType::OutcomeId],
            DBCol::OutgoingReceipts => &[DBKeyType::BlockHash, DBKeyType::ShardId],
            DBCol::IncomingReceipts => &[DBKeyType::BlockHash, DBKeyType::ShardId],
            DBCol::_Peers => &[DBKeyType::PeerId],
            DBCol::RecentOutboundConnections => &[DBKeyType::Empty],
            DBCol::EpochInfo => &[DBKeyType::EpochId],
            DBCol::BlockInfo => &[DBKeyType::BlockHash],
            DBCol::Chunks => &[DBKeyType::ChunkHash],
            DBCol::PartialChunks => &[DBKeyType::ChunkHash],
            DBCol::BlocksToCatchup => &[DBKeyType::BlockHash],
            DBCol::StateDlInfos => &[DBKeyType::BlockHash],
            DBCol::ChallengedBlocks => &[DBKeyType::BlockHash],
            DBCol::StateHeaders => &[DBKeyType::ShardId, DBKeyType::BlockHash],
            DBCol::InvalidChunks => &[DBKeyType::ChunkHash],
            DBCol::BlockExtra => &[DBKeyType::BlockHash],
            DBCol::BlockPerHeight => &[DBKeyType::BlockHeight],
            DBCol::StateParts => &[DBKeyType::BlockHash, DBKeyType::ShardId, DBKeyType::PartId],
            DBCol::EpochStart => &[DBKeyType::EpochId],
            DBCol::AccountAnnouncements => &[DBKeyType::AccountId],
            DBCol::NextBlockHashes => &[DBKeyType::PreviousBlockHash],
            DBCol::EpochLightClientBlocks => &[DBKeyType::EpochId],
            DBCol::ReceiptIdToShardId => &[DBKeyType::ReceiptHash],
            DBCol::_NextBlockWithNewChunk => &[DBKeyType::BlockHash, DBKeyType::ShardId],
            DBCol::_LastBlockWithNewChunk => &[DBKeyType::ShardId],
            DBCol::PeerComponent => &[DBKeyType::PeerId],
            DBCol::ComponentEdges => &[DBKeyType::Nonce],
            DBCol::LastComponentNonce => &[DBKeyType::Empty],
            DBCol::Transactions => &[DBKeyType::TransactionHash],
            DBCol::_ChunkPerHeightShard => &[DBKeyType::BlockHeight, DBKeyType::ShardId],
            DBCol::StateChanges => &[DBKeyType::BlockHash, DBKeyType::TrieKey],
            DBCol::BlockRefCount => &[DBKeyType::BlockHash],
            DBCol::TrieChanges => &[DBKeyType::BlockHash, DBKeyType::ShardUId],
            DBCol::BlockMerkleTree => &[DBKeyType::BlockHash],
            DBCol::ChunkHashesByHeight => &[DBKeyType::BlockHeight],
            DBCol::BlockOrdinal => &[DBKeyType::BlockOrdinal],
            DBCol::_GCCount => &[DBKeyType::ColumnId],
            DBCol::OutcomeIds => &[DBKeyType::BlockHash, DBKeyType::ShardId],
            DBCol::_TransactionRefCount => &[DBKeyType::TransactionHash],
            DBCol::ProcessedBlockHeights => &[DBKeyType::BlockHeight],
            DBCol::Receipts => &[DBKeyType::ReceiptHash],
            DBCol::CachedContractCode => &[DBKeyType::ContractCacheKey],
            DBCol::EpochValidatorInfo => &[DBKeyType::EpochId],
            DBCol::HeaderHashesByHeight => &[DBKeyType::BlockHeight],
            DBCol::StateChangesForSplitStates => &[DBKeyType::BlockHash, DBKeyType::ShardId],
            DBCol::TransactionResultForBlock => &[DBKeyType::OutcomeId, DBKeyType::BlockHash],
            DBCol::FlatState => &[DBKeyType::ShardUId, DBKeyType::TrieKey],
            DBCol::FlatStateChanges => &[DBKeyType::ShardUId, DBKeyType::BlockHash],
            DBCol::FlatStateDeltaMetadata => &[DBKeyType::ShardUId, DBKeyType::BlockHash],
            DBCol::FlatStorageStatus => &[DBKeyType::ShardUId],
        }
    }
}

impl fmt::Display for DBCol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[test]
fn column_props_sanity() {
    use strum::IntoEnumIterator;

    for col in DBCol::iter() {
        // Check that rc and write_once are mutually exclusive.
        assert!((col.is_rc() as u32) + (col.is_insert_only() as u32) <= 1, "{col}")
    }
}
