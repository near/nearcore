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
    /// Column that store Misc cells.
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
    /// Column that stores mapping from block height to block hash.
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
    /// Info about the peers that we are connected to. Mapping from peer_id to KnownPeerState.
    /// - *Rows*: peer_id (PublicKey)
    /// - *Content type*: [network_primitives::types::KnownPeerState]
    Peers,
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
    /// Map account_id to announce_account (which peer has announced which account in the current epoch). // TODO: explain account annoucement
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
    /// Mapping from Block to its refcount. (Refcounts are used in handling chain forks)
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
    /// - *Rows*: ordinal (u64)
    /// - *Column type*: BlockHash (CryptoHash)
    BlockOrdinal,
    /// Deprecated.
    #[strum(serialize = "GCCount")]
    _GCCount,
    /// All Outcome ids by block hash and shard id. For each shard it is ordered by execution order.
    /// TODO: seems that it has only 'transaction ids' there (not sure if intentional)
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
    /// Mapping from receipt hash to Receipt.
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
    /// - *Rows*: trie key (Vec<u8>)
    /// - *Column type*: ValueRef
    #[cfg(feature = "protocol_feature_flat_state")]
    FlatState,
    /// Deltas for flat state. Stores how flat state should be updated for the given shard and block.
    /// - *Rows*: `KeyForFlatStateDelta { shard_id, block_hash }`
    /// - *Column type*: `FlatStateDelta`
    #[cfg(feature = "protocol_feature_flat_state")]
    FlatStateDeltas,
    /// Miscellaneous data for flat state. Currently stores flat state head for each shard.
    /// - *Rows*: shard id
    /// - *Column type*: block hash (CryptoHash)
    // TODO (#7327): use only during testing, come up with proper format.
    #[cfg(feature = "protocol_feature_flat_state")]
    FlatStateMisc,
}

/// Defines different logical parts of a db key.
/// To access a column you can use a concatenation of several key types.
/// This is needed to define DBCol::key_type.
/// Update this enum and DBCol::key_type accordingly when creating a new column.
/// Currently only used in cold storage continuous migration.
#[derive(PartialEq, Copy, Clone, Debug, Hash, Eq, strum::EnumIter)]
pub enum DBKeyType {
    /// Empty row name. Used in DBCol::LastComponentNonce.
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
    /// In some sence, insert-only column acts as an rc-column, where rc is
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

    /// Whethere this column is reference-counted.
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
    pub const fn is_cold(&self) -> bool {
        match self {
            DBCol::Block
            | DBCol::BlockExtra
            | DBCol::BlockInfo
            | DBCol::ChunkExtra
            | DBCol::Chunks
            | DBCol::IncomingReceipts
            | DBCol::NextBlockHashes
            | DBCol::OutcomeIds
            | DBCol::OutgoingReceipts
            | DBCol::ReceiptIdToShardId
            | DBCol::Receipts
            | DBCol::State
            | DBCol::StateChanges
            | DBCol::StateChangesForSplitStates
            | DBCol::StateHeaders
            | DBCol::TransactionResultForBlock
            | DBCol::Transactions => true,
            _ => false,
        }
    }

    /// Vector of DBKeyType s concatenation of which results in key for the column.
    pub fn key_type(&self) -> &'static [DBKeyType] {
        match self {
            DBCol::DbVersion => &[DBKeyType::StringLiteral],
            DBCol::BlockMisc => &[DBKeyType::StringLiteral],
            DBCol::Block => &[DBKeyType::BlockHash],
            DBCol::BlockHeader => &[DBKeyType::BlockHash],
            DBCol::BlockHeight => &[DBKeyType::BlockHeight],
            DBCol::State => &[DBKeyType::ShardUId, DBKeyType::TrieNodeOrValueHash],
            DBCol::ChunkExtra => &[DBKeyType::BlockHash, DBKeyType::ShardUId],
            DBCol::_TransactionResult => &[DBKeyType::OutcomeId],
            DBCol::OutgoingReceipts => &[DBKeyType::BlockHash, DBKeyType::ShardId],
            DBCol::IncomingReceipts => &[DBKeyType::BlockHash, DBKeyType::ShardId],
            DBCol::Peers => &[DBKeyType::PeerId],
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
            #[cfg(feature = "protocol_feature_flat_state")]
            DBCol::FlatState => &[DBKeyType::TrieKey],
            #[cfg(feature = "protocol_feature_flat_state")]
            DBCol::FlatStateDeltas => &[DBKeyType::ShardId, DBKeyType::BlockHash],
            #[cfg(feature = "protocol_feature_flat_state")]
            DBCol::FlatStateMisc => &[DBKeyType::ShardId],
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
