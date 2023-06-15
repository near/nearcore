/// This file contains structures for the Entity Debug UI.
/// They are to be sent to the UI as JSON.
use near_primitives::{
    hash::CryptoHash,
    shard_layout::ShardUId,
    types::{BlockHeight, EpochId, ShardId},
};
use serde::{Deserialize, Serialize};

use crate::errors::RpcError;

/// One entry to be displayed in the UI as a single row.
#[derive(Serialize)]
pub struct EntityDataEntry {
    /// Can be a struct field name or a stringified array index.
    pub name: String,
    pub value: EntityDataValue,
}

/// Represents either a single value, or a struct. An array is also considered
/// a struct, with keys being array indices. All value types are represented as
/// strings even if they are numerical, for simplicity.
#[derive(Serialize)]
#[serde(untagged)]
pub enum EntityDataValue {
    String(String),
    Struct(Box<EntityDataStruct>),
}

/// A list of entries - either a struct or an array.
#[derive(Serialize)]
pub struct EntityDataStruct {
    pub entries: Vec<EntityDataEntry>,
}

impl EntityDataStruct {
    pub fn new() -> EntityDataStruct {
        EntityDataStruct { entries: Vec::new() }
    }
}

/// All queries supported by the Entity Debug UI.
/// To add a new query, make a new enum variant. The only constraints are:
///   - The variant must either be (()), or a struct variant whose field names
///     correspond to some EntityKeyType (in the UI code).
///   - Across all queries, each unique entity key name must have the same type,
///     e.g. "epoch_id" must always have the same type, in this case 'EpochId'.
///
/// Queries in general can return anything. On the UI side we annotate on the
/// returned structure to provide links for further queries. For example, on the
/// UI side we annotate that TipAtHead returns a structure whose prev_block_hash
/// corresponds to a block_hash entity key, which can then be used to query for
/// e.g. BlockHeaderByHash.
#[derive(Serialize, Deserialize)]
pub enum EntityQuery {
    TipAtHead(()),
    TipAtHeaderHead(()),
    TipAtFinalHead(()),
    BlockByHash { block_hash: CryptoHash },
    BlockHeaderByHash { block_hash: CryptoHash },
    BlockHashByHeight { block_height: BlockHeight },
    ChunkByHash { chunk_hash: CryptoHash },
    EpochInfoByEpochId { epoch_id: EpochId },
    TransactionByHash { transaction_hash: CryptoHash },
    ReceiptById { receipt_id: CryptoHash },
    OutcomeByTransactionHash { transaction_hash: CryptoHash },
    OutcomeByTransactionHashAndBlockHash { transaction_hash: CryptoHash, block_hash: CryptoHash },
    OutcomeByReceiptId { receipt_id: CryptoHash },
    OutcomeByReceiptIdAndBlockHash { receipt_id: CryptoHash, block_hash: CryptoHash },
    TrieRootByChunkHash { chunk_hash: CryptoHash },
    TrieRootByStateRoot { state_root: CryptoHash, shard_uid: ShardUId },
    TrieNode { trie_path: String },
    ShardIdByAccountId { account_id: String, epoch_id: EpochId },
    ShardUIdByShardId { shard_id: ShardId, epoch_id: EpochId },
    ShardLayoutByEpochId { epoch_id: EpochId },
    AllShardsByEpochId { epoch_id: EpochId },
    FlatStorageStatusByShardUId { shard_uid: ShardUId },
    FlatStateByTrieKey { trie_key: String, shard_uid: ShardUId },
    FlatStateChangesByBlockHash { block_hash: CryptoHash, shard_uid: ShardUId },
    FlatStateDeltaMetadataByBlockHash { block_hash: CryptoHash, shard_uid: ShardUId },
}

/// We use a trait for this, because jsonrpc does not have access to low-level
/// blockchain data structures for implementing the queries.
pub trait EntityDebugHandler: Sync + Send {
    fn query(&self, query: EntityQuery) -> Result<EntityDataValue, RpcError>;
}

/// For tests.
pub struct DummyEntityDebugHandler {}

impl EntityDebugHandler for DummyEntityDebugHandler {
    fn query(&self, _query: EntityQuery) -> Result<EntityDataValue, RpcError> {
        Err(RpcError::new_internal_error(None, "Not implemented".to_string()))
    }
}
