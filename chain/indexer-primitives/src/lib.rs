pub use near_primitives::hash::CryptoHash;
pub use near_primitives::{self, types, views};

/// Resulting struct represents block with chunks
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct StreamerMessage {
    pub block: views::BlockView,
    pub shards: Vec<IndexerShard>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct IndexerChunkView {
    pub author: types::AccountId,
    pub header: views::ChunkHeaderView,
    pub transactions: Vec<IndexerTransactionWithOutcome>,
    pub receipts: Vec<views::ReceiptView>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IndexerTransactionWithOutcome {
    pub transaction: views::SignedTransactionView,
    pub outcome: IndexerExecutionOutcomeWithOptionalReceipt,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IndexerExecutionOutcomeWithOptionalReceipt {
    pub execution_outcome: views::ExecutionOutcomeWithIdView,
    pub receipt: Option<views::ReceiptView>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IndexerExecutionOutcomeWithReceipt {
    pub execution_outcome: views::ExecutionOutcomeWithIdView,
    pub receipt: views::ReceiptView,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct IndexerShard {
    pub shard_id: types::ShardId,
    pub chunk: Option<IndexerChunkView>,
    pub receipt_execution_outcomes: Vec<IndexerExecutionOutcomeWithReceipt>,
    pub state_changes: views::StateChangesView,
}
