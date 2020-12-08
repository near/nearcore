pub use near_primitives::hash::CryptoHash;
pub use near_primitives::{types, views};

/// Resulting struct represents block with chunks
#[derive(Debug)]
pub struct StreamerMessage {
    pub block: views::BlockView,
    pub chunks: Vec<IndexerChunkView>,
    pub state_changes: views::StateChangesView,
}

#[derive(Debug)]
pub struct IndexerChunkView {
    pub author: types::AccountId,
    pub header: views::ChunkHeaderView,
    pub transactions: Vec<IndexerTransactionWithOutcome>,
    pub receipts: Vec<views::ReceiptView>,
    pub receipt_execution_outcomes: Vec<IndexerExecutionOutcomeWithReceipt>,
}

#[derive(Clone, Debug)]
pub struct IndexerTransactionWithOutcome {
    pub transaction: views::SignedTransactionView,
    pub outcome: IndexerExecutionOutcomeWithReceipt,
}

#[derive(Clone, Debug)]
pub struct IndexerExecutionOutcomeWithReceipt {
    pub execution_outcome: views::ExecutionOutcomeWithIdView,
    pub receipt: Option<views::ReceiptView>,
}
