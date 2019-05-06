use crate::transaction::{ReceiptTransaction, TransactionResult};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ReceiptInfo {
    pub receipt: ReceiptTransaction,
    pub block_index: u64,
    pub result: TransactionResult,
}
