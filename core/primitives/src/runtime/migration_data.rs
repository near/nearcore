use crate::receipt::ReceiptResult;
use crate::types::AccountId;
use crate::types::Gas;
use std::fmt;
use std::fmt::{Debug, Formatter};

#[derive(Default)]
pub struct MigrationData {
    pub storage_usage_delta: Vec<(AccountId, u64)>,
    pub storage_usage_fix_gas: Gas,
    pub restored_receipts: ReceiptResult,
}

impl Debug for MigrationData {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigrationData").finish()
    }
}

#[derive(Debug, Default)]
pub struct MigrationFlags {
    // True iff the current block is the first one in the chain with current protocol version
    pub is_first_block_of_version: bool,
    // True iff, among all blocks containing chunk for some specific shard, the current block is the
    // first one in the first epoch with the current protocol version
    pub is_first_block_with_chunk_of_version: bool,
}
