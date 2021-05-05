#[cfg(feature = "protocol_feature_restore_receipts_after_fix")]
use crate::receipt::ReceiptResult;
#[cfg(feature = "protocol_feature_fix_storage_usage")]
use crate::types::AccountId;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
// #[cfg(feature = "protocol_feature_restore_receipts_after_fix")]
// use crate::borsh::maybestd::collections::HashMap;

#[derive(Default)]
pub struct MigrationData {
    #[cfg(feature = "protocol_feature_fix_storage_usage")]
    pub storage_usage_delta: Vec<(AccountId, u64)>,
    #[cfg(feature = "protocol_feature_restore_receipts_after_fix")]
    pub restored_receipts: ReceiptResult,
}

#[derive(Default)]
pub struct MigrationContext {
    pub is_first_block_with_current_version: bool,
    pub migration_data: Arc<MigrationData>,
}

impl Debug for MigrationContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigrationContext")
            .field("is_first_block_with_current_version", &self.is_first_block_with_current_version)
            .finish()
    }
}
