#[cfg(feature = "protocol_feature_restore_receipts_after_fix")]
use crate::receipt::ReceiptResult;
#[cfg(feature = "protocol_feature_fix_storage_usage")]
use crate::types::AccountId;
#[cfg(feature = "protocol_feature_fix_storage_usage")]
use crate::types::Gas;
use std::fmt;
use std::fmt::{Debug, Formatter};

#[derive(Default)]
pub struct MigrationData {
    #[cfg(feature = "protocol_feature_fix_storage_usage")]
    pub storage_usage_delta: Vec<(AccountId, u64)>,
    #[cfg(feature = "protocol_feature_fix_storage_usage")]
    pub storage_usage_fix_gas: Gas,
    #[cfg(feature = "protocol_feature_restore_receipts_after_fix")]
    pub restored_receipts: ReceiptResult,
}

impl Debug for MigrationData {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigrationData").finish()
    }
}
