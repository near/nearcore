#[cfg(feature = "protocol_feature_fix_storage_usage")]
use crate::types::AccountId;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct MigrationData {
    #[cfg(feature = "protocol_feature_fix_storage_usage")]
    pub storage_usage_delta: Vec<(AccountId, u64)>,
}

impl MigrationData {
    pub const EMPTY: MigrationData = MigrationData {
        #[cfg(feature = "protocol_feature_fix_storage_usage")]
        storage_usage_delta: Vec::new(),
    };
}

pub struct MigrationContext {
    pub is_first_block_with_current_version: bool,
    pub migration_data: Arc<MigrationData>,
}

impl Default for MigrationContext {
    fn default() -> Self {
        MigrationContext {
            is_first_block_with_current_version: false,
            migration_data: Arc::new(MigrationData::EMPTY),
        }
    }
}

impl Debug for MigrationContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigrationContext")
            .field("is_first_block_with_current_version", &self.is_first_block_with_current_version)
            .finish()
    }
}
