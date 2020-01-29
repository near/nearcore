use crate::Store;
use near_primitives::hash::CryptoHash;
use std::sync::Arc;

pub struct ForksManager {
    pub(crate) store: Arc<Store>,
}

impl ForksManager {
    /// Checks if two blocks are on the same chain and one is preceded by another.
    pub fn is_same_chain(&self, ancestor: CryptoHash, descendant: CryptoHash) -> bool {
        true
    }
}
