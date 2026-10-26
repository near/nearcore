use super::receipt_proof::ReceiptProofServePolicy;
use crate::spice::data_manager::{DataId, SpiceData};
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::types::AccountId;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashSet;
use std::sync::Arc;

/// A data type's chain-dependent properties for a node serving it: who produces and who
/// receives the data, and where it is stored.
pub(crate) trait ServePolicy {
    /// The producers of `id`, in the parts' encoding order.
    fn producers(&self, id: &DataId) -> Result<Vec<AccountId>, Error>;

    /// The nodes that apply `id`'s data.
    fn recipients(&self, id: &DataId) -> Result<HashSet<AccountId>, Error>;

    /// The data `id` names, if this node has it in the store.
    fn stored_data(&self, id: &DataId) -> Option<SpiceData>;
}

/// The serve policies, one per [`DataId`] variant.
pub(crate) struct ServePolicies {
    receipt_proofs: ReceiptProofServePolicy,
}

impl ServePolicies {
    pub(crate) fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        Self { receipt_proofs: ReceiptProofServePolicy::new(chain_store, epoch_manager) }
    }

    fn policy(&self, id: &DataId) -> &dyn ServePolicy {
        match id {
            DataId::ReceiptProof { .. } => &self.receipt_proofs,
        }
    }
}

/// Dispatches every call to the policy of `id`'s data type.
impl ServePolicy for ServePolicies {
    fn producers(&self, id: &DataId) -> Result<Vec<AccountId>, Error> {
        self.policy(id).producers(id)
    }

    fn recipients(&self, id: &DataId) -> Result<HashSet<AccountId>, Error> {
        self.policy(id).recipients(id)
    }

    fn stored_data(&self, id: &DataId) -> Option<SpiceData> {
        self.policy(id).stored_data(id)
    }
}
