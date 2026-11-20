use super::receipt_proof::ReceiptProofServePolicy;
use super::witness::WitnessServePolicy;
use crate::spice::data_manager::{DataId, SpiceData};
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::types::AccountId;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashSet;
use std::sync::Arc;

/// Each data type's chain-dependent properties for a node serving it: who receives the data,
/// and where it is stored.
pub(crate) struct ServePolicies {
    receipt_proofs: ReceiptProofServePolicy,
    witnesses: WitnessServePolicy,
}

impl ServePolicies {
    pub(crate) fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        Self {
            receipt_proofs: ReceiptProofServePolicy::new(
                chain_store.clone(),
                epoch_manager.clone(),
            ),
            witnesses: WitnessServePolicy::new(chain_store, epoch_manager),
        }
    }

    /// The nodes that apply `id`'s data.
    pub(crate) fn recipients(&self, id: &DataId) -> Result<HashSet<AccountId>, Error> {
        match id {
            DataId::ReceiptProof(id) => self.receipt_proofs.recipients(id),
            DataId::Witness(id) => self.witnesses.recipients(id),
        }
    }

    /// The data `id` names, if this node has it in the store.
    pub(crate) fn stored_data(&self, id: &DataId) -> Option<SpiceData> {
        match id {
            DataId::ReceiptProof(id) => {
                self.receipt_proofs.stored_data(id).map(SpiceData::ReceiptProof)
            }
            DataId::Witness(id) => self
                .witnesses
                .stored_data(id)
                .map(|witness| SpiceData::StateWitness(Box::new(witness))),
        }
    }
}
