use super::receipt_proof::ReceiptProofFetchPolicy;
use crate::spice::data_manager::DataId;
use near_chain::{Block, Error};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::types::AccountId;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::sync::Arc;

/// A data type's chain-dependent properties for a node fetching it: relevance, doneness,
/// who holds the data.
pub(crate) trait FetchPolicy {
    /// The ids of this type that this node needs from `block`, each with its producers in
    /// the parts' encoding order.
    // TODO(spice-data-distribution): each id comes with a tri-state `Interest` once
    // witnesses move onto the engine (#16275).
    fn needed_items(&self, block: &BlockHeader) -> Result<Vec<(DataId, Vec<AccountId>)>, Error>;

    /// Whether the durable artifact this item exists to obtain is already in the store.
    fn is_done(&self, id: &DataId) -> bool;

    /// The ids of this type that `block` makes pullable, tracked or not.
    fn made_pullable_by(&self, block: &Block) -> Result<Vec<DataId>, Error>;

    /// The producers of `id`, in the parts' encoding order.
    fn producers(&self, id: &DataId) -> Result<Vec<AccountId>, Error>;
}

/// The fetch policies, one per [`DataId`] variant.
pub(crate) struct FetchPolicies {
    receipt_proofs: ReceiptProofFetchPolicy,
}

impl FetchPolicies {
    pub(crate) fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
    ) -> Self {
        Self {
            receipt_proofs: ReceiptProofFetchPolicy::new(chain_store, epoch_manager, shard_tracker),
        }
    }

    fn policy(&self, id: &DataId) -> &dyn FetchPolicy {
        match id {
            DataId::ReceiptProof { .. } => &self.receipt_proofs,
        }
    }
}

/// Fans out per-block queries over every policy; dispatches per-id calls to the policy
/// of `id`'s data type.
impl FetchPolicy for FetchPolicies {
    fn needed_items(&self, block: &BlockHeader) -> Result<Vec<(DataId, Vec<AccountId>)>, Error> {
        self.receipt_proofs.needed_items(block)
    }

    fn is_done(&self, id: &DataId) -> bool {
        self.policy(id).is_done(id)
    }

    fn made_pullable_by(&self, block: &Block) -> Result<Vec<DataId>, Error> {
        self.receipt_proofs.made_pullable_by(block)
    }

    fn producers(&self, id: &DataId) -> Result<Vec<AccountId>, Error> {
        self.policy(id).producers(id)
    }
}
