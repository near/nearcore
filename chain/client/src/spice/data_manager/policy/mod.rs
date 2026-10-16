mod receipt_proof;

use super::{DataId, SpiceData};
use near_chain::{Block, Error};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::types::AccountId;
use near_store::adapter::chain_store::ChainStoreAdapter;
use receipt_proof::ReceiptProofPolicy;
use std::collections::HashSet;
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

/// A data type's chain-dependent properties for a node serving it: who produces and who
/// receives the data, and where it is stored.
pub(crate) trait ServePolicy {
    /// Whether every shard `id` names is in the layout it is read against.
    fn names_known_shards(&self, id: &DataId) -> Result<bool, Error>;

    /// The producers of `id`, in the parts' encoding order.
    fn producers(&self, id: &DataId) -> Result<Vec<AccountId>, Error>;

    /// The nodes the push of `id` goes to; none of them is a producer.
    fn recipients(&self, id: &DataId) -> Result<HashSet<AccountId>, Error>;

    /// The data `id` names, if this node has it in the store.
    fn stored_data(&self, id: &DataId) -> Option<SpiceData>;
}

/// The per-data-type policies, one per [`DataId`] variant.
pub(crate) struct Policies {
    receipt_proofs: ReceiptProofPolicy,
}

impl Policies {
    pub(crate) fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
    ) -> Self {
        Self { receipt_proofs: ReceiptProofPolicy::new(chain_store, epoch_manager, shard_tracker) }
    }

    fn fetch_policy(&self, id: &DataId) -> &dyn FetchPolicy {
        match id {
            DataId::ReceiptProof { .. } => &self.receipt_proofs,
        }
    }

    fn serve_policy(&self, id: &DataId) -> &dyn ServePolicy {
        match id {
            DataId::ReceiptProof { .. } => &self.receipt_proofs,
        }
    }
}

/// Fans out per-block queries over every policy; dispatches per-id calls to the policy
/// of `id`'s data type.
impl FetchPolicy for Policies {
    fn needed_items(&self, block: &BlockHeader) -> Result<Vec<(DataId, Vec<AccountId>)>, Error> {
        self.receipt_proofs.needed_items(block)
    }

    fn is_done(&self, id: &DataId) -> bool {
        self.fetch_policy(id).is_done(id)
    }

    fn made_pullable_by(&self, block: &Block) -> Result<Vec<DataId>, Error> {
        self.receipt_proofs.made_pullable_by(block)
    }

    fn producers(&self, id: &DataId) -> Result<Vec<AccountId>, Error> {
        self.fetch_policy(id).producers(id)
    }
}

/// Dispatches every call to the policy of `id`'s data type.
impl ServePolicy for Policies {
    fn names_known_shards(&self, id: &DataId) -> Result<bool, Error> {
        self.serve_policy(id).names_known_shards(id)
    }

    fn producers(&self, id: &DataId) -> Result<Vec<AccountId>, Error> {
        self.serve_policy(id).producers(id)
    }

    fn recipients(&self, id: &DataId) -> Result<HashSet<AccountId>, Error> {
        self.serve_policy(id).recipients(id)
    }

    fn stored_data(&self, id: &DataId) -> Option<SpiceData> {
        self.serve_policy(id).stored_data(id)
    }
}
