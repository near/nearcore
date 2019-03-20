use std::sync::Arc;

use primitives::types::{AuthorityId, BlockIndex};
use primitives::chain::ChainPayload;
use primitives::signature::Signature;
use primitives::hash::hash_struct;
use primitives::signer::BlockSigner;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PayloadGossip {
    pub sender_id: AuthorityId,
    pub receiver_id: AuthorityId,
    pub payload: ChainPayload,
    pub block_index: BlockIndex,
    signature: Signature,
}

impl PayloadGossip {
    pub fn new(block_index: BlockIndex, sender_id: AuthorityId, receiver_id: AuthorityId, payload: ChainPayload, signer: Arc<BlockSigner>) -> Self {
        let hash = hash_struct(&(receiver_id, &payload));
        PayloadGossip {
            block_index,
            sender_id,
            receiver_id,
            payload,
            signature: signer.sign(hash.as_ref()),
        }
    }
}
