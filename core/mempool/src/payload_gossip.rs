use std::sync::Arc;

use primitives::types::AuthorityId;
use primitives::chain::ChainPayload;
use primitives::signature::Signature;
use primitives::hash::hash_struct;
use primitives::signer::BlockSigner;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PayloadGossip {
    pub sender_id: AuthorityId,
    pub receiver_id: AuthorityId,
    pub payload: ChainPayload,
    signature: Signature,
}

impl PayloadGossip {
    pub fn new(sender_id: AuthorityId, receiver_id: AuthorityId, payload: ChainPayload, signer: Arc<BlockSigner>) -> Self {
        let hash = hash_struct(&(receiver_id, &payload));
        PayloadGossip {
            sender_id,
            receiver_id,
            payload,
            signature: signer.sign(hash.as_ref()),
        }
    }
}
