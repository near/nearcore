use primitives::types::AuthorityId;
use primitives::chain::ChainPayload;
use primitives::signature::Signature;
use primitives::hash::hash_struct;
use primitives::signature::SecretKey;
use primitives::signature::sign;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TxGossip {
    pub sender_id: AuthorityId,
    pub receiver_id: AuthorityId,
    pub payload: ChainPayload,
    signature: Signature,
}

impl TxGossip {
    pub fn new(sender_id: AuthorityId, receiver_id: AuthorityId, payload: ChainPayload, sk: SecretKey) -> Self {
        let hash = hash_struct(&(receiver_id, &payload));
        TxGossip {
            sender_id,
            receiver_id,
            payload,
            signature: sign(hash.as_ref(), &sk),
        }
    }
}
