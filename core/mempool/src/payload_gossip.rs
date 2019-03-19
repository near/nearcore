use std::sync::Arc;

use primitives::types::AuthorityId;
use primitives::chain::ChainPayload;
use primitives::signature::Signature;
use primitives::hash::hash_struct;
use primitives::signer::BlockSigner;
use near_protos::nightshade as nightshade_proto;
use protobuf::SingularPtrField;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PayloadGossip {
    pub sender_id: AuthorityId,
    pub receiver_id: AuthorityId,
    pub payload: ChainPayload,
    signature: Signature,
}

impl From<nightshade_proto::PayloadGossip> for PayloadGossip {
    fn from(proto: nightshade_proto::PayloadGossip) -> Self {
        PayloadGossip {
            sender_id: proto.sender_id as AuthorityId,
            receiver_id: proto.receiver_id as AuthorityId,
            payload: proto.payload.unwrap().into(),
            signature: Signature::from(&proto.signature)
        }
    }
}

impl From<PayloadGossip> for nightshade_proto::PayloadGossip {
    fn from(gossip: PayloadGossip) -> Self {
        nightshade_proto::PayloadGossip {
            sender_id: gossip.sender_id as u64,
            receiver_id: gossip.receiver_id as u64,
            payload: SingularPtrField::some(gossip.payload.into()),
            signature: gossip.signature.to_string(),
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }
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
