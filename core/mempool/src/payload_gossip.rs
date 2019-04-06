use std::convert::TryFrom;
use std::sync::Arc;

use near_protos::nightshade as nightshade_proto;
use primitives::chain::ChainPayload;
use primitives::crypto::signature::Signature;
use primitives::crypto::signer::EDSigner;
use primitives::hash::hash_struct;
use primitives::types::{AuthorityId, BlockIndex};
use primitives::utils::proto_to_type;
use protobuf::SingularPtrField;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct PayloadGossip {
    pub sender_id: AuthorityId,
    pub receiver_id: AuthorityId,
    pub payload: ChainPayload,
    pub block_index: BlockIndex,
    signature: Signature,
}

impl TryFrom<nightshade_proto::PayloadGossip> for PayloadGossip {
    type Error = String;

    fn try_from(proto: nightshade_proto::PayloadGossip) -> Result<Self, Self::Error> {
        let payload = proto_to_type(proto.payload)?;
        Ok(PayloadGossip {
            sender_id: proto.sender_id as AuthorityId,
            receiver_id: proto.receiver_id as AuthorityId,
            payload,
            block_index: proto.block_index,
            signature: Signature::try_from(proto.signature.as_str())?,
        })
    }
}

impl From<PayloadGossip> for nightshade_proto::PayloadGossip {
    fn from(gossip: PayloadGossip) -> Self {
        nightshade_proto::PayloadGossip {
            sender_id: gossip.sender_id as u64,
            receiver_id: gossip.receiver_id as u64,
            payload: SingularPtrField::some(gossip.payload.into()),
            block_index: gossip.block_index,
            signature: gossip.signature.to_string(),
            ..Default::default()
        }
    }
}

impl PayloadGossip {
    pub fn new(
        block_index: BlockIndex,
        sender_id: AuthorityId,
        receiver_id: AuthorityId,
        payload: ChainPayload,
        signer: Arc<EDSigner>,
    ) -> Self {
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
