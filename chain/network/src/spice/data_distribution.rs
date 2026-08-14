use crate::recv_permit::RecvMessagePermit;
use near_async::messaging::Sender;
use near_async::{MultiSend, MultiSenderFrom};
use near_primitives::spice::partial_data::{SpiceDataIdentifier, SpicePartialData};
use near_primitives::stateless_validation::contract_distribution::{
    SpiceChunkContractAccesses, SpiceContractCodeRequest, SpiceContractCodeResponse,
};
use near_primitives::types::AccountId;
use near_schema_checker_lib::ProtocolSchema;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug)]
pub struct SpiceIncomingPartialData {
    pub data: SpicePartialData,
    pub recv_permit: RecvMessagePermit,
}

/// One `entry` costs that many rate limit tokens (apart from included ordinals that would each cost 1).
/// A bit random number to distinguish serving an entry with 10 ordinals from serving 10 entries with 1 ordinal each,
/// that would require 10 re-encodes, but still have a single token.
const ENTRY_TOKEN_COST: u32 = 16;

/// Request for spice data. Batched: several items wanted from the same peer travel in one
/// message under a single requester. Each entry names the part ordinals it wants and is
/// served or skipped independently.
#[derive(
    Debug, Clone, PartialEq, Eq, borsh::BorshSerialize, borsh::BorshDeserialize, ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum SpiceDataRequest {
    V1(SpiceDataRequestV1) = 0,
}

#[derive(
    Debug, Clone, PartialEq, Eq, borsh::BorshSerialize, borsh::BorshDeserialize, ProtocolSchema,
)]
pub struct SpiceDataRequestV1 {
    wants: BTreeMap<SpiceDataIdentifier, BTreeSet<u64>>,
    requester: AccountId,
}

impl SpiceDataRequest {
    pub fn new(wants: BTreeMap<SpiceDataIdentifier, BTreeSet<u64>>, requester: AccountId) -> Self {
        Self::V1(SpiceDataRequestV1 { wants, requester })
    }

    /// Rate limit tokens this request costs. Saturates: this is read before any cap applies.
    pub fn token_cost(&self) -> u32 {
        match self {
            // Starts at 1 so an empty request is not free.
            Self::V1(request) => request.wants.iter().fold(1u32, |total, (_, ordinals)| {
                total.saturating_add(ENTRY_TOKEN_COST).saturating_add(ordinals.len() as u32)
            }),
        }
    }

    pub fn into_parts(self) -> (BTreeMap<SpiceDataIdentifier, BTreeSet<u64>>, AccountId) {
        match self {
            Self::V1(request) => (request.wants, request.requester),
        }
    }
}

#[derive(Debug)]
pub struct SpiceDataRequestMessage {
    pub request: SpiceDataRequest,
    pub recv_permit: RecvMessagePermit,
}

#[derive(Debug)]
pub struct SpiceChunkContractAccessesMessage(pub SpiceChunkContractAccesses, pub RecvMessagePermit);

#[derive(Debug)]
pub struct SpiceContractCodeRequestMessage(pub SpiceContractCodeRequest, pub RecvMessagePermit);

#[derive(Debug)]
pub struct SpiceContractCodeResponseMessage(pub SpiceContractCodeResponse, pub RecvMessagePermit);

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct SpiceDataDistributorSenderForNetwork {
    pub incoming: Sender<SpiceIncomingPartialData>,
    pub request: Sender<SpiceDataRequestMessage>,
    pub contract_accesses: Sender<SpiceChunkContractAccessesMessage>,
    pub contract_code_request: Sender<SpiceContractCodeRequestMessage>,
    pub contract_code_response: Sender<SpiceContractCodeResponseMessage>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::ShardId;
    use std::str::FromStr as _;

    #[test]
    fn test_data_request_borsh_round_trip() {
        let request = SpiceDataRequest::new(
            BTreeMap::from([
                (
                    SpiceDataIdentifier::Witness {
                        block_hash: CryptoHash::hash_bytes(&[1]),
                        shard_id: ShardId::new(3),
                    },
                    BTreeSet::from([0, 5, 7]),
                ),
                (
                    SpiceDataIdentifier::ReceiptProof {
                        block_hash: CryptoHash::hash_bytes(&[2]),
                        from_shard_id: ShardId::new(1),
                        to_shard_id: ShardId::new(2),
                    },
                    BTreeSet::from([4]),
                ),
            ]),
            AccountId::from_str("requester.near").unwrap(),
        );

        let bytes = borsh::to_vec(&request).unwrap();
        assert_eq!(bytes[0], 0, "V1 discriminant");
        assert_eq!(borsh::from_slice::<SpiceDataRequest>(&bytes).unwrap(), request);
    }
}
