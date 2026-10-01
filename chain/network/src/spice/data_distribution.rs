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

/// Rate limit tokens each entry costs, on top of 1 per requested ordinal. Somewhat arbitrary;
/// chosen to keep 1 entry with 10 ordinals cheaper than 10 entries with 1 ordinal each, which cost
/// 10 re-encodes.
const PER_ENTRY_TOKEN_COST: u32 = 16;

/// Request for spice data, answered with `SpicePartialData`. Batched: several entries wanted from
/// the same peer travel in one message under a single requester. An entry is one `wants` element:
/// a data id plus the part ordinals wanted for it. Each entry is served or skipped independently.
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
    // TODO(spice): Validate the per-entry cost and the SpiceDataRequest bucket against production
    // scale (chunk producers, witness validators, shards per epoch).
    pub fn token_cost(&self) -> u32 {
        match self {
            // Starts at 1 so an empty request is not free.
            Self::V1(request) => request.wants.iter().fold(1u32, |total, (_, ordinals)| {
                total.saturating_add(PER_ENTRY_TOKEN_COST).saturating_add(ordinals.len() as u32)
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
