use near_async::messaging::Sender;
use near_async::{MultiSend, MultiSenderFrom};
use near_primitives::spice_partial_data::{SpiceDataIdentifier, SpicePartialData};
use near_primitives::stateless_validation::contract_distribution::{
    SpiceChunkContractAccesses, SpiceContractCodeRequest, SpiceContractCodeResponse,
};
use near_primitives::types::AccountId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiceIncomingPartialData {
    pub data: SpicePartialData,
}

#[derive(Debug, Clone, PartialEq, Eq, borsh::BorshSerialize, borsh::BorshDeserialize)]
pub struct SpicePartialDataRequest {
    pub data_id: SpiceDataIdentifier,
    pub requester: AccountId,
    /// When true, the producer should also send contract accesses alongside the
    /// witness data. Set by chunk validators that need accesses to validate
    /// (e.g. after catching up on missed blocks).
    pub include_contract_accesses: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiceChunkContractAccessesMessage(pub SpiceChunkContractAccesses);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiceContractCodeRequestMessage(pub SpiceContractCodeRequest);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiceContractCodeResponseMessage(pub SpiceContractCodeResponse);

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct SpiceDataDistributorSenderForNetwork {
    pub incoming: Sender<SpiceIncomingPartialData>,
    pub request: Sender<SpicePartialDataRequest>,
    pub contract_accesses: Sender<SpiceChunkContractAccessesMessage>,
    pub contract_code_request: Sender<SpiceContractCodeRequestMessage>,
    pub contract_code_response: Sender<SpiceContractCodeResponseMessage>,
}
