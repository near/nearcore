use crate::recv_permit::RecvMessagePermit;
use near_async::messaging::Sender;
use near_async::{MultiSend, MultiSenderFrom};
use near_primitives::spice::partial_data::{SpiceDataIdentifier, SpicePartialData};
use near_primitives::stateless_validation::contract_distribution::{
    SpiceChunkContractAccesses, SpiceContractCodeRequest, SpiceContractCodeResponse,
};
use near_primitives::types::AccountId;

#[derive(Debug)]
pub struct SpiceIncomingPartialData {
    pub data: SpicePartialData,
    pub recv_permit: RecvMessagePermit,
}

#[derive(Debug, Clone, PartialEq, Eq, borsh::BorshSerialize, borsh::BorshDeserialize)]
pub struct SpicePartialDataRequest {
    pub data_id: SpiceDataIdentifier,
    pub requester: AccountId,
}

#[derive(Debug)]
pub struct SpicePartialDataRequestMessage {
    pub request: SpicePartialDataRequest,
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
    pub request: Sender<SpicePartialDataRequestMessage>,
    pub contract_accesses: Sender<SpiceChunkContractAccessesMessage>,
    pub contract_code_request: Sender<SpiceContractCodeRequestMessage>,
    pub contract_code_response: Sender<SpiceContractCodeResponseMessage>,
}
