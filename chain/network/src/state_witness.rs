use near_async::messaging::Sender;
use near_async::{MultiSend, MultiSendMessage, MultiSenderFrom};
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractAccesses, ContractCodeRequest, ContractCodeResponse,
};
use near_primitives::stateless_validation::partial_witness::PartialEncodedStateWitness;
use near_primitives::stateless_validation::state_witness::ChunkStateWitnessAck;

#[derive(actix::Message, Clone, Debug, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct ChunkStateWitnessAckMessage(pub ChunkStateWitnessAck);

#[derive(actix::Message, Clone, Debug, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct PartialEncodedStateWitnessMessage(pub PartialEncodedStateWitness);

#[derive(actix::Message, Clone, Debug, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct PartialEncodedStateWitnessForwardMessage(pub PartialEncodedStateWitness);

#[derive(actix::Message, Clone, Debug, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct ChunkContractAccessesMessage(pub ChunkContractAccesses);

#[derive(actix::Message, Clone, Debug, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct ContractCodeRequestMessage(pub ContractCodeRequest);

// TODO(#11099): Use the compressed form of the message.
#[derive(actix::Message, Clone, Debug, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct ContractCodeResponseMessage(pub ContractCodeResponse);

#[derive(Clone, MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
#[multi_send_input_derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialWitnessSenderForNetwork {
    pub chunk_state_witness_ack: Sender<ChunkStateWitnessAckMessage>,
    pub partial_encoded_state_witness: Sender<PartialEncodedStateWitnessMessage>,
    pub partial_encoded_state_witness_forward: Sender<PartialEncodedStateWitnessForwardMessage>,
    pub chunk_contract_accesses: Sender<ChunkContractAccessesMessage>,
    pub contract_code_request: Sender<ContractCodeRequestMessage>,
    pub contract_code_response: Sender<ContractCodeResponseMessage>,
}
