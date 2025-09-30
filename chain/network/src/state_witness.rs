use near_async::messaging::Sender;
use near_async::{Message, MultiSend, MultiSendMessage, MultiSenderFrom};
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractAccesses, ContractCodeRequest, ContractCodeResponse, PartialEncodedContractDeploys,
};
use near_primitives::stateless_validation::partial_witness::PartialEncodedStateWitness;
use near_primitives::stateless_validation::state_witness::ChunkStateWitnessAck;

#[derive(Message, Clone, Debug, PartialEq, Eq)]
pub struct ChunkStateWitnessAckMessage(pub ChunkStateWitnessAck);

#[derive(Message, Clone, Debug, PartialEq, Eq)]
pub struct PartialEncodedStateWitnessMessage(pub PartialEncodedStateWitness);

#[derive(Message, Clone, Debug, PartialEq, Eq)]
pub struct PartialEncodedStateWitnessForwardMessage(pub PartialEncodedStateWitness);

/// Message to partial witness actor (on a chunk validator) that contains code-hashes of
/// the contracts that are accessed when applying the previous chunk.
#[derive(Message, Clone, Debug, PartialEq, Eq)]
pub struct ChunkContractAccessesMessage(pub ChunkContractAccesses);

/// Message to partial witness actor that contains part of code for newly-deployed contracts.
#[derive(Message, Clone, Debug, PartialEq, Eq)]
pub struct PartialEncodedContractDeploysMessage(pub PartialEncodedContractDeploys);

/// Message to partial witness actor (on a chunk producer) that requests contract code
/// by providing hashes of the code.
#[derive(Message, Clone, Debug, PartialEq, Eq)]
pub struct ContractCodeRequestMessage(pub ContractCodeRequest);

/// Message to partial witness actor (on a chunk validator) that provides contract code
/// requested beforehand.
#[derive(Message, Clone, Debug, PartialEq, Eq)]
pub struct ContractCodeResponseMessage(pub ContractCodeResponse);

/// Multi-sender for forwarding messages received from network to PartialWitnessActor.
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
    pub partial_encoded_contract_deploys: Sender<PartialEncodedContractDeploysMessage>,
}
