use near_async::messaging::Sender;
use near_async::{MultiSend, MultiSenderFrom};
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractAccesses, ContractCodeRequest, ContractCodeResponse, PartialEncodedContractDeploys,
};
use near_primitives::stateless_validation::partial_witness::VersionedPartialEncodedStateWitness;
use near_primitives::stateless_validation::state_witness::ChunkStateWitnessAck;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkStateWitnessAckMessage(pub ChunkStateWitnessAck);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialEncodedStateWitnessMessage(pub VersionedPartialEncodedStateWitness);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialEncodedStateWitnessForwardMessage(pub VersionedPartialEncodedStateWitness);

/// Message to partial witness actor (on a chunk validator) that contains code-hashes of
/// the contracts that are accessed when applying the previous chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkContractAccessesMessage(pub ChunkContractAccesses);

/// Message to partial witness actor that contains part of code for newly-deployed contracts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialEncodedContractDeploysMessage(pub PartialEncodedContractDeploys);

/// Message to partial witness actor (on a chunk producer) that requests contract code
/// by providing hashes of the code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContractCodeRequestMessage(pub ContractCodeRequest);

/// Message to partial witness actor (on a chunk validator) that provides contract code
/// requested beforehand.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContractCodeResponseMessage(pub ContractCodeResponse);

/// Multi-sender for forwarding messages received from network to PartialWitnessActor.
#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct PartialWitnessSenderForNetwork {
    pub chunk_state_witness_ack: Sender<ChunkStateWitnessAckMessage>,
    pub partial_encoded_state_witness: Sender<PartialEncodedStateWitnessMessage>,
    pub partial_encoded_state_witness_forward: Sender<PartialEncodedStateWitnessForwardMessage>,
    pub chunk_contract_accesses: Sender<ChunkContractAccessesMessage>,
    pub contract_code_request: Sender<ContractCodeRequestMessage>,
    pub contract_code_response: Sender<ContractCodeResponseMessage>,
    pub partial_encoded_contract_deploys: Sender<PartialEncodedContractDeploysMessage>,
}
