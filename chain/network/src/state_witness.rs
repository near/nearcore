use near_async::messaging::Sender;
use near_async::{MultiSend, MultiSenderFrom};
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractAccesses, ContractCodeRequest, ContractCodeResponse, PartialEncodedContractDeploys,
};
use near_primitives::stateless_validation::partial_witness::VersionedPartialEncodedStateWitness;
use near_primitives::stateless_validation::state_witness::ChunkStateWitnessAck;

use crate::recv_permit::RecvMessagePermit;

#[derive(Debug)]
pub struct ChunkStateWitnessAckMessage(pub ChunkStateWitnessAck, pub RecvMessagePermit);

#[derive(Debug)]
pub struct PartialEncodedStateWitnessMessage(
    pub VersionedPartialEncodedStateWitness,
    pub RecvMessagePermit,
);

#[derive(Debug)]
pub struct PartialEncodedStateWitnessForwardMessage(
    pub VersionedPartialEncodedStateWitness,
    pub RecvMessagePermit,
);

/// Message to partial witness actor (on a chunk validator) that contains code-hashes of
/// the contracts that are accessed when applying the previous chunk.
#[derive(Debug)]
pub struct ChunkContractAccessesMessage(pub ChunkContractAccesses, pub RecvMessagePermit);

/// Message to partial witness actor that contains part of code for newly-deployed contracts.
#[derive(Debug)]
pub struct PartialEncodedContractDeploysMessage(
    pub PartialEncodedContractDeploys,
    pub RecvMessagePermit,
);

/// Message to partial witness actor (on a chunk producer) that requests contract code
/// by providing hashes of the code.
#[derive(Debug)]
pub struct ContractCodeRequestMessage(pub ContractCodeRequest, pub RecvMessagePermit);

/// Message to partial witness actor (on a chunk validator) that provides contract code
/// requested beforehand.
#[derive(Debug)]
pub struct ContractCodeResponseMessage(pub ContractCodeResponse, pub RecvMessagePermit);

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
