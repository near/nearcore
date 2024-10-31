use crate::challenge::PartialState;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use near_schema_checker_lib::ProtocolSchema;

use super::contract_distribution::{CodeBytes, CodeHash};

/// Stored on disk for each chunk, including missing chunks, in order to
/// produce a chunk state witness when needed.
// TODO(#11099): Implement migration to combine these into single version
// after the feature of excluding contracts from state witness is stabilized.
#[derive(Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum StoredChunkStateTransitionData {
    V1(StoredChunkStateTransitionDataV1),
    V2(StoredChunkStateTransitionDataV2),
    V3(StoredChunkStateTransitionDataV3),
}

#[derive(Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct StoredChunkStateTransitionDataV1 {
    /// The partial state that is needed to apply the state transition,
    /// whether it is a new chunk state transition or a implicit missing chunk
    /// state transition.
    pub base_state: PartialState,
    /// If this is a new chunk state transition, the hash of the receipts that
    /// were used to apply the state transition. This is redundant information,
    /// but is used to validate against `StateChunkWitness::exact_receipts_hash`
    /// to ease debugging of why a state witness may be incorrect.
    pub receipts_hash: CryptoHash,
    /// The code-hashes of the contracts that are accessed (called) during the state transition.
    pub contract_accesses: Vec<CodeHash>,
}

#[derive(Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct StoredChunkStateTransitionDataV2 {
    /// The partial state that is needed to apply the state transition,
    /// whether it is a new chunk state transition or a implicit missing chunk
    /// state transition.
    pub base_state: PartialState,
    /// If this is a new chunk state transition, the hash of the receipts that
    /// were used to apply the state transition. This is redundant information,
    /// but is used to validate against `StateChunkWitness::exact_receipts_hash`
    /// to ease debugging of why a state witness may be incorrect.
    pub receipts_hash: CryptoHash,
    /// The code-hashes of the contracts that are accessed (called) during the state transition.
    pub contract_accesses: Vec<CodeHash>,
    /// The code-hashes of the contracts that are deployed during the state transition.
    pub contract_deploys: Vec<CodeHash>,
}

#[derive(Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct StoredChunkStateTransitionDataV3 {
    /// The partial state that is needed to apply the state transition,
    /// whether it is a new chunk state transition or a implicit missing chunk
    /// state transition.
    pub base_state: PartialState,
    /// If this is a new chunk state transition, the hash of the receipts that
    /// were used to apply the state transition. This is redundant information,
    /// but is used to validate against `StateChunkWitness::exact_receipts_hash`
    /// to ease debugging of why a state witness may be incorrect.
    pub receipts_hash: CryptoHash,
    /// The code-hashes of the contracts that are accessed (called) during the state transition.
    pub contract_accesses: Vec<CodeHash>,
    /// Contracts that are deployed during the state transition.
    pub contract_deploys: Vec<CodeBytes>,
}
