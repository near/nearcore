use crate::challenge::PartialState;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use near_schema_checker_lib::ProtocolSchema;

/// Stored on disk for each chunk, including missing chunks, in order to
/// produce a chunk state witness when needed.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct StoredChunkStateTransitionData {
    /// The partial state that is needed to apply the state transition,
    /// whether it is a new chunk state transition or a implicit missing chunk
    /// state transition.
    pub base_state: PartialState,
    /// If this is a new chunk state transition, the hash of the receipts that
    /// were used to apply the state transition. This is redundant information,
    /// but is used to validate against `StateChunkWitness::exact_receipts_hash`
    /// to ease debugging of why a state witness may be incorrect.
    pub receipts_hash: CryptoHash,
}
