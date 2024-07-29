use near_primitives_core::types::{BlockHeight, ShardId};

use crate::types::EpochId;

pub mod chunk_endorsement;
pub mod partial_witness;
pub mod state_witness;
pub mod stored_chunk_state_transition_data;
pub mod validator_assignment;

/// An arbitrary static string to make sure that this struct cannot be
/// serialized to look identical to another serialized struct. For chunk
/// production we are signing a chunk hash, so we need to make sure that
/// this signature means something different.
///
/// This is a messy workaround until we know what to do with NEP 483.
type SignatureDifferentiator = String;

/// This struct contains combination of fields that uniquely identify chunk production.
/// It means that for a given instance only one chunk could be produced.
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct ChunkProductionKey {
    pub shard_id: ShardId,
    pub epoch_id: EpochId,
    pub height_created: BlockHeight,
}
