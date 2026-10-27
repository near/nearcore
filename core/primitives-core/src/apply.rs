use crate::hash::CryptoHash;
use std::fmt::{Display, Formatter};

/// Indicates the phase of block production the apply-chunk operation is invoked.
/// This is used to differentiate the logics in the chunk application, as well as analyzing and comparing
/// the metrics collected while applying the receipts for different purposes, eg. while updating state of
/// a tracked shard vs. validating a chunk.
/// TODO: Consider combining ApplyChunkReason, ApplyChunkBlockContext, and ApplyChunkBlockContext
/// under a common wrapper struct such as ApplyChunkContext.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyChunkReason {
    /// Apply-chunk is invoked to update the state of a shards being tracked.
    UpdateTrackedShard,
    /// Apply-chunk is invoked to validate the state witness for a shard in the context of stateless validation.
    ValidateChunkStateWitness,
    /// Apply-chunk is invoked to view the state of a tracked shard (eg. calling a function from a specific state).
    ViewTrackedShard,
}

impl ApplyChunkReason {
    /// Returns a static, short string representation of the reason, to be used for metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            ApplyChunkReason::UpdateTrackedShard => "update_shard",
            ApplyChunkReason::ValidateChunkStateWitness => "validate_chunk",
            ApplyChunkReason::ViewTrackedShard => "view_shard",
        }
    }
}

impl Display for ApplyChunkReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(self.as_str())
    }
}

/// The previous block, as the chunk producer saw it when building the chunk.
/// External calls see this block instead of the one the chunk is applied in,
/// so that they run with the same values when the producer runs them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrevBlockContext {
    /// The previous block's timestamp (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    pub timestamp: u64,
    /// The previous block's random value.
    pub random_seed: CryptoHash,
}
