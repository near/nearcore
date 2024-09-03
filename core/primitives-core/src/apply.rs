use std::fmt::{Display, Formatter};

/// Indicates the phase of block production the apply-chunk operation is invoked.
/// This is currently used to analyze and compare the metrics collected while applying the receipts
/// for different purposes, eg. while updating state of a tracked shard vs. validating a chunk.
/// TODO: Consider combining ApplyChunkReason, ApplyChunkBlockContext, and ApplyChunkBlockContext
/// under a common wrapper struct such as ApplyChunkContext.
#[derive(Clone, Debug)]
pub enum ApplyChunkReason {
    /// Apply-chunk is invoked to update the state of a shards being tracked.
    UpdateTrackedShard,
    /// Apply-chunk is invoked to validate the state witness for a shard in the context of stateless validation.
    ValidateChunkStateWitness,
    Experiment,
}

impl ApplyChunkReason {
    /// Returns a static, short string representation of the reason, to be used for metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            ApplyChunkReason::UpdateTrackedShard => "update_shard",
            ApplyChunkReason::ValidateChunkStateWitness => "validate_chunk",
            ApplyChunkReason::Experiment => "experiment",
        }
    }
}

impl Display for ApplyChunkReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(self.as_str())
    }
}
