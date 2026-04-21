#[cfg(feature = "sandbox")]
pub mod state_patch {
    use crate::state_record::StateRecord;

    /// Changes to the state to be applied via sandbox-only state patching
    /// feature.
    ///
    /// On non-sandbox builds this struct is a ZST whose methods are no-ops.
    #[derive(Default, Clone)]
    pub struct SandboxStatePatch {
        records: Vec<StateRecord>,
    }

    impl SandboxStatePatch {
        pub fn new(records: Vec<StateRecord>) -> SandboxStatePatch {
            SandboxStatePatch { records }
        }

        pub fn is_empty(&self) -> bool {
            self.records.is_empty()
        }

        pub fn take(&mut self) -> SandboxStatePatch {
            Self { records: core::mem::take(&mut self.records) }
        }

        pub fn merge(&mut self, other: SandboxStatePatch) {
            self.records.extend(other.records);
        }
    }

    impl IntoIterator for SandboxStatePatch {
        type Item = StateRecord;
        type IntoIter = std::vec::IntoIter<StateRecord>;

        fn into_iter(self) -> Self::IntoIter {
            self.records.into_iter()
        }
    }

    /// Tracks sandbox state patches through the block processing pipeline.
    ///
    /// Wraps `SandboxStatePatch` with a generation counter so the RPC can
    /// accurately report when a patch has been committed to the DB (not just
    /// consumed from the pending queue).
    #[derive(Default)]
    pub struct SandboxPatchTracker {
        pending: SandboxStatePatch,
        /// Incremented each time `submit()` is called with a non-empty patch.
        generation: u64,
        /// Advanced to match `generation` after the block carrying the patch
        /// is committed to the DB.
        committed_gen: u64,
    }

    impl SandboxPatchTracker {
        /// Queue a state patch for inclusion in a future block.
        pub fn submit(&mut self, patch: SandboxStatePatch) {
            if patch.is_empty() {
                return;
            }
            self.pending.merge(patch);
            self.generation += 1;
        }

        /// Whether there is a submitted patch that hasn't been committed yet.
        pub fn in_progress(&self) -> bool {
            self.generation != self.committed_gen
        }

        /// Take the pending patch for a shard, but only if the shard has a new
        /// chunk.  The first shard with a new chunk gets the entire patch;
        /// subsequent shards get an empty one.
        pub fn take_for_shard(&mut self, shard_has_new_chunk: bool) -> SandboxStatePatch {
            if shard_has_new_chunk && !self.pending.is_empty() {
                self.pending.take()
            } else {
                SandboxStatePatch::default()
            }
        }

        /// Returns the generation to stamp on `BlockPreprocessInfo`.
        ///
        /// If the pending queue was drained during this block's preprocessing,
        /// returns the current generation so that `mark_committed` will advance
        /// `committed_gen`.  Otherwise returns a sentinel that won't advance.
        pub fn generation_for_block(&self) -> u64 {
            if self.pending.is_empty() && self.generation > self.committed_gen {
                self.generation
            } else {
                self.committed_gen
            }
        }

        /// Called after `chain_update.commit()`.  Advances `committed_gen` if
        /// the block carried a patch whose generation is newer.
        pub fn mark_committed(&mut self, generation: u64) {
            if generation > self.committed_gen {
                self.committed_gen = generation;
            }
        }
    }
}

#[cfg(not(feature = "sandbox"))]
pub mod state_patch {
    use crate::state_record::StateRecord;

    #[derive(Default, Clone)]
    pub struct SandboxStatePatch;

    impl SandboxStatePatch {
        #[inline(always)]
        pub fn is_empty(&self) -> bool {
            true
        }
        #[inline(always)]
        pub fn take(&mut self) -> Self {
            Self
        }
        #[inline(always)]
        pub fn merge(&mut self, _other: SandboxStatePatch) {}
    }

    impl IntoIterator for SandboxStatePatch {
        type Item = StateRecord;
        type IntoIter = std::iter::Empty<StateRecord>;

        #[inline(always)]
        fn into_iter(self) -> Self::IntoIter {
            std::iter::empty()
        }
    }

    #[derive(Default)]
    pub struct SandboxPatchTracker;

    impl SandboxPatchTracker {
        #[inline(always)]
        pub fn submit(&mut self, _patch: SandboxStatePatch) {}
        #[inline(always)]
        pub fn in_progress(&self) -> bool {
            false
        }
        #[inline(always)]
        pub fn take_for_shard(&mut self, _shard_has_new_chunk: bool) -> SandboxStatePatch {
            SandboxStatePatch
        }
        #[inline(always)]
        pub fn generation_for_block(&self) -> u64 {
            0
        }
        #[inline(always)]
        pub fn mark_committed(&mut self, _generation: u64) {}
    }
}
