//! The adversarial knobs of the chunk executor, for tests. The knobs' state is plain data
//! the executor carries whatever the features, so the structs that hold it need no
//! gating; everything that acts on it is compiled under `test_features` only, in the one
//! module at the bottom.

use super::ChunkExecutorActor;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// How an adversarial chunk producer tampers with the boundary witness it distributes,
/// in the spots the unit tests of `boundary_chunk_validation.rs` tamper. The producer
/// still sends its receipts and endorses its own honest result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AdvBoundaryWitnessMode {
    /// Every implicit transition claims a post state root other than the replay's.
    /// Changes nothing when the witness carries no implicit transition, which it does
    /// only when the boundary block lacks the shard's chunk.
    WrongPostState,
    /// One source receipt proof is left out.
    DropReceiptProof,
    /// The applied receipts hash is replaced.
    WrongReceiptsHash,
    /// The implicit transitions are left out. Changes nothing when there are none.
    MissingImplicitTransition,
}

/// Makes this node's chunk executor tamper with every boundary witness it distributes
/// from now on. The test loop sends it to the executor directly; it is handled under
/// `test_features` only.
#[derive(Debug)]
pub struct AdvSpiceBoundaryWitness(pub AdvBoundaryWitnessMode);

/// The knobs' state, shared by the coordinator, which sets it, and the per-shard
/// executors, which read it.
#[derive(Default)]
#[cfg_attr(not(feature = "test_features"), allow(dead_code))]
pub(crate) struct AdversarialState {
    boundary_witness: Mutex<Option<AdvBoundaryWitnessMode>>,
    boundary_witnesses_tampered: AtomicU64,
}

impl ChunkExecutorActor {
    /// How many boundary witnesses the adversarial mode changed before they went out.
    pub fn adversarial_boundary_witnesses_tampered(&self) -> u64 {
        self.adversarial.boundary_witnesses_tampered.load(Ordering::Relaxed)
    }
}

#[cfg(feature = "test_features")]
mod knobs {
    use super::{AdvBoundaryWitnessMode, AdvSpiceBoundaryWitness, AdversarialState};
    use crate::spice::chunk_executor_actor::ChunkExecutorActor;
    use near_async::messaging::Handler;
    use near_chain::Block;
    use near_primitives::hash::CryptoHash;
    use near_primitives::spice::state_witness::SpiceBoundaryChunkStateWitness;
    use near_primitives::types::ShardId;
    use std::sync::atomic::Ordering;

    impl Handler<AdvSpiceBoundaryWitness> for ChunkExecutorActor {
        fn handle(&mut self, AdvSpiceBoundaryWitness(mode): AdvSpiceBoundaryWitness) {
            tracing::info!(target: "adversary", ?mode, "setting adversary boundary witness");
            *self.adversarial.boundary_witness.lock() = Some(mode);
        }
    }

    impl AdversarialState {
        /// `witness` tampered with as the mode in force says, counted; `witness` as built
        /// when no mode is in force.
        pub(crate) fn tampered_boundary_witness(
            &self,
            mut witness: SpiceBoundaryChunkStateWitness,
            block: &Block,
            shard_id: ShardId,
        ) -> SpiceBoundaryChunkStateWitness {
            let Some(mode) = *self.boundary_witness.lock() else {
                return witness;
            };
            if tamper_boundary_witness(&mut witness, mode) {
                self.boundary_witnesses_tampered.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(target: "adversary", ?mode, block_hash = %block.hash(), %shard_id, "distributing a tampered boundary witness");
            } else {
                tracing::warn!(target: "adversary", ?mode, block_hash = %block.hash(), %shard_id, "the adversarial mode changes nothing in this boundary witness, distributing it as built");
            }
            witness
        }
    }

    /// Applies `mode` to `witness`, returning whether anything changed.
    fn tamper_boundary_witness(
        witness: &mut SpiceBoundaryChunkStateWitness,
        mode: AdvBoundaryWitnessMode,
    ) -> bool {
        match mode {
            AdvBoundaryWitnessMode::WrongPostState => {
                for transition in &mut witness.implicit_transitions {
                    transition.post_state_root =
                        CryptoHash::hash_borsh(&transition.post_state_root);
                }
                !witness.implicit_transitions.is_empty()
            }
            AdvBoundaryWitnessMode::DropReceiptProof => {
                // The smallest key, so the choice does not depend on the map's order.
                let Some(chunk_hash) =
                    witness.source_receipt_proofs.keys().min_by_key(|hash| hash.0).cloned()
                else {
                    return false;
                };
                witness.source_receipt_proofs.remove(&chunk_hash);
                true
            }
            AdvBoundaryWitnessMode::WrongReceiptsHash => {
                witness.applied_receipts_hash =
                    CryptoHash::hash_borsh(&witness.applied_receipts_hash);
                true
            }
            AdvBoundaryWitnessMode::MissingImplicitTransition => {
                let had_transitions = !witness.implicit_transitions.is_empty();
                witness.implicit_transitions.clear();
                had_transitions
            }
        }
    }
}
