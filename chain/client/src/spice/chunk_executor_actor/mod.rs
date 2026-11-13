//! SPICE chunk executor. Split into:
//! - [`coordinator`] — the actor that owns the per-shard registry and routing,
//! - [`per_shard`] — the per-tracked-shard apply path,
//! - [`receipt_tracker`] — the per-shard unverified-receipt buffer,
//! - [`storage`] — DB read/write helpers for per-chunk artifacts.

mod coordinator;
mod per_shard;
mod receipt_tracker;
mod storage;
pub mod testonly;

pub use coordinator::{
    ChunkExecutorActor, ExecutorApplyChunksDone, ExecutorIncomingUnverifiedReceipts,
};
pub(crate) use storage::get_contract_accesses;
pub use storage::{get_receipt_proof, get_witness, receipt_proof_exists};
// Re-exported for the test modules under `spice/tests/`.
#[cfg(test)]
pub(crate) use per_shard::is_descendant_of_final_execution_head;
#[cfg(test)]
pub(crate) use storage::{save_receipt_proof, save_witness_and_contract_accesses};
