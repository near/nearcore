//! Test-only stub for SPICE receipt distribution.
//!
//! Under SPICE, receipts produced by chunk execution are distributed via a
//! T2 pull mechanism that PR 2/3 will introduce. PR 1 stubs that pull: when
//! the executor reports `MissingReceipts`, the stub copies receipt proofs
//! from another node's store, verifies them against the local certified
//! `ChunkExecutionResult`'s `outgoing_receipts_root`, persists them, and
//! signals the executor to retry.
//!
//! Each test-loop node has its own stub holding handles to the other nodes'
//! stores. Cross-node store access is a test-only convenience; production code
//! never reads peer stores directly.
use near_async::messaging::Sender;
use near_client::chunk_executor_actor::{
    ReceiptProofsAvailable, SpiceReceiptStubAdapter, get_receipt_proofs_for_shard,
    receipt_proof_exists, save_receipt_proof,
};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{ChunkExecutionResult, ShardId};
use near_primitives::utils::get_execution_results_key;
use near_store::DBCol;
use near_store::Store;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

/// Total number of receipt proofs the stub has saved across all nodes in the
/// current test process. Tests can read this to confirm the stub actually fired.
pub static SPICE_STUB_PROOFS_SAVED: AtomicU64 = AtomicU64::new(0);

pub struct SpiceReceiptStub {
    own_store: Store,
    peer_stores: Vec<Store>,
    executor_sender: Sender<ReceiptProofsAvailable>,
}

impl std::fmt::Debug for SpiceReceiptStub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpiceReceiptStub")
            .field("peer_stores_count", &self.peer_stores.len())
            .finish()
    }
}

impl SpiceReceiptStub {
    pub fn new(
        own_store: Store,
        peer_stores: Vec<Store>,
        executor_sender: Sender<ReceiptProofsAvailable>,
    ) -> Self {
        Self { own_store, peer_stores, executor_sender }
    }

    /// Returns true iff at least one new receipt proof was persisted (and an
    /// executor retry was scheduled). False indicates "nothing useful from
    /// peers yet", not a failure.
    fn satisfy_inner(&self, block_hash: CryptoHash, to_shard_id: ShardId) -> bool {
        let mut store_update = self.own_store.store_update();
        let mut newly_saved: u64 = 0;
        // Track shards we've already staged in this transaction — multiple
        // peers may have the same proof, but a single StoreUpdate can't
        // contain the same key twice.
        let mut staged_from_shards: HashSet<ShardId> = HashSet::new();
        for peer_store in &self.peer_stores {
            let peer_proofs = get_receipt_proofs_for_shard(peer_store, &block_hash, to_shard_id);
            for proof in peer_proofs {
                let from_shard_id = proof.1.from_shard_id;
                if staged_from_shards.contains(&from_shard_id) {
                    continue;
                }
                if receipt_proof_exists(&self.own_store, &block_hash, to_shard_id, from_shard_id) {
                    continue;
                }
                let key = get_execution_results_key(&block_hash, from_shard_id);
                let Some(execution_result): Option<ChunkExecutionResult> =
                    self.own_store.get_ser(DBCol::execution_results(), &key)
                else {
                    // We don't have a CER for the source shard yet — can't
                    // verify. Skip; we'll be called again later.
                    continue;
                };
                assert!(
                    proof.verify_against_receipt_root(execution_result.outgoing_receipts_root),
                    "spice_receipt_stub: receipt proof verification failed for \
                     ({block_hash}, from={from_shard_id}, to={to_shard_id}); \
                     test fixture inconsistency",
                );
                save_receipt_proof(&mut store_update, &block_hash, &proof);
                staged_from_shards.insert(from_shard_id);
                newly_saved += 1;
            }
        }
        if newly_saved == 0 {
            return false;
        }
        store_update.commit();
        SPICE_STUB_PROOFS_SAVED.fetch_add(newly_saved, Ordering::Relaxed);
        self.executor_sender.send(ReceiptProofsAvailable { block_hash, to_shard_id });
        true
    }
}

impl SpiceReceiptStubAdapter for SpiceReceiptStub {
    fn satisfy(&self, block_hash: CryptoHash, to_shard_id: ShardId) {
        if !self.satisfy_inner(block_hash, to_shard_id) {
            tracing::debug!(
                target: "spice_receipt_stub",
                %block_hash,
                %to_shard_id,
                "no new peer receipts available yet",
            );
        }
    }
}
