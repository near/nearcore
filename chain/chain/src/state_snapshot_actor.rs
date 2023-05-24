use crate::types::RuntimeAdapter;
use actix::AsyncContext;
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext, WithSpanContextExt};
use near_primitives::hash::CryptoHash;
use near_store::flat::FlatStorageManager;
use std::sync::Arc;

/// Runs tasks related to state snapshots.
pub struct StateSnapshotActor {
    flat_storage_manager: FlatStorageManager,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
}

impl StateSnapshotActor {
    pub fn new(
        flat_storage_manager: FlatStorageManager,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
    ) -> Self {
        Self { flat_storage_manager, runtime_adapter }
    }
}

impl actix::Actor for StateSnapshotActor {
    type Context = actix::Context<Self>;
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
struct MakeSnapshotRequest {
    /// prev_hash of the last processed block.
    prev_block_hash: CryptoHash,
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
struct CompactSnapshotRequest {
    /// Identifies the snapshot.
    prev_block_hash: CryptoHash,
}

/// Makes a state snapshot in the background.
/// I don't know what exactly RocksDB checkpointing mechanism does in the presence of concurrent writes.
/// But FlatStorage is managed separately its state is guaranteed to be consistent.
impl actix::Handler<WithSpanContext<MakeSnapshotRequest>> for StateSnapshotActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<MakeSnapshotRequest>,
        _ctx: &mut actix::Context<Self>,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "state_snapshot", msg);
        let MakeSnapshotRequest { prev_block_hash } = msg;

        // TODO(nikurt): Add `set_flat_state_updates_mode()` to the trait `RuntimeAdapter`.
        assert_ne!(
            self.flat_storage_manager.set_flat_state_updates_mode(false),
            Some(false),
            "Failed to lock flat state updates"
        );
        let run_compaction =
            if let Err(err) = self.runtime_adapter.make_state_snapshot(&prev_block_hash) {
                tracing::error!(target: "state_snapshot", ?err, "State snapshot creation failed");
                false
            } else {
                true
            };
        assert_ne!(
            self.flat_storage_manager.set_flat_state_updates_mode(true),
            Some(true),
            "Failed to unlock flat state updates"
        );
        if run_compaction {
            _ctx.address().do_send(CompactSnapshotRequest { prev_block_hash }.with_span_context());
        }
    }
}

/// Runs compaction of the snapshot store.
impl actix::Handler<WithSpanContext<CompactSnapshotRequest>> for StateSnapshotActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<CompactSnapshotRequest>,
        _ctx: &mut actix::Context<Self>,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "state_snapshot", msg);
        let CompactSnapshotRequest { prev_block_hash } = msg;

        if let Err(err) = self.runtime_adapter.compact_state_snapshot(&prev_block_hash) {
            tracing::error!(target: "state_snapshot", ?prev_block_hash, ?err, "State snapshot compaction failed");
        }
    }
}

pub type MakeSnapshotCallback = Arc<dyn Fn(CryptoHash) -> () + Send + Sync + 'static>;

/// Sends a request to make a state snapshot.
pub fn get_make_snapshot_callback(
    state_snapshot_addr: Arc<actix::Addr<StateSnapshotActor>>,
) -> MakeSnapshotCallback {
    Arc::new(move |prev_block_hash| {
        tracing::info!(target: "state_snapshot", ?prev_block_hash, "start_snapshot_callback sends `MakeSnapshotCallback` to state_snapshot_addr");
        state_snapshot_addr.do_send(MakeSnapshotRequest { prev_block_hash }.with_span_context());
    })
}
