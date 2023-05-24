use crate::types::RuntimeAdapter;
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext, WithSpanContextExt};
use near_primitives::hash::CryptoHash;
use near_store::flat::FlatStorageManager;
use std::sync::Arc;

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
struct StartSnapshotRequest {
    prev_block_hash: CryptoHash,
}

impl actix::Handler<WithSpanContext<StartSnapshotRequest>> for StateSnapshotActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<StartSnapshotRequest>,
        _: &mut actix::Context<Self>,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "state_snapshot", msg);
        let StartSnapshotRequest { prev_block_hash } = msg;

        // TODO(nikurt): Add `set_flat_state_updates_mode()` to the trait `RuntimeAdapter`.
        assert_ne!(
            self.flat_storage_manager.set_flat_state_updates_mode(false),
            Some(false),
            "Failed to lock flat state updates"
        );
        if let Err(err) = self.runtime_adapter.make_state_snapshot(&prev_block_hash) {
            tracing::error!(target: "state_snapshot", ?err, "State snapshot creation failed");
        }
        assert_ne!(
            self.flat_storage_manager.set_flat_state_updates_mode(true),
            Some(true),
            "Failed to unlock flat state updates"
        );
    }
}

pub type StartSnapshotCallback = Arc<dyn Fn(CryptoHash) -> () + Send + Sync + 'static>;

pub fn get_start_snapshot_callback(
    state_snapshot_addr: Arc<actix::Addr<StateSnapshotActor>>,
) -> StartSnapshotCallback {
    Arc::new(move |prev_block_hash| {
        tracing::info!(target: "state_snapshot", ?prev_block_hash, "start_snapshot_callback sends `StartSnapshotCallback` to state_snapshot_addr");
        state_snapshot_addr.do_send(StartSnapshotRequest { prev_block_hash }.with_span_context());
    })
}
