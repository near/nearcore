use crate::types::RuntimeAdapter;
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext, WithSpanContextExt};
use near_primitives::hash::CryptoHash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct StateSnapshotActor {
    in_progress: Arc<AtomicBool>,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
}

impl StateSnapshotActor {
    pub fn new(in_progress: Arc<AtomicBool>, runtime_adapter: Arc<dyn RuntimeAdapter>) -> Self {
        Self { in_progress, runtime_adapter }
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

        assert!(
            !self.in_progress.swap(true, Ordering::Relaxed),
            "Tried to start a state snapshot while state snapshotting"
        );
        if let Err(err) = self.runtime_adapter.make_state_snapshot(&prev_block_hash) {
            tracing::error!(target: "state_snapshot", ?err, "State snapshot creation failed");
        }
        assert!(
            self.in_progress.swap(false, Ordering::Relaxed),
            "Tried to stop a state snapshot which wasn't running"
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
