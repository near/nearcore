use actix::AsyncContext;
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext, WithSpanContextExt};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_store::flat::FlatStorageManager;
use near_store::ShardTries;
use std::sync::Arc;

/// Runs tasks related to state snapshots.
pub struct StateSnapshotActor {
    flat_storage_manager: FlatStorageManager,
    tries: ShardTries,
}

impl StateSnapshotActor {
    pub fn new(flat_storage_manager: FlatStorageManager, tries: ShardTries) -> Self {
        Self { flat_storage_manager, tries }
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
    /// Shards that need to be present in the snapshot.
    shard_uids: Vec<ShardUId>,
    /// Whether to perform compaction.
    compaction_enabled: bool,
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
struct CompactSnapshotRequest {}

/// Makes a state snapshot in the background.
impl actix::Handler<WithSpanContext<MakeSnapshotRequest>> for StateSnapshotActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<MakeSnapshotRequest>,
        _ctx: &mut actix::Context<Self>,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "state_snapshot", msg);
        let MakeSnapshotRequest { prev_block_hash, shard_uids, compaction_enabled } = msg;

        let res = self.tries.make_state_snapshot(&prev_block_hash, &shard_uids);
        if !self.flat_storage_manager.set_flat_state_updates_mode(true) {
            tracing::error!(target: "state_snapshot", ?prev_block_hash, ?shard_uids, "Failed to unlock flat state updates");
        }
        match res {
            Ok(_) => {
                if compaction_enabled {
                    _ctx.address().do_send(CompactSnapshotRequest {}.with_span_context());
                } else {
                    tracing::info!(target: "state_snapshot", "State snapshot ready, not running compaction.");
                }
            }
            Err(err) => {
                tracing::error!(target: "state_snapshot", ?err, "State snapshot creation failed")
            }
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
        let (_span, _msg) = handler_debug_span!(target: "state_snapshot", msg);

        if let Err(err) = self.tries.compact_state_snapshot() {
            tracing::error!(target: "state_snapshot", ?err, "State snapshot compaction failed");
        } else {
            tracing::info!(target: "state_snapshot", "State snapshot compaction succeeded");
        }
    }
}

pub type MakeSnapshotCallback =
    Arc<dyn Fn(CryptoHash, Vec<ShardUId>) -> () + Send + Sync + 'static>;

/// Sends a request to make a state snapshot.
pub fn get_make_snapshot_callback(
    state_snapshot_addr: Arc<actix::Addr<StateSnapshotActor>>,
    flat_storage_manager: FlatStorageManager,
    compaction_enabled: bool,
) -> MakeSnapshotCallback {
    Arc::new(move |prev_block_hash, shard_uids| {
        tracing::info!(target: "state_snapshot", ?prev_block_hash, ?shard_uids, "start_snapshot_callback sends `MakeSnapshotCallback` to state_snapshot_addr");
        if flat_storage_manager.set_flat_state_updates_mode(false) {
            state_snapshot_addr.do_send(
                MakeSnapshotRequest { prev_block_hash, shard_uids, compaction_enabled }
                    .with_span_context(),
            );
        }
    })
}
