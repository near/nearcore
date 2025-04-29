use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::{Actor, CanSend, Handler, HandlerWithContext, Sender};
use near_async::time::Duration;
use near_async::{MultiSend, MultiSenderFrom};
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_performance_metrics_macros::perf;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{BlockHeight, EpochHeight, ShardIndex};
use near_store::flat::FlatStorageManager;
use near_store::{ShardTries, StateSnapshotConfig};
use std::sync::Arc;

/// Runs tasks related to state snapshots.
/// There are three main handlers in StateSnapshotActor and they are called in sequence
/// 1. [`DeleteSnapshotRequest`]: deletes a snapshot.
/// 2. [`CreateSnapshotRequest`]: creates a new snapshot.
pub struct StateSnapshotActor {
    flat_storage_manager: FlatStorageManager,
    network_adapter: PeerManagerAdapter,
    tries: ShardTries,
}

impl Actor for StateSnapshotActor {}

impl StateSnapshotActor {
    pub fn new(
        flat_storage_manager: FlatStorageManager,
        network_adapter: PeerManagerAdapter,
        tries: ShardTries,
    ) -> Self {
        Self { flat_storage_manager, network_adapter, tries }
    }
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct DeleteSnapshotRequest {}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct CreateSnapshotRequest {
    /// equal to self.block.header().prev_hash()
    prev_block_hash: CryptoHash,
    /// Min height of chunk.prev_block_hash() for each chunk in `block`
    min_chunk_prev_height: BlockHeight,
    /// epoch height associated with prev_block_hash
    epoch_height: EpochHeight,
    /// Shards that need to be present in the snapshot.
    shard_indexes_and_uids: Vec<(ShardIndex, ShardUId)>,
    /// prev block of the "sync_hash" block.
    block: Block,
}

impl std::fmt::Debug for CreateSnapshotRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateSnapshotRequest")
            .field("block_hash", self.block.hash())
            .field("prev_block_hash", &self.prev_block_hash)
            .field("min_chunk_prev_height", &self.min_chunk_prev_height)
            .field("epoch_height", &self.epoch_height)
            .field(
                "shard_uids",
                &self.shard_indexes_and_uids.iter().map(|(_index, uid)| uid).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl StateSnapshotActor {
    pub fn handle_delete_snapshot_request(&mut self, msg: DeleteSnapshotRequest) {
        tracing::debug!(target: "state_snapshot", ?msg);

        // We don't need to acquire any locks on flat storage or snapshot.
        self.tries.delete_state_snapshot();
    }

    /// Returns true if we shouldn't yet try to create a snapshot because a flat storage resharding
    /// is in progress.
    fn should_wait_for_resharding_split(
        &self,
        min_chunk_prev_height: BlockHeight,
        shard_indexes_and_uids: &[(ShardIndex, ShardUId)],
    ) -> anyhow::Result<bool> {
        let shard_uids = shard_indexes_and_uids.iter().map(|(_idx, uid)| *uid);
        let Some(min_height) =
            self.flat_storage_manager.resharding_catchup_height_reached(shard_uids)?
        else {
            // No flat storage split + catchup is in progress, ok to proceed
            return Ok(false);
        };
        let Some(min_height) = min_height else {
            // storage split + catchup is in progress and not all shards have reached the catchup phase yet. Can't proceed
            return Ok(true);
        };
        // Proceed if the catchup code is already reasonably close to being finished. This is not a correctness issue,
        // as this line of code could just be replaced with Ok(false), and things would work. But in that case, if there are for
        // some reason lots of deltas to apply (e.g. the sync hash is 1000s of blocks past the start of the epoch because of missed
        // chunks), then we'll duplicate a lot of work that's being done by the resharding catchup code. So we might as well just
        // come back later after most of that work has already been done.
        Ok(min_height + 10 < min_chunk_prev_height)
    }

    pub fn handle_create_snapshot_request(
        &mut self,
        msg: CreateSnapshotRequest,
        ctx: &mut dyn DelayedActionRunner<Self>,
    ) {
        if let StateSnapshotConfig::Disabled = self.tries.state_snapshot_config() {
            tracing::info!(target: "state_snapshot", ?msg, "Snapshots are disabled");
            return;
        }
        if let Some(last_requested_hash) = self.flat_storage_manager.snapshot_hash_wanted() {
            if last_requested_hash != msg.prev_block_hash {
                tracing::info!(target: "state_snapshot", ?msg, %last_requested_hash, "Skipping state snapshot in favor of more recent request");
                return;
            }
        }
        let should_wait = match self.should_wait_for_resharding_split(
            msg.min_chunk_prev_height,
            &msg.shard_indexes_and_uids,
        ) {
            Ok(s) => s,
            Err(err) => {
                tracing::error!(target: "state_snapshot", ?err, "State Snapshot Actor failed to check resharding status. Not making snapshot");
                return;
            }
        };
        // TODO: instead of resending the same message over and over, wait on a Condvar.
        // This would require making testloop work with Condvars that normally are meant to be woken up by another thread
        if should_wait {
            tracing::debug!(target: "state_snapshot", prev_block_hash=?&msg.prev_block_hash, "Postpone CreateSnapshotRequest");
            ctx.run_later(
                "ReshardingActor FlatStorageSplitShard",
                Duration::seconds(1),
                move |act, ctx| {
                    act.handle_create_snapshot_request(msg, ctx);
                },
            );
            return;
        }

        tracing::debug!(target: "state_snapshot", prev_block_hash=?&msg.prev_block_hash, "Handle CreateSnapshotRequest");
        let CreateSnapshotRequest {
            prev_block_hash,
            epoch_height,
            shard_indexes_and_uids,
            block,
            ..
        } = msg;

        self.tries.delete_state_snapshot();
        let res =
            self.tries.create_state_snapshot(prev_block_hash, &shard_indexes_and_uids, &block);

        // Unlocking flat state head can be done asynchronously in state_snapshot_actor.
        // The next flat storage update will bring flat storage to latest head.
        // TODO(resharding): this can actually be called sooner, just after the rocksdb checkpoint is made.
        self.flat_storage_manager.snapshot_taken(&prev_block_hash);
        match res {
            Ok(res_shard_uids) => {
                let Some(res_shard_uids) = res_shard_uids else {
                    return;
                };

                self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::SnapshotHostInfo {
                        sync_hash: prev_block_hash,
                        epoch_height,
                        shards: res_shard_uids.iter().map(|uid| uid.shard_id.into()).collect(),
                    },
                ));
            }
            Err(err) => {
                tracing::error!(target: "state_snapshot", ?err, "State snapshot creation failed")
            }
        }
    }
}

impl Handler<DeleteSnapshotRequest> for StateSnapshotActor {
    #[perf]
    fn handle(&mut self, msg: DeleteSnapshotRequest) {
        self.handle_delete_snapshot_request(msg)
    }
}

impl HandlerWithContext<CreateSnapshotRequest> for StateSnapshotActor {
    #[perf]
    fn handle(&mut self, msg: CreateSnapshotRequest, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.handle_create_snapshot_request(msg, ctx)
    }
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct StateSnapshotSenderForStateSnapshot {
    create_snapshot: Sender<CreateSnapshotRequest>,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct StateSnapshotSenderForClient {
    delete_snapshot: Sender<DeleteSnapshotRequest>,
    create_snapshot: Sender<CreateSnapshotRequest>,
}

type MakeSnapshotCallback = Arc<
    dyn Fn(BlockHeight, EpochHeight, Vec<(ShardIndex, ShardUId)>, Block) -> ()
        + Send
        + Sync
        + 'static,
>;

type DeleteSnapshotCallback = Arc<dyn Fn() -> () + Send + Sync + 'static>;

pub struct SnapshotCallbacks {
    pub make_snapshot_callback: MakeSnapshotCallback,
    pub delete_snapshot_callback: DeleteSnapshotCallback,
}

/// Sends a request to make a state snapshot.
pub fn get_make_snapshot_callback(
    sender: StateSnapshotSenderForClient,
    flat_storage_manager: FlatStorageManager,
) -> MakeSnapshotCallback {
    Arc::new(move |min_chunk_prev_height, epoch_height, shard_indexes_and_uids, block| {
        let prev_block_hash = *block.header().prev_hash();
        tracing::info!(
            target: "state_snapshot",
            ?prev_block_hash,
            ?shard_indexes_and_uids,
            "make_snapshot_callback sends `CreateSnapshotRequest` to state_snapshot_addr");
        // We need to stop flat head updates synchronously in the client thread.
        // Async update in state_snapshot_actor can potentially lead to flat head progressing beyond prev_block_hash
        // This also prevents post-resharding flat storage catchup from advancing past `prev_block_hash`
        flat_storage_manager.want_snapshot(prev_block_hash, min_chunk_prev_height);
        let create_snapshot_request = CreateSnapshotRequest {
            prev_block_hash,
            min_chunk_prev_height,
            epoch_height,
            shard_indexes_and_uids,
            block,
        };
        sender.send(create_snapshot_request);
    })
}

/// Sends a request to delete a state snapshot.
pub fn get_delete_snapshot_callback(
    sender: StateSnapshotSenderForClient,
) -> DeleteSnapshotCallback {
    Arc::new(move || {
        tracing::info!(
            target: "state_snapshot",
            "delete_snapshot_callback sends `DeleteSnapshotRequest` to state_snapshot_addr");
        sender.send(DeleteSnapshotRequest {});
    })
}
