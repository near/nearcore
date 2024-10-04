use near_async::messaging::{Actor, CanSend, Handler, Sender};
use near_async::{MultiSend, MultiSenderFrom};
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_performance_metrics_macros::perf;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::EpochHeight;
use near_store::flat::FlatStorageManager;
use near_store::ShardTries;
use std::sync::Arc;

/// Runs tasks related to state snapshots.
/// There are three main handlers in StateSnapshotActor and they are called in sequence
/// 1. [`DeleteAndMaybeCreateSnapshotRequest`]: deletes a snapshot and optionally calls CreateSnapshotRequest.
/// 2. [`CreateSnapshotRequest`]: creates a new snapshot.
pub struct StateSnapshotActor {
    flat_storage_manager: FlatStorageManager,
    network_adapter: PeerManagerAdapter,
    tries: ShardTries,
    self_sender: StateSnapshotSenderForStateSnapshot,
}

impl Actor for StateSnapshotActor {}

impl StateSnapshotActor {
    pub fn new(
        flat_storage_manager: FlatStorageManager,
        network_adapter: PeerManagerAdapter,
        tries: ShardTries,
        self_sender: StateSnapshotSenderForStateSnapshot,
    ) -> Self {
        Self { flat_storage_manager, network_adapter, tries, self_sender }
    }
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct DeleteAndMaybeCreateSnapshotRequest {
    /// Optionally send request to create a new snapshot after deleting any existing snapshots.
    create_snapshot_request: Option<CreateSnapshotRequest>,
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct CreateSnapshotRequest {
    /// prev_hash of the last processed block.
    prev_block_hash: CryptoHash,
    /// epoch height associated with prev_block_hash
    epoch_height: EpochHeight,
    /// Shards that need to be present in the snapshot.
    shard_uids: Vec<ShardUId>,
    /// Last block of the prev epoch.
    block: Block,
}

impl StateSnapshotActor {
    pub fn handle_delete_and_maybe_create_snapshot_request(
        &mut self,
        msg: DeleteAndMaybeCreateSnapshotRequest,
    ) {
        tracing::debug!(target: "state_snapshot", ?msg);

        // We don't need to acquire any locks on flat storage or snapshot.
        let DeleteAndMaybeCreateSnapshotRequest { create_snapshot_request } = msg;
        self.tries.delete_state_snapshot();

        // Optionally send a create_snapshot_request after deletion
        if let Some(create_snapshot_request) = create_snapshot_request {
            self.self_sender.send(create_snapshot_request);
        }
    }

    pub fn handle_create_snapshot_request(&mut self, msg: CreateSnapshotRequest) {
        tracing::debug!(target: "state_snapshot", ?msg);

        let CreateSnapshotRequest { prev_block_hash, epoch_height, shard_uids, block } = msg;
        let res = self.tries.create_state_snapshot(prev_block_hash, &shard_uids, &block);

        // Unlocking flat state head can be done asynchronously in state_snapshot_actor.
        // The next flat storage update will bring flat storage to latest head.
        if !self.flat_storage_manager.set_flat_state_updates_mode(true) {
            tracing::error!(target: "state_snapshot", ?prev_block_hash, ?shard_uids, "Failed to unlock flat state updates");
        }
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

impl Handler<DeleteAndMaybeCreateSnapshotRequest> for StateSnapshotActor {
    #[perf]
    fn handle(&mut self, msg: DeleteAndMaybeCreateSnapshotRequest) {
        self.handle_delete_and_maybe_create_snapshot_request(msg)
    }
}

impl Handler<CreateSnapshotRequest> for StateSnapshotActor {
    #[perf]
    fn handle(&mut self, msg: CreateSnapshotRequest) {
        self.handle_create_snapshot_request(msg)
    }
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct StateSnapshotSenderForStateSnapshot {
    create_snapshot: Sender<CreateSnapshotRequest>,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct StateSnapshotSenderForClient(Sender<DeleteAndMaybeCreateSnapshotRequest>);

type MakeSnapshotCallback =
    Arc<dyn Fn(CryptoHash, EpochHeight, Vec<ShardUId>, Block) -> () + Send + Sync + 'static>;

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
    Arc::new(move |prev_block_hash, epoch_height, shard_uids, block| {
        tracing::info!(
            target: "state_snapshot",
            ?prev_block_hash,
            ?shard_uids,
            "make_snapshot_callback sends `DeleteAndMaybeCreateSnapshotRequest` to state_snapshot_addr");
        // We need to stop flat head updates synchronously in the client thread.
        // Async update in state_snapshot_actor and potentially lead to flat head progressing beyond prev_block_hash
        if !flat_storage_manager.set_flat_state_updates_mode(false) {
            tracing::error!(target: "state_snapshot", ?prev_block_hash, ?shard_uids, "Failed to lock flat state updates");
            return;
        }
        let create_snapshot_request =
            CreateSnapshotRequest { prev_block_hash, epoch_height, shard_uids, block };
        sender.send(DeleteAndMaybeCreateSnapshotRequest {
            create_snapshot_request: Some(create_snapshot_request),
        });
    })
}

/// Sends a request to delete a state snapshot.
pub fn get_delete_snapshot_callback(
    sender: StateSnapshotSenderForClient,
) -> DeleteSnapshotCallback {
    Arc::new(move || {
        tracing::info!(
            target: "state_snapshot",
            "delete_snapshot_callback sends `DeleteAndMaybeCreateSnapshotRequest` to state_snapshot_addr");
        sender.send(DeleteAndMaybeCreateSnapshotRequest { create_snapshot_request: None });
    })
}
