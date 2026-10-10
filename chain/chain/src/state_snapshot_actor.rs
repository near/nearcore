use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::{Actor, CanSend, Handler, HandlerWithContext, Sender};
use near_async::time::Duration;
use near_async::{MultiSend, MultiSenderFrom};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::types::{
    NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest, SnapshotHostEvent,
};
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{BlockHeight, EpochHeight, ShardId, ShardIndex, SpiceChunkId};
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::flat::{FlatStorageManager, FlatStorageReshardingStatus, FlatStorageStatus};
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
    chain_store: ChainStoreAdapter,
}

impl Actor for StateSnapshotActor {}

impl StateSnapshotActor {
    pub fn new(
        flat_storage_manager: FlatStorageManager,
        network_adapter: PeerManagerAdapter,
        tries: ShardTries,
        chain_store: ChainStoreAdapter,
    ) -> Self {
        Self { flat_storage_manager, network_adapter, tries, chain_store }
    }
}

/// A snapshot that exists on disk but is not yet announced to the network.
///
/// Spice commits a chunk's state root through the execution result a later block certifies,
/// so before that happens no peer can prove the roots the snapshot serves. The host info is
/// therefore held back until the snapshotted block's chunks are certified.
#[derive(Debug, Clone)]
struct PendingAdvertisement {
    /// The block the snapshot was taken at, which under spice is the state sync hash.
    sync_hash: CryptoHash,
    epoch_height: EpochHeight,
    shards: Vec<ShardId>,
}

#[derive(Debug)]
pub struct DeleteSnapshotRequest {}

pub struct CreateSnapshotRequest {
    /// The block the snapshot is taken at, and the hash it is keyed by.
    snapshot_hash: CryptoHash,
    /// Min height of chunk.prev_block_hash() for each chunk in `block`
    min_chunk_prev_height: BlockHeight,
    /// epoch height associated with the block being snapshotted
    epoch_height: EpochHeight,
    /// Shards that need to be present in the snapshot.
    shard_indexes_and_uids: Vec<(ShardIndex, ShardUId)>,
    /// The block whose state is being snapshotted. Without spice that is the prev block of
    /// the "sync_hash" block; under spice it is the sync block itself.
    block: Arc<Block>,
}

impl std::fmt::Debug for CreateSnapshotRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateSnapshotRequest")
            .field("block_hash", self.block.hash())
            .field("snapshot_hash", &self.snapshot_hash)
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
    ) -> bool {
        let shard_uids = shard_indexes_and_uids.iter().map(|(_idx, uid)| *uid);
        let Some(min_height) =
            self.flat_storage_manager.resharding_catchup_height_reached(shard_uids)
        else {
            // No flat storage split + catchup is in progress, ok to proceed
            return false;
        };
        let Some(min_height) = min_height else {
            // storage split + catchup is in progress and not all shards have reached the catchup phase yet. Can't proceed
            let not_ready_shards: Vec<ShardUId> = shard_indexes_and_uids
                .iter()
                .filter_map(|(_idx, shard_uid)| {
                    match self.flat_storage_manager.get_flat_storage_status(*shard_uid) {
                        FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CatchingUp(
                            _,
                        )) => None,
                        FlatStorageStatus::Resharding(_) => Some(*shard_uid),
                        _ => None,
                    }
                })
                .collect();
            tracing::debug!(target: "state_snapshot", ?not_ready_shards, "waiting for resharding: shards not in catchup phase");
            return true;
        };
        // Proceed if the catchup code is already reasonably close to being finished. This is not a correctness issue,
        // as this line of code could just be replaced with false, and things would work. But in that case, if there are for
        // some reason lots of deltas to apply (e.g. the sync hash is 1000s of blocks past the start of the epoch because of missed
        // chunks), then we'll duplicate a lot of work that's being done by the resharding catchup code. So we might as well just
        // come back later after most of that work has already been done.
        let should_wait = min_height + 10 < min_chunk_prev_height;
        if should_wait {
            tracing::debug!(target: "state_snapshot", min_height, min_chunk_prev_height, "waiting for resharding catchup");
        }
        should_wait
    }

    pub fn handle_create_snapshot_request(
        &mut self,
        msg: CreateSnapshotRequest,
        ctx: &mut dyn DelayedActionRunner<Self>,
    ) {
        let chain_progressed_msg =
            SnapshotHostEvent::ChainProgressed { epoch_height: msg.epoch_height };
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::SnapshotHostEvent(chain_progressed_msg),
        ));

        if let StateSnapshotConfig::Disabled = self.tries.state_snapshot_config() {
            tracing::info!(target: "state_snapshot", ?msg, "snapshots are disabled");
            return;
        }
        if let Some(last_requested_hash) = self.flat_storage_manager.snapshot_hash_wanted() {
            if last_requested_hash != msg.snapshot_hash {
                tracing::info!(target: "state_snapshot", ?msg, %last_requested_hash, "skipping state snapshot in favor of more recent request");
                return;
            }
        }
        let should_wait = self.should_wait_for_resharding_split(
            msg.min_chunk_prev_height,
            &msg.shard_indexes_and_uids,
        );
        // TODO: instead of resending the same message over and over, wait on a Condvar.
        // This would require making testloop work with Condvars that normally are meant to be woken up by another thread
        if should_wait {
            tracing::debug!(target: "state_snapshot", snapshot_hash = ?&msg.snapshot_hash, "postpone create snapshot request");
            ctx.run_later(
                "ReshardingActor FlatStorageSplitShard",
                Duration::seconds(1),
                move |act, ctx| {
                    act.handle_create_snapshot_request(msg, ctx);
                },
            );
            return;
        }

        tracing::debug!(target: "state_snapshot", snapshot_hash = ?&msg.snapshot_hash, "handle create snapshot request");
        let CreateSnapshotRequest {
            snapshot_hash,
            epoch_height,
            shard_indexes_and_uids,
            block,
            ..
        } = msg;

        self.tries.delete_state_snapshot();
        let res = self.tries.create_state_snapshot(snapshot_hash, &shard_indexes_and_uids, &block);

        // Unlocking flat state head can be done asynchronously in state_snapshot_actor.
        // The next flat storage update will bring flat storage to latest head.
        // TODO(resharding): this can actually be called sooner, just after the rocksdb checkpoint is made.
        self.flat_storage_manager.snapshot_taken(&snapshot_hash);
        match res {
            Ok(res_shard_uids) => {
                let Some(res_shard_uids) = res_shard_uids else {
                    return;
                };

                let pending = PendingAdvertisement {
                    sync_hash: snapshot_hash,
                    epoch_height,
                    shards: res_shard_uids.iter().map(|uid| uid.shard_id.into()).collect(),
                };
                if block.is_spice_block() {
                    self.advertise_once_certified(pending, ctx);
                } else {
                    self.advertise(pending);
                }
            }
            Err(err) => {
                tracing::error!(target: "state_snapshot", ?err, "state snapshot creation failed")
            }
        }
    }

    /// Sends the host info out, making the snapshot discoverable by syncing nodes.
    fn advertise(&self, pending: PendingAdvertisement) {
        let PendingAdvertisement { sync_hash, epoch_height, shards } = pending;
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::SnapshotHostEvent(SnapshotHostEvent::SnapshotCreated {
                sync_hash,
                epoch_height,
                shards,
            }),
        ));
    }

    /// Holds the host info back until every snapshotted shard's chunk in the sync block has
    /// been certified, then sends it. A syncing peer proves the state root against the
    /// `chunk_execution_root` of the block that committed the execution result, so a snapshot
    /// announced before certification could not be served.
    fn advertise_once_certified(
        &self,
        pending: PendingAdvertisement,
        ctx: &mut dyn DelayedActionRunner<Self>,
    ) {
        if !self.all_shards_certified(&pending) {
            tracing::debug!(
                target: "state_snapshot",
                sync_hash = ?pending.sync_hash,
                "snapshot is not certified yet; delaying host advertisement",
            );
            // TODO: react to certification instead of polling for it.
            ctx.run_later(
                "StateSnapshotActor advertise once certified",
                Duration::seconds(1),
                move |act, ctx| {
                    act.advertise_once_certified(pending, ctx);
                },
            );
            return;
        }
        self.advertise(pending);
    }

    fn all_shards_certified(&self, pending: &PendingAdvertisement) -> bool {
        pending.shards.iter().all(|&shard_id| {
            let chunk_id = SpiceChunkId { block_hash: pending.sync_hash, shard_id };
            self.chain_store.get_chunk_certifying_block(&chunk_id).is_some()
        })
    }
}

impl Handler<DeleteSnapshotRequest> for StateSnapshotActor {
    fn handle(&mut self, msg: DeleteSnapshotRequest) {
        self.handle_delete_snapshot_request(msg)
    }
}

impl HandlerWithContext<CreateSnapshotRequest> for StateSnapshotActor {
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
    dyn Fn(BlockHeight, EpochHeight, Vec<(ShardIndex, ShardUId)>, Arc<Block>) -> ()
        + Send
        + Sync
        + 'static,
>;

type DeleteSnapshotCallback = Arc<dyn Fn() -> () + Send + Sync + 'static>;

#[derive(Clone)]
pub struct SnapshotCallbacks {
    pub make_snapshot_callback: MakeSnapshotCallback,
    pub delete_snapshot_callback: DeleteSnapshotCallback,
}

/// Requests a snapshot of the state at `block`, if the configured cadence wants one for this
/// epoch.
///
/// The caller decides *when* that state exists: without spice the block's chunks have been
/// applied by the time it reaches the head, while under spice the chunk executor calls this
/// once they have executed.
pub fn request_state_snapshot(
    snapshot_callbacks: &SnapshotCallbacks,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    chain_store: &ChainStoreAdapter,
    tries: &ShardTries,
    block: Arc<Block>,
) -> Result<(), Error> {
    if let StateSnapshotConfig::Disabled = tries.state_snapshot_config() {
        return Ok(());
    }
    let prev_hash = *block.header().prev_hash();
    let epoch_height = epoch_manager.get_epoch_height_from_prev_block(&prev_hash)?;
    if epoch_height % tries.state_snapshot_config().snapshot_cadence() != 0
        && !epoch_manager.is_resharding_epoch(block.hash())?
    {
        // Force the resharding epoch's snapshot even off-cadence; cloud archival requires it.
        // A node snapshotting every epoch (cadence 1) never reaches this branch.
        return Ok(());
    }
    let shard_layout = epoch_manager.get_shard_layout_from_prev_block(&prev_hash)?;
    let shard_indexes_and_uids: Vec<(ShardIndex, ShardUId)> = shard_layout
        .shard_uids()
        .enumerate()
        .filter(|&(_, shard_uid)| shard_tracker.cares_about_shard(&prev_hash, shard_uid.shard_id()))
        .collect();
    let min_chunk_prev_height = min_chunk_prev_height(chain_store, &block)?;
    (snapshot_callbacks.make_snapshot_callback)(
        min_chunk_prev_height,
        epoch_height,
        shard_indexes_and_uids,
        block,
    );
    Ok(())
}

/// The oldest height any of `block`'s chunks builds on.
fn min_chunk_prev_height(
    chain_store: &ChainStoreAdapter,
    block: &Block,
) -> Result<BlockHeight, Error> {
    let mut ret = None;
    for chunk in block.chunks().iter() {
        let prev_height = if chunk.prev_block_hash() == &CryptoHash::default() {
            0
        } else {
            chain_store.get_block_header(chunk.prev_block_hash())?.height()
        };
        ret = Some(ret.map_or(prev_height, |min: BlockHeight| min.min(prev_height)));
    }
    Ok(ret.unwrap_or(0))
}

/// Sends a request to make a state snapshot.
pub fn get_make_snapshot_callback(
    sender: StateSnapshotSenderForClient,
    flat_storage_manager: FlatStorageManager,
) -> MakeSnapshotCallback {
    Arc::new(move |min_chunk_prev_height, epoch_height, shard_indexes_and_uids, block| {
        // Which block the snapshot sits at. Spice snapshots the state `block`'s own chunks
        // left behind, everything else the state as of the block before it. `StateSnapshot`
        // moves the snapshot's flat head to the same block.
        let snapshot_hash =
            if block.is_spice_block() { *block.hash() } else { *block.header().prev_hash() };
        tracing::info!(
            target: "state_snapshot",
            ?snapshot_hash,
            ?shard_indexes_and_uids,
            "make_snapshot_callback sends `CreateSnapshotRequest` to state_snapshot_addr");
        // We need to stop flat head updates synchronously in the client thread.
        // Async update in state_snapshot_actor can potentially lead to flat head progressing beyond snapshot_hash
        // This also prevents post-resharding flat storage catchup from advancing past `snapshot_hash`
        flat_storage_manager.want_snapshot(snapshot_hash, min_chunk_prev_height);
        let create_snapshot_request = CreateSnapshotRequest {
            snapshot_hash,
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
