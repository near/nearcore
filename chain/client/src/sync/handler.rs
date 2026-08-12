use super::block::BlockSync;
use super::epoch::EpochSync;
use super::header::HeaderSync;
use super::state::StateSync;
use crate::sync::state::StateSyncResult;
use near_chain::chain::ApplyChunksDoneSender;
use near_chain::{BlockProcessingArtifact, Chain, ChainStoreAccess};
use near_chain_configs::ClientConfig;
use near_client_primitives::types::{EpochSyncStatus, StateSyncStatus, SyncStatus};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::types::HighestHeightPeerInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_store::adapter::StoreAdapter;

/// Handles syncing chain to the actual state of the network.
pub struct SyncHandler {
    config: ClientConfig,
    pub sync_status: SyncStatus,
    /// Keeps track of information needed to perform the initial Epoch Sync
    pub epoch_sync: EpochSync,
    /// Keeps track of syncing headers.
    header_sync: HeaderSync,
    /// Keeps track of syncing state.
    pub state_sync: StateSync,
    /// Keeps track of syncing block.
    block_sync: BlockSync,
}

/// Request to the client to perform some action to continue syncing.
pub enum SyncHandlerRequest {
    /// Need to request new blocks from given peers.
    NeedRequestBlocks(Vec<(CryptoHash, PeerId)>),
    /// Need to process block artifact unlocked by state sync.
    NeedProcessBlockArtifact(BlockProcessingArtifact),
    /// Sync hash is stale — the network has moved past the epoch we are syncing
    /// from. Write the data reset marker and shut down so the supervisor can
    /// wipe and restart.
    EpochSyncDataReset,
}

impl SyncHandler {
    pub fn new(
        config: ClientConfig,
        epoch_sync: EpochSync,
        header_sync: HeaderSync,
        state_sync: StateSync,
        block_sync: BlockSync,
    ) -> Self {
        Self {
            config,
            sync_status: SyncStatus::AwaitingPeers,
            epoch_sync,
            header_sync,
            state_sync,
            block_sync,
        }
    }

    /// Handle the SyncRequirement::SyncNeeded.
    ///
    /// Single linear sync pipeline:
    ///
    ///   EpochSync → HeaderSync → StateSync → BlockSync → NoSync
    ///
    /// Near horizon (within `epoch_sync_horizon`): enters at BlockSync.
    ///   Both header sync and block sync are called each tick. Header sync
    ///   fetches batches of headers, populating the `NextBlockHashes` chain.
    ///   Block sync walks that chain via `get_next_block_hash()` — when no
    ///   headers exist ahead it gets `DBNotFoundErr` and waits until the
    ///   next 2s timeout retry, so it naturally self-paces behind header sync.
    ///
    /// Far horizon (beyond `epoch_sync_horizon`): enters at EpochSync.
    ///   After epoch sync completes, headers advance past the epoch boundary,
    ///   state sync downloads state at the sync hash, and finally block sync
    ///   catches up.
    pub fn handle_sync_needed(
        &mut self,
        chain: &mut Chain,
        shard_tracker: &ShardTracker,
        highest_height: u64,
        highest_height_peers: &[HighestHeightPeerInfo],
        apply_chunks_done_sender: Option<ApplyChunksDoneSender>,
    ) -> Result<Option<SyncHandlerRequest>, near_chain::Error> {
        if matches!(self.sync_status, SyncStatus::NoSync | SyncStatus::AwaitingPeers) {
            self.decide_initial_phase(chain, highest_height)?;
        }

        match &mut self.sync_status {
            SyncStatus::EpochSync(EpochSyncStatus::Done) => {
                // Epoch sync finished - transition to header sync
                // download the remaining headers before starting state sync.
                let header_head = chain.header_head()?;
                self.sync_status.update(SyncStatus::HeaderSync {
                    start_height: header_head.height,
                    current_height: header_head.height,
                    highest_height,
                });
            }
            SyncStatus::EpochSync(epoch_sync_status) => {
                // Epoch sync still in progress (NotStarted or InProgress) —
                // keep requesting/waiting for the epoch sync proof from a peer.
                self.epoch_sync.run(epoch_sync_status, highest_height_peers)?;
            }
            SyncStatus::HeaderSync { current_height, highest_height: hh, .. } => {
                // ban stalling peers during primary header sync
                self.header_sync.run(chain, highest_height, highest_height_peers, true)?;

                let header_head = chain.header_head()?;
                *current_height = header_head.height;
                *hh = highest_height;

                // Once we have enough headers (within block_header_fetch_horizon of
                // highest_height), look up the sync hash and transition to state sync.
                let min_header_height =
                    highest_height.saturating_sub(self.config.block_header_fetch_horizon);
                if header_head.height >= min_header_height {
                    if let Some(sync_hash) = chain.find_sync_hash()? {
                        tracing::debug!(target: "sync", ?sync_hash, "sync: transition to state sync");
                        self.sync_status
                            .update(SyncStatus::StateSync(StateSyncStatus::new(sync_hash)));
                    }
                }
            }
            SyncStatus::StateSync(state_sync_status) => {
                match self.state_sync.run(
                    state_sync_status,
                    shard_tracker,
                    chain,
                    highest_height,
                    highest_height_peers,
                    apply_chunks_done_sender,
                )? {
                    StateSyncResult::NeedBlocks(blocks) => {
                        // NeedBlocks requests are forwarded to the caller so the client can fetch
                        // the blocks required by state sync.
                        tracing::debug!(target: "sync", num_blocks = blocks.len(), "waiting for sync blocks");
                        return Ok(Some(SyncHandlerRequest::NeedRequestBlocks(blocks)));
                    }
                    StateSyncResult::InProgress => {}
                    StateSyncResult::Completed(artifacts) => {
                        let head = chain.head()?;
                        self.sync_status.update(SyncStatus::BlockSync {
                            start_height: head.height,
                            current_height: head.height,
                            highest_height,
                        });
                        return Ok(Some(SyncHandlerRequest::NeedProcessBlockArtifact(artifacts)));
                    }
                    StateSyncResult::StaleSyncHash => {
                        return Ok(Some(SyncHandlerRequest::EpochSyncDataReset));
                    }
                }
            }
            SyncStatus::BlockSync { current_height, highest_height: hh, .. } => {
                // Fetching remaining blocks to catch up to the network tip.
                // Header sync continues alongside block sync — it extends the
                // NextBlockHashes chain while block sync follows it. Exit from
                // sync is handled by run_sync_step() detecting AlreadyCaughtUp.
                // don't ban during block sync — peers may be serving blocks
                self.header_sync.run(chain, highest_height, highest_height_peers, false)?;
                self.block_sync.run(chain, highest_height_peers)?;

                let head = chain.head()?;
                *current_height = head.height;
                *hh = highest_height;
            }
            status => unreachable!("unexpected sync status in handle_sync_needed: {:?}", status),
        }
        Ok(None)
    }

    /// Decide the initial sync phase based on node state and network distance.
    ///
    /// Checks (in order):
    /// 1. Archival or near horizon: if archival or head (block head) is within
    ///    the epoch sync horizon, enter BlockSync.
    /// 2. Restart recovery: if an epoch sync proof exists and header_head is
    ///    within the epoch sync horizon, the node crashed mid-pipeline but is
    ///    still close enough to resume via HeaderSync.
    /// 3. Epoch sync: everything else. Stale nodes (header_head past genesis)
    ///    are detected in the epoch sync response handler and trigger data reset.
    fn decide_initial_phase(
        &mut self,
        chain: &Chain,
        highest_height: u64,
    ) -> Result<(), near_chain::Error> {
        let head = chain.head()?;
        let header_head = chain.header_head()?;
        let horizon = self.config.epoch_sync.epoch_sync_horizon_num_epochs * chain.epoch_length;
        let head_within_horizon = head.height + horizon >= highest_height;
        let header_head_within_horizon = header_head.height + horizon >= highest_height;

        // Archival nodes must process every block; epoch sync would skip them.
        // Near horizon nodes can catch up with header+block sync alone.
        if self.config.archive || head_within_horizon {
            tracing::info!(target: "sync", ?head, ?highest_height, "entering block sync");
            self.sync_status.update(SyncStatus::BlockSync {
                start_height: head.height,
                current_height: head.height,
                highest_height,
            });
            return Ok(());
        }

        // Restart recovery: epoch sync proof exists and header_head is close
        // enough to the tip to resume from where we left off. Enter HeaderSync
        // which will transition to StateSync once headers are caught up.
        // Previously downloaded state parts are preserved (DBCol::StateParts).
        let has_epoch_sync_proof =
            chain.chain_store().store().epoch_store().get_epoch_sync_proof()?.is_some();
        if has_epoch_sync_proof && header_head_within_horizon {
            tracing::info!(target: "sync", ?head, ?header_head, ?highest_height, "restart recovery: resuming with header sync");
            self.sync_status.update(SyncStatus::HeaderSync {
                start_height: header_head.height,
                current_height: header_head.height,
                highest_height,
            });
            return Ok(());
        }

        // Far horizon — initiate epoch sync. Stale nodes (header_head past
        // genesis from a prior sync attempt) will be detected in the epoch sync
        // response handler, which triggers data reset before applying the proof.
        tracing::info!(target: "sync", ?head, ?header_head, ?highest_height, "entering epoch sync");
        self.sync_status.update(SyncStatus::EpochSync(EpochSyncStatus::NotStarted));
        Ok(())
    }
}
