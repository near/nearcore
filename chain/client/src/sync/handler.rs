use super::block::BlockSync;
use super::epoch::{EpochSync, EpochSyncAction};
use super::header::{HeaderSync, HeaderSyncMode};
use super::state::StateSync;
use crate::sync::state::StateSyncResult;
use near_async::time::{Clock, Utc};
use near_chain::chain::ApplyChunksDoneSender;
use near_chain::types::Tip;
use near_chain::{BlockHeader, BlockProcessingArtifact, Chain};
use near_chain_configs::ClientConfig;
use near_chunks::logic::get_shards_cares_about_this_or_next_epoch;
use near_client_primitives::types::{StateSyncStatus, SyncStatus};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::types::HighestHeightPeerInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::BlockHeight;
use near_primitives::views::{StateSyncStatusView, SyncStatusView};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;

// A small helper macro to unwrap a result of some state sync operation. If the
// result is an error this macro will log it and return from the function.
#[macro_export]
macro_rules! unwrap_and_report_state_sync_result (($obj: ident) => (match $obj {
    Ok(v) => v,
    Err(err) => {
        tracing::error!(target: "sync", obj = stringify!($obj), ?err, "sync: unexpected error");
        return None;
    }
}));

/// Progress tracking for display purposes. Updated by the handler on every
/// tick for HeaderSync and BlockSync. Not used for control flow.
#[derive(Clone, Debug, Default)]
pub struct SyncProgress {
    pub start_height: BlockHeight,
    pub current_height: BlockHeight,
    pub highest_height: BlockHeight,
}

/// Handles syncing chain to the actual state of the network.
pub struct SyncHandler {
    clock: Clock,
    config: ClientConfig,
    pub sync_status: SyncStatus,
    /// Progress tracking for HeaderSync/BlockSync display.
    pub sync_progress: SyncProgress,
    /// Active state sync tracking. Some when in StateSync, None otherwise.
    pub state_sync_status: Option<StateSyncStatus>,
    /// A map storing the last time a block was requested for state sync.
    last_time_sync_block_requested: HashMap<CryptoHash, near_async::time::Utc>,
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
}

impl SyncHandler {
    pub fn new(
        clock: Clock,
        config: ClientConfig,
        epoch_sync: EpochSync,
        header_sync: HeaderSync,
        state_sync: StateSync,
        block_sync: BlockSync,
    ) -> Self {
        Self {
            clock,
            config,
            sync_status: SyncStatus::AwaitingPeers,
            sync_progress: SyncProgress::default(),
            state_sync_status: None,
            last_time_sync_block_requested: HashMap::new(),
            epoch_sync,
            header_sync,
            state_sync,
            block_sync,
        }
    }

    /// Update sync status with transition logging. Clears state_sync_status
    /// when leaving StateSync.
    pub fn set_sync_status(&mut self, new: SyncStatus) {
        if self.sync_status == new {
            return;
        }
        tracing::debug!(
            target: "sync",
            old = ?self.sync_status,
            new = ?new,
            "sync status transition"
        );
        if self.sync_status == SyncStatus::StateSync && new != SyncStatus::StateSync {
            self.state_sync_status = None;
        }
        self.sync_status = new;
    }

    /// Build a SyncStatusView for RPC/debug endpoints.
    pub fn sync_status_view(&self) -> SyncStatusView {
        match self.sync_status {
            SyncStatus::AwaitingPeers => SyncStatusView::AwaitingPeers,
            SyncStatus::NoSync => SyncStatusView::NoSync,
            SyncStatus::EpochSync => {
                if let Some(req) = self.epoch_sync.last_request() {
                    SyncStatusView::EpochSync {
                        source_peer_height: req.peer_height,
                        source_peer_id: req.peer_id.to_string(),
                        attempt_time: req.time.to_string(),
                    }
                } else {
                    SyncStatusView::EpochSync {
                        source_peer_height: 0,
                        source_peer_id: String::new(),
                        attempt_time: String::new(),
                    }
                }
            }
            SyncStatus::EpochSyncDone => SyncStatusView::EpochSyncDone,
            SyncStatus::HeaderSync => SyncStatusView::HeaderSync {
                start_height: self.sync_progress.start_height,
                current_height: self.sync_progress.current_height,
                highest_height: self.sync_progress.highest_height,
            },
            SyncStatus::StateSync => {
                if let Some(ss) = &self.state_sync_status {
                    SyncStatusView::StateSync(StateSyncStatusView {
                        sync_hash: ss.sync_hash,
                        shard_sync_status: ss
                            .sync_status
                            .iter()
                            .map(|(id, s)| (*id, s.to_string()))
                            .collect(),
                        download_tasks: ss.download_tasks.clone(),
                        computation_tasks: ss.computation_tasks.clone(),
                    })
                } else {
                    SyncStatusView::StateSync(StateSyncStatusView {
                        sync_hash: CryptoHash::default(),
                        shard_sync_status: HashMap::new(),
                        download_tasks: vec![],
                        computation_tasks: vec![],
                    })
                }
            }
            SyncStatus::StateSyncDone => SyncStatusView::StateSyncDone,
            SyncStatus::BlockSync => SyncStatusView::BlockSync {
                start_height: self.sync_progress.start_height,
                current_height: self.sync_progress.current_height,
                highest_height: self.sync_progress.highest_height,
            },
        }
    }

    /// Handle the SyncRequirement::SyncNeeded.
    ///
    /// Match-based state machine: sync_status drives what runs each tick.
    pub fn handle_sync_needed(
        &mut self,
        chain: &mut Chain,
        shard_tracker: &ShardTracker,
        highest_height: u64,
        highest_height_peers: &[HighestHeightPeerInfo],
        apply_chunks_done_sender: Option<ApplyChunksDoneSender>,
    ) -> Option<SyncHandlerRequest> {
        match self.sync_status {
            SyncStatus::AwaitingPeers | SyncStatus::NoSync => {
                // Decide whether to start epoch sync or header sync.
                if self.epoch_sync.need_epoch_sync(chain, highest_height) {
                    self.run_epoch_sync(chain, highest_height, highest_height_peers)
                } else {
                    self.run_header_sync_and_set_status(chain, highest_height, highest_height_peers)
                }
            }
            SyncStatus::EpochSync => {
                // Waiting for async epoch sync proof. Retry on timeout.
                // If need_epoch_sync flips false (peers advanced), fall through
                // to header sync instead of staying stuck.
                if !self.epoch_sync.need_epoch_sync(chain, highest_height) {
                    return self.run_header_sync_and_set_status(
                        chain,
                        highest_height,
                        highest_height_peers,
                    );
                }
                self.run_epoch_sync(chain, highest_height, highest_height_peers)
            }
            SyncStatus::EpochSyncDone => {
                // Transient state after epoch sync proof applied. Start header sync.
                self.run_header_sync_and_set_status(chain, highest_height, highest_height_peers)
            }
            SyncStatus::HeaderSync => {
                // Run primary header sync and check for transitions.
                let header_sync_result = self.header_sync.run(
                    HeaderSyncMode::Primary,
                    chain,
                    highest_height,
                    highest_height_peers,
                );
                unwrap_and_report_state_sync_result!(header_sync_result);
                let header_head = chain.header_head();
                let header_head = unwrap_and_report_state_sync_result!(header_head);

                // Update progress tracking.
                let start_height = if self.sync_status == SyncStatus::HeaderSync {
                    self.sync_progress.start_height
                } else {
                    chain.head().map(|t| t.height).unwrap_or(0)
                };
                self.sync_progress = SyncProgress {
                    start_height,
                    current_height: header_head.height,
                    highest_height,
                };
                self.set_sync_status(SyncStatus::HeaderSync);

                // Headers not ready yet — stay in HeaderSync.
                let min_header_height =
                    highest_height.saturating_sub(self.config.block_header_fetch_horizon);
                if header_head.height < min_header_height {
                    return None;
                }

                // Epoch sync boundary constraints — stay in HeaderSync.
                if let Some(epoch_sync_boundary_block_header) =
                    self.epoch_sync.my_own_epoch_sync_boundary_block_header()
                {
                    let current_epoch_start =
                        chain.epoch_manager.get_epoch_start_height(&header_head.last_block_hash);
                    let current_epoch_start =
                        unwrap_and_report_state_sync_result!(current_epoch_start);
                    if &header_head.epoch_id == epoch_sync_boundary_block_header.epoch_id() {
                        return None;
                    }
                    if epoch_sync_boundary_block_header.height()
                        + chain.transaction_validity_period()
                        > current_epoch_start
                    {
                        return None;
                    }
                }

                // State sync vs block sync.
                self.state_or_block_sync(
                    chain,
                    &header_head,
                    shard_tracker,
                    highest_height,
                    highest_height_peers,
                    apply_chunks_done_sender,
                )
            }
            SyncStatus::StateSync => {
                // Background header sync + state sync polling.
                self.run_background_header_sync(chain, highest_height, highest_height_peers);
                self.run_state_sync(
                    chain,
                    shard_tracker,
                    highest_height_peers,
                    apply_chunks_done_sender,
                )
            }
            SyncStatus::StateSyncDone => {
                // Transient state after state sync done. Start block sync.
                self.run_background_header_sync(chain, highest_height, highest_height_peers);
                self.run_block_sync(chain, highest_height, highest_height_peers)
            }
            SyncStatus::BlockSync => {
                // Background header sync + block sync. Re-evaluate state sync
                // need as headers advance — if header head moves far enough
                // ahead, transition to state sync.
                self.run_background_header_sync(chain, highest_height, highest_height_peers);
                let header_head = chain.header_head();
                let header_head = unwrap_and_report_state_sync_result!(header_head);
                self.state_or_block_sync(
                    chain,
                    &header_head,
                    shard_tracker,
                    highest_height,
                    highest_height_peers,
                    apply_chunks_done_sender,
                )
            }
        }
    }

    /// Run epoch sync and update EpochSync status if a request was sent.
    fn run_epoch_sync(
        &mut self,
        chain: &Chain,
        highest_height: u64,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Option<SyncHandlerRequest> {
        let action = self.epoch_sync.run(chain, highest_height, highest_height_peers);
        let action = unwrap_and_report_state_sync_result!(action);
        if let EpochSyncAction::RequestSent { .. } = action {
            self.set_sync_status(SyncStatus::EpochSync);
        }
        None
    }

    /// Run primary header sync and set HeaderSync status. Used by entry points
    /// (AwaitingPeers, EpochSyncDone) that need to kick off header syncing.
    fn run_header_sync_and_set_status(
        &mut self,
        chain: &Chain,
        highest_height: u64,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Option<SyncHandlerRequest> {
        let header_sync_result = self.header_sync.run(
            HeaderSyncMode::Primary,
            chain,
            highest_height,
            highest_height_peers,
        );
        unwrap_and_report_state_sync_result!(header_sync_result);
        let header_head = chain.header_head();
        let header_head = unwrap_and_report_state_sync_result!(header_head);
        // Use head.height as start_height, except after epoch sync where head
        // is at genesis — in that case header_head is the better baseline.
        let start_height = match chain.head() {
            Ok(head) if head.height > 0 => head.height,
            _ => header_head.height,
        };
        self.sync_progress =
            SyncProgress { start_height, current_height: header_head.height, highest_height };
        self.set_sync_status(SyncStatus::HeaderSync);
        None
    }

    /// Run header sync in background mode (never bans peers). Fire-and-forget;
    /// errors are logged but don't block the caller.
    fn run_background_header_sync(
        &mut self,
        chain: &Chain,
        highest_height: u64,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) {
        let header_sync_result = self.header_sync.run(
            HeaderSyncMode::Background,
            chain,
            highest_height,
            highest_height_peers,
        );
        if let Err(err) = header_sync_result {
            tracing::error!(target: "sync", ?err, "sync: background header sync error");
        }
    }

    /// Decide between state sync and block sync based on current head vs
    /// header_head epoch distance. Used by both handle_header_sync (first
    /// entry) and handle_block_sync (re-evaluation as headers advance).
    fn state_or_block_sync(
        &mut self,
        chain: &mut Chain,
        header_head: &Tip,
        shard_tracker: &ShardTracker,
        highest_height: u64,
        highest_height_peers: &[HighestHeightPeerInfo],
        apply_chunks_done_sender: Option<ApplyChunksDoneSender>,
    ) -> Option<SyncHandlerRequest> {
        let need_state_sync = self.need_state_sync(chain, header_head);
        let need_state_sync = unwrap_and_report_state_sync_result!(need_state_sync);
        if need_state_sync {
            tracing::debug!(target: "sync", "transition to state sync");
            self.run_state_sync(
                chain,
                shard_tracker,
                highest_height_peers,
                apply_chunks_done_sender,
            )
        } else {
            self.run_block_sync(chain, highest_height, highest_height_peers)
        }
    }

    /// Run block sync, request blocks, and set BlockSync status. Preserves
    /// existing start_height if already in BlockSync, otherwise uses head.
    fn run_block_sync(
        &mut self,
        chain: &Chain,
        highest_height: u64,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Option<SyncHandlerRequest> {
        let head = chain.head();
        let head = unwrap_and_report_state_sync_result!(head);
        let block_sync_result =
            self.block_sync.run(chain, highest_height_peers, self.config.sync_max_block_requests);
        unwrap_and_report_state_sync_result!(block_sync_result);
        let start_height = if self.sync_status == SyncStatus::BlockSync {
            self.sync_progress.start_height
        } else {
            head.height
        };
        self.sync_progress =
            SyncProgress { start_height, current_height: head.height, highest_height };
        self.set_sync_status(SyncStatus::BlockSync);
        None
    }

    /// Pure decision: is this node far enough behind to need state sync?
    /// Checks if the head is 2+ epochs behind the header head.
    fn need_state_sync(&self, chain: &Chain, header_head: &Tip) -> Result<bool, near_chain::Error> {
        if self.config.archive || !self.config.state_sync_enabled {
            return Ok(false);
        }

        let head = chain.head()?;
        near_o11y::log_assert!(head.height <= header_head.height);

        // Only if the header head is more than one epoch ahead, then consider State Sync.
        // block_fetch_horizon is used for testing to prevent test nodes from switching to
        // State Sync too eagerly.
        let prefer_state_sync = head.epoch_id != header_head.epoch_id
            && head.next_epoch_id != header_head.epoch_id
            && head.height.saturating_add(self.config.block_fetch_horizon) < header_head.height;
        if prefer_state_sync {
            tracing::debug!(
                target: "sync",
                head_epoch_id = ?head.epoch_id,
                header_head_epoch_id = ?header_head.epoch_id,
                head_next_epoch_id = ?head.next_epoch_id,
                head_height = head.height,
                header_head_height = header_head.height,
                block_fetch_horizon = self.config.block_fetch_horizon,
                "switched from block sync to state sync");
        }
        Ok(prefer_state_sync)
    }

    /// Runs state sync: update status, request blocks, download state, reset heads.
    fn run_state_sync(
        &mut self,
        chain: &mut Chain,
        shard_tracker: &ShardTracker,
        highest_height_peers: &[HighestHeightPeerInfo],
        apply_chunks_done_sender: Option<ApplyChunksDoneSender>,
    ) -> Option<SyncHandlerRequest> {
        let update_sync_status_result = self.update_sync_status(chain);
        unwrap_and_report_state_sync_result!(update_sync_status_result);

        let sync_hash = match &self.state_sync_status {
            Some(s) => s.sync_hash,
            // sync hash isn't known yet. Return and try again later.
            None => return None,
        };

        let block_header = chain.get_block_header(&sync_hash);
        let block_header = unwrap_and_report_state_sync_result!(block_header);
        let shards_to_sync = get_shards_cares_about_this_or_next_epoch(
            &block_header,
            &shard_tracker,
            chain.epoch_manager.as_ref(),
        );

        let blocks_to_request =
            self.request_sync_blocks(chain, &block_header, highest_height_peers);

        let state_sync_status = self
            .state_sync_status
            .as_mut()
            .expect("state_sync_status should be Some when in StateSync");

        // Waiting for all the sync blocks to be available because they are
        // needed to finalize state sync.
        if !blocks_to_request.is_empty() {
            tracing::debug!(target: "sync", ?blocks_to_request, "waiting for sync blocks");
            return Some(SyncHandlerRequest::NeedRequestBlocks(blocks_to_request));
        }

        let state_sync_result = self.state_sync.run(sync_hash, state_sync_status, &shards_to_sync);
        let state_sync_result = unwrap_and_report_state_sync_result!(state_sync_result);
        if matches!(state_sync_result, StateSyncResult::InProgress) {
            return None;
        }

        tracing::info!(target: "sync", "state sync: all shards are done");
        let mut block_processing_artifacts = BlockProcessingArtifact::default();

        let reset_heads_result = chain.reset_heads_post_state_sync(
            sync_hash,
            &mut block_processing_artifacts,
            apply_chunks_done_sender,
        );
        unwrap_and_report_state_sync_result!(reset_heads_result);
        self.set_sync_status(SyncStatus::StateSyncDone);

        Some(SyncHandlerRequest::NeedProcessBlockArtifact(block_processing_artifacts))
    }

    /// Update sync status to StateSync and reset data if needed.
    fn update_sync_status(&mut self, chain: &mut Chain) -> Result<(), near_chain::Error> {
        if self.state_sync_status.is_some() {
            return Ok(());
        }

        let sync_hash = if let Some(sync_hash) = chain.find_sync_hash()? {
            sync_hash
        } else {
            return Ok(());
        };
        if !self.config.archive {
            let runtime_adapter = chain.runtime_adapter.clone();
            let epoch_manager = chain.epoch_manager.clone();
            chain.mut_chain_store().reset_data_pre_state_sync(
                sync_hash,
                runtime_adapter,
                epoch_manager,
            )?;
        }
        self.state_sync_status = Some(StateSyncStatus::new(sync_hash));
        self.set_sync_status(SyncStatus::StateSync);
        self.last_time_sync_block_requested.clear();
        Ok(())
    }

    /// Verifies if the node possesses the given block. If the block is absent,
    /// the node should request it from peers.
    ///
    /// the return value is a tuple (request_block, have_block)
    ///
    /// the return value (false, false) means that the node already requested
    /// the block but hasn't received it yet
    fn sync_block_status(
        &self,
        chain: &Chain,
        sync_hash: &CryptoHash,
        block_hash: &CryptoHash,
        now: Utc,
    ) -> (bool, bool) {
        // The sync hash block is saved as an orphan. The other blocks are saved
        // as regular blocks. Check if block exists depending on that.
        let block_exists = if sync_hash == block_hash {
            chain.is_orphan(block_hash)
        } else {
            chain.block_exists(block_hash)
        };

        if block_exists {
            return (false, true);
        }
        let timeout = self.config.state_sync_external_timeout;
        let timeout = near_async::time::Duration::try_from(timeout);
        let timeout = timeout.unwrap();

        let Some(last_time) = self.last_time_sync_block_requested.get(block_hash) else {
            return (true, false);
        };

        if (now - *last_time) >= timeout {
            tracing::error!(
                target: "sync",
                %block_hash,
                ?timeout,
                "state sync: block request timed out"
            );
            (true, false)
        } else {
            (false, false)
        }
    }

    /// Checks if the sync blocks are available and requests them if needed.
    /// Returns true if all the blocks are available.
    fn request_sync_blocks(
        &mut self,
        chain: &Chain,
        block_header: &BlockHeader,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Vec<(CryptoHash, PeerId)> {
        let now = self.clock.now_utc();

        let sync_hash = *block_header.hash();
        let prev_hash = *block_header.prev_hash();

        let mut needed_block_hashes = vec![prev_hash, sync_hash];
        let mut extra_block_hashes = chain.get_extra_sync_block_hashes(&prev_hash);
        tracing::trace!(target: "sync", ?needed_block_hashes, ?extra_block_hashes, "request_sync_blocks: block hashes for state sync");
        needed_block_hashes.append(&mut extra_block_hashes);
        let mut blocks_to_request = vec![];

        for hash in needed_block_hashes.clone() {
            let (request_block, have_block) = self.sync_block_status(chain, &sync_hash, &hash, now);
            tracing::trace!(target: "sync", ?hash, ?request_block, ?have_block, "request_sync_blocks");

            if have_block {
                self.last_time_sync_block_requested.remove(&hash);
            }

            if !request_block {
                tracing::trace!(target: "sync", ?hash, ?have_block, "request_sync_blocks: skipping - no request");
                continue;
            }

            let peer_info = highest_height_peers.choose(&mut thread_rng());
            let Some(peer_info) = peer_info else {
                tracing::trace!(target: "sync", ?hash, "request_sync_blocks: skipping - no peer");
                continue;
            };
            let peer_id = peer_info.peer_info.id.clone();
            self.last_time_sync_block_requested.insert(hash, now);
            blocks_to_request.push((hash, peer_id));
        }

        tracing::trace!(target: "sync", num_blocks_to_request = blocks_to_request.len(), "request_sync_blocks: done");

        blocks_to_request
    }
}
