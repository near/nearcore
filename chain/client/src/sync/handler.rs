use super::block::BlockSync;
use super::epoch::EpochSync;
use super::header::HeaderSync;
use super::state::StateSync;
use crate::sync::state::StateSyncResult;
use near_chain::chain::ApplyChunksDoneSender;
use near_chain::types::Tip;
use near_chain::{BlockProcessingArtifact, Chain};
use near_chain_configs::ClientConfig;
use near_client_primitives::types::{EpochSyncStatus, StateSyncStatus, SyncStatus};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::types::HighestHeightPeerInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};

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
    /// Dispatches to v1 or v2 handler based on protocol version. When
    /// `ProtocolFeature::SyncV2` is enabled, the v2 logic runs.
    /// Otherwise, falls back to the legacy v1 handler below.
    pub fn handle_sync_needed(
        &mut self,
        chain: &mut Chain,
        shard_tracker: &ShardTracker,
        highest_height: u64,
        highest_height_peers: &[HighestHeightPeerInfo],
        apply_chunks_done_sender: Option<ApplyChunksDoneSender>,
    ) -> Option<SyncHandlerRequest> {
        if ProtocolFeature::SyncV2.enabled(PROTOCOL_VERSION) {
            match self.handle_sync_needed_v2(
                chain,
                shard_tracker,
                highest_height,
                highest_height_peers,
                apply_chunks_done_sender,
            ) {
                Ok(request) => return request,
                Err(err) => {
                    tracing::error!(target: "sync", ?err, "sync: error in v2 handler");
                    return None;
                }
            }
        }
        self.handle_sync_needed_v1(
            chain,
            shard_tracker,
            highest_height,
            highest_height_peers,
            apply_chunks_done_sender,
        )
    }

    fn handle_sync_needed_v1(
        &mut self,
        chain: &mut Chain,
        shard_tracker: &ShardTracker,
        highest_height: u64,
        highest_height_peers: &[HighestHeightPeerInfo],
        apply_chunks_done_sender: Option<ApplyChunksDoneSender>,
    ) -> Option<SyncHandlerRequest> {
        // Run epoch sync first; if this is applicable then nothing else is.
        let epoch_sync_result = self.epoch_sync.run(
            &mut self.sync_status,
            &chain,
            highest_height,
            &highest_height_peers,
        );
        unwrap_and_report_state_sync_result!(epoch_sync_result);

        // Run header sync as long as there are headers to catch up.
        let header_sync_result = self.header_sync.run(
            &mut self.sync_status,
            &chain,
            highest_height,
            &highest_height_peers,
        );
        unwrap_and_report_state_sync_result!(header_sync_result);
        // Only body / state sync if header height is close to the latest.
        let chain_header_head = chain.header_head();
        let header_head = unwrap_and_report_state_sync_result!(chain_header_head);

        // We should state sync if it's already started or if we have enough
        // headers and blocks. The should_state_sync method may run block sync.
        let should_state_sync =
            self.should_state_sync(chain, &header_head, highest_height, highest_height_peers);
        let should_state_sync = unwrap_and_report_state_sync_result!(should_state_sync);
        if !should_state_sync {
            return None;
        }
        let update_sync_status_result = self.update_sync_status(chain);
        unwrap_and_report_state_sync_result!(update_sync_status_result);

        let state_sync_status = match &mut self.sync_status {
            SyncStatus::StateSync(s) => s,
            // sync hash isn't known yet. Return and try again later.
            _ => return None,
        };

        let state_sync_result = self.state_sync.run(
            state_sync_status,
            shard_tracker,
            chain,
            highest_height_peers,
            apply_chunks_done_sender,
        );
        let state_sync_result = unwrap_and_report_state_sync_result!(state_sync_result);
        match state_sync_result {
            StateSyncResult::NeedBlocks(blocks) => {
                tracing::debug!(target: "sync", ?blocks, "waiting for sync blocks");
                Some(SyncHandlerRequest::NeedRequestBlocks(blocks))
            }
            StateSyncResult::InProgress => None,
            StateSyncResult::Completed(block_processing_artifacts) => {
                self.sync_status.update(SyncStatus::StateSyncDone);
                Some(SyncHandlerRequest::NeedProcessBlockArtifact(block_processing_artifacts))
            }
        }
    }

    /// Update sync status to StateSync and reset data if needed.
    fn update_sync_status(&mut self, chain: &mut Chain) -> Result<(), near_chain::Error> {
        if let SyncStatus::StateSync(_) = self.sync_status {
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
        let new_state_sync_status = StateSyncStatus::new(sync_hash);
        let new_sync_status = SyncStatus::StateSync(new_state_sync_status);
        self.sync_status.update(new_sync_status);
        Ok(())
    }

    /// This method returns whether we should move on to state sync. It may run
    /// block sync if state sync is not yet started and we have enough headers.
    fn should_state_sync(
        &mut self,
        chain: &Chain,
        header_head: &Tip,
        highest_height: u64,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<bool, near_chain::Error> {
        // State sync is already started, continue.
        if let SyncStatus::StateSync(_) = self.sync_status {
            return Ok(true);
        }

        // Check that we have enough headers to start block sync.
        let min_header_height =
            highest_height.saturating_sub(self.config.block_header_fetch_horizon);
        if header_head.height < min_header_height {
            return Ok(false);
        }

        if let Some(epoch_sync_boundary_block_header) =
            self.epoch_sync.my_own_epoch_sync_boundary_block_header()
        {
            let current_epoch_start =
                chain.epoch_manager.get_epoch_start_height(&header_head.last_block_hash)?;
            if &header_head.epoch_id == epoch_sync_boundary_block_header.epoch_id() {
                // We do not want to state sync into the same epoch that epoch sync bootstrapped us with,
                // because we're missing block headers before this epoch. Wait till we have a header in
                // the next epoch before starting state sync. (This is not a long process; epoch sync
                // should have picked an old enough epoch so that there is a new epoch already available;
                // we just need to download more headers.)
                return Ok(false);
            }
            if epoch_sync_boundary_block_header.height() + chain.transaction_validity_period()
                > current_epoch_start
            {
                // We also do not want to state sync, if by doing so we would not have enough headers to
                // perform transaction validity checks. Again, epoch sync should have picked an old
                // enough epoch to ensure that we would have enough headers if we just continued with
                // header sync.
                return Ok(false);
            }
        }

        let block_sync_result = self.block_sync.run(
            &mut self.sync_status,
            &chain,
            highest_height,
            highest_height_peers,
        )?;
        Ok(block_sync_result)
    }

    /// V2 sync handler — single linear pipeline.
    ///
    ///   EpochSync → HeaderSync → StateSync → BlockSync → NoSync
    ///
    /// - **Near horizon** (within `epoch_sync_horizon`): enters at BlockSync.
    ///   Header sync runs alongside block sync; block sync self-paces via
    ///   the `NextBlockHashes` chain.
    /// - **Far horizon** (beyond `epoch_sync_horizon`): enters at EpochSync.
    ///   After epoch sync completes, headers advance past the epoch boundary,
    ///   state sync downloads state at the sync hash, and finally block sync
    ///   catches up.
    fn handle_sync_needed_v2(
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
                self.epoch_sync.run_v2(epoch_sync_status, highest_height_peers)?;
            }
            SyncStatus::HeaderSync { current_height, highest_height: hh, .. } => {
                // Downloading headers towards the network tip.
                self.header_sync.run_v2(chain, highest_height, highest_height_peers)?;
                self.header_sync.check_and_ban_stalling_peer();

                let header_head = chain.header_head()?;
                *current_height = header_head.height;
                *hh = highest_height;

                // Once we have enough headers (within block_header_fetch_horizon of
                // highest_height), look up the sync hash and transition to state sync.
                let min_header_height =
                    highest_height.saturating_sub(self.config.block_header_fetch_horizon);
                if header_head.height >= min_header_height {
                    if let Some(sync_hash) = chain.find_sync_hash()? {
                        tracing::debug!(target: "sync", "sync: transition to state sync");
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
                    highest_height_peers,
                    apply_chunks_done_sender,
                )? {
                    StateSyncResult::NeedBlocks(blocks) => {
                        // NeedBlocks requests are forwarded to the caller so the client can fetch
                        // the blocks required by state sync.
                        tracing::debug!(num_blocks = blocks.len(), "v2: waiting for sync blocks");
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
                }
            }
            SyncStatus::BlockSync { current_height, highest_height: hh, .. } => {
                // Fetching remaining blocks to catch up to the network tip.
                // Header sync continues alongside block sync — it extends the
                // NextBlockHashes chain while block sync follows it. Exit from
                // sync is handled by run_sync_step() detecting AlreadyCaughtUp.
                self.header_sync.run_v2(chain, highest_height, highest_height_peers)?;
                self.block_sync.run_v2(chain, highest_height_peers)?;

                let head = chain.head()?;
                *current_height = head.height;
                *hh = highest_height;
            }
            status => unreachable!("unexpected sync status in handle_sync_needed_v2: {:?}", status),
        }
        Ok(None)
    }

    /// Decide whether to enter at EpochSync (far horizon) or BlockSync (near horizon).
    fn decide_initial_phase(
        &mut self,
        chain: &Chain,
        highest_height: u64,
    ) -> Result<(), near_chain::Error> {
        if self.epoch_sync.is_epoch_sync_needed(chain, highest_height)? {
            tracing::info!(highest_height, "v2: entering epoch sync (far horizon)");
            self.sync_status.update(SyncStatus::EpochSync(EpochSyncStatus::NotStarted));
        } else {
            let head = chain.head()?;
            self.sync_status.update(SyncStatus::BlockSync {
                start_height: head.height,
                current_height: head.height,
                highest_height,
            });
        }
        Ok(())
    }
}
