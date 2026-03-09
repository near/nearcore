use super::block::BlockSync;
use super::epoch::EpochSync;
use super::header::HeaderSync;
use super::state::StateSync;
use crate::sync::state::StateSyncResult;
use near_chain::chain::ApplyChunksDoneSender;
use near_chain::types::Tip;
use near_chain::{BlockProcessingArtifact, Chain};
use near_chain_configs::ClientConfig;
use near_client_primitives::types::{StateSyncStatus, SyncStatus};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::types::HighestHeightPeerInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;

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
    /// This method performs whatever syncing technique is needed (epoch sync, header sync,
    /// state sync, block sync) to make progress towards bring the node up to date.
    ///
    /// Returns true iff state sync is completed.
    pub fn handle_sync_needed(
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
            self.config.sync_max_block_requests,
        )?;
        Ok(block_sync_result)
    }
}
