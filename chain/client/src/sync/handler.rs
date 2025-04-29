use super::block::BlockSync;
use super::epoch::EpochSync;
use super::header::HeaderSync;
use super::state::StateSync;
use crate::sync::state::StateSyncResult;
use near_async::time::{Clock, Utc};
use near_chain::chain::ApplyChunksDoneMessage;
use near_chain::types::Tip;
use near_chain::{BlockHeader, BlockProcessingArtifact, Chain};
use near_chain_configs::ClientConfig;
use near_chunks::logic::get_shards_cares_about_this_or_next_epoch;
use near_client_primitives::types::{StateSyncStatus, SyncStatus};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::types::HighestHeightPeerInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::validator_signer::ValidatorSigner;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::sync::Arc;

// A small helper macro to unwrap a result of some state sync operation. If the
// result is an error this macro will log it and return from the function.
#[macro_export]
macro_rules! unwrap_and_report_state_sync_result (($obj: ident) => (match $obj {
    Ok(v) => v,
    Err(err) => {
        tracing::error!(target: "sync", "Sync: Unexpected error: {}: {}", stringify!($obj), err);
        return None;
    }
}));

/// Handles syncing chain to the actual state of the network.
pub struct SyncHandler {
    clock: Clock,
    config: ClientConfig,
    pub sync_status: SyncStatus,
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
            last_time_sync_block_requested: HashMap::new(),
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
        signer: &Option<Arc<ValidatorSigner>>,
        apply_chunks_done_sender: Option<near_async::messaging::Sender<ApplyChunksDoneMessage>>,
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
            self.should_state_sync(chain, header_head, highest_height, highest_height_peers);
        let should_state_sync = unwrap_and_report_state_sync_result!(should_state_sync);
        if !should_state_sync {
            return None;
        }
        let update_sync_status_result = self.update_sync_status(chain);
        unwrap_and_report_state_sync_result!(update_sync_status_result);

        let sync_hash = match &self.sync_status {
            SyncStatus::StateSync(s) => s.sync_hash,
            // sync hash isn't known yet. Return and try again later.
            _ => return None,
        };

        let me = signer.as_ref().map(|x| x.validator_id().clone());
        let block_header = chain.get_block_header(&sync_hash);
        let block_header = unwrap_and_report_state_sync_result!(block_header);
        let shards_to_sync = get_shards_cares_about_this_or_next_epoch(
            me.as_ref(),
            true,
            &block_header,
            &shard_tracker,
            chain.epoch_manager.as_ref(),
        );

        let blocks_to_request = self.request_sync_blocks(chain, block_header, highest_height_peers);
        let blocks_to_request = unwrap_and_report_state_sync_result!(blocks_to_request);

        let state_sync_status = match &mut self.sync_status {
            SyncStatus::StateSync(s) => s,
            _ => unreachable!("Sync status should have been StateSync!"),
        };

        // Waiting for all the sync blocks to be available because they are
        // needed to finalize state sync.
        if !blocks_to_request.is_empty() {
            tracing::debug!(target: "sync", ?blocks_to_request, "waiting for sync blocks");
            return Some(SyncHandlerRequest::NeedRequestBlocks(blocks_to_request));
        }

        let state_sync_result = self.state_sync.run(
            sync_hash,
            state_sync_status,
            &highest_height_peers,
            &shards_to_sync,
        );
        let state_sync_result = unwrap_and_report_state_sync_result!(state_sync_result);
        if matches!(state_sync_result, StateSyncResult::InProgress) {
            return None;
        }

        tracing::info!(target: "sync", "State sync: all shards are done");
        let mut block_processing_artifacts = BlockProcessingArtifact::default();

        let reset_heads_result = chain.reset_heads_post_state_sync(
            &me,
            sync_hash,
            &mut block_processing_artifacts,
            apply_chunks_done_sender,
        );
        unwrap_and_report_state_sync_result!(reset_heads_result);
        self.sync_status.update(SyncStatus::StateSyncDone);

        Some(SyncHandlerRequest::NeedProcessBlockArtifact(block_processing_artifacts))
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
        self.last_time_sync_block_requested.clear();
        Ok(())
    }

    /// This method returns whether we should move on to state sync. It may run
    /// block sync if state sync is not yet started and we have enough headers.
    fn should_state_sync(
        &mut self,
        chain: &Chain,
        header_head: Tip,
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
    ) -> Result<(bool, bool), near_chain::Error> {
        // The sync hash block is saved as an orphan. The other blocks are saved
        // as regular blocks. Check if block exists depending on that.
        let block_exists = if sync_hash == block_hash {
            chain.is_orphan(block_hash)
        } else {
            chain.block_exists(block_hash)?
        };

        if block_exists {
            return Ok((false, true));
        }
        let timeout = self.config.state_sync_external_timeout;
        let timeout = near_async::time::Duration::try_from(timeout);
        let timeout = timeout.unwrap();

        let Some(last_time) = self.last_time_sync_block_requested.get(block_hash) else {
            return Ok((true, false));
        };

        if (now - *last_time) >= timeout {
            tracing::error!(
                target: "sync",
                %block_hash,
                ?timeout,
                "State sync: block request timed out"
            );
            Ok((true, false))
        } else {
            Ok((false, false))
        }
    }

    /// Checks if the sync blocks are available and requests them if needed.
    /// Returns true if all the blocks are available.
    fn request_sync_blocks(
        &mut self,
        chain: &Chain,
        block_header: BlockHeader,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<Vec<(CryptoHash, PeerId)>, near_chain::Error> {
        let now = self.clock.now_utc();

        let sync_hash = *block_header.hash();
        let prev_hash = *block_header.prev_hash();

        let mut needed_block_hashes = vec![prev_hash, sync_hash];
        let mut extra_block_hashes = chain.get_extra_sync_block_hashes(&prev_hash);
        tracing::trace!(target: "sync", ?needed_block_hashes, ?extra_block_hashes, "request_sync_blocks: block hashes for state sync");
        needed_block_hashes.append(&mut extra_block_hashes);
        let mut blocks_to_request = vec![];

        for hash in needed_block_hashes.clone() {
            let (request_block, have_block) =
                self.sync_block_status(chain, &sync_hash, &hash, now)?;
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

        Ok(blocks_to_request)
    }
}
