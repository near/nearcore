//! V2 sync handler — single linear pipeline.
//!
//!   EpochSync → HeaderSync → StateSync → BlockSync → NoSync
//!
//! - **Near horizon** (within `epoch_sync_horizon`): enters at BlockSync.
//!   Header sync runs alongside block sync; block sync self-paces via
//!   the `NextBlockHashes` chain.
//! - **Far horizon** (beyond `epoch_sync_horizon`): enters at EpochSync.
//!   After epoch sync completes, headers advance past the epoch boundary,
//!   state sync downloads state at the sync hash, and finally block sync
//!   catches up.
//!
//! Gated behind `ProtocolFeature::SyncV2` (version 151).

use super::handler::{SyncHandler, SyncHandlerRequest};
use super::state::StateSyncResult;
use near_chain::Chain;
use near_chain::chain::ApplyChunksDoneSender;
use near_client_primitives::types::{EpochSyncStatus, StateSyncStatus, SyncStatus};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::types::HighestHeightPeerInfo;

impl SyncHandler {
    pub(crate) fn handle_sync_needed_v2(
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
                let header_head = chain.header_head()?;
                self.sync_status.update(SyncStatus::HeaderSync {
                    start_height: header_head.height,
                    current_height: header_head.height,
                    highest_height,
                });
            }
            SyncStatus::EpochSync(s) => {
                self.epoch_sync.run_v2(s, highest_height_peers)?;
            }
            SyncStatus::HeaderSync { current_height, highest_height: hh, .. } => {
                self.header_sync.run_v2(chain, highest_height, highest_height_peers)?;
                self.header_sync.check_and_ban_stalling_peer();

                let header_head = chain.header_head()?;
                *current_height = header_head.height;
                *hh = highest_height;

                // Transition to state sync once headers are within block_header_fetch_horizon
                // of the network tip.
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
                // Header sync and block sync run together. Header sync extends
                // the NextBlockHashes chain; block sync follows it via
                // get_next_block_hash(). When block sync reaches the end,
                // it gets DBNotFoundErr and retries after a 2s timeout.
                // Exit from sync is handled by run_sync_step() detecting AlreadyCaughtUp.
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
