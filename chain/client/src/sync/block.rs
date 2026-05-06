use near_async::messaging::CanSend;
use near_async::time::{Clock, Duration, Utc};
use near_chain::Chain;
use near_chain::ChainStoreAccess;
use near_chain::chain::BlockKnowledge;
use near_chain_primitives::error::Error;
use near_client_primitives::types::SyncStatus;
use near_network::types::PeerManagerMessageRequest;
use near_network::types::{HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter};
use near_o11y::log_assert;
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, BlockHeightDelta};
use near_primitives::version::ProtocolFeature;
use rand::seq::IteratorRandom;
use tracing::instrument;

/// Expect to receive the requested block in this time.
const BLOCK_REQUEST_TIMEOUT_MS: i64 = 2_000;

#[derive(Clone)]
pub struct BlockSyncRequest {
    // Head of the chain at the time of the last requests.
    head: CryptoHash,
    // When the last requests were made.
    when: Utc,
}

/// Helper to track block syncing.
pub struct BlockSync {
    clock: Clock,

    network_adapter: PeerManagerAdapter,

    // When the last block requests were made.
    last_request: Option<BlockSyncRequest>,

    /// How far to fetch blocks vs fetch state.
    block_fetch_horizon: BlockHeightDelta,

    /// Archival nodes are not allowed to do State Sync, as they need all state from all blocks.
    archive: bool,

    /// Whether State Sync should be enabled when a node falls far enough behind.
    state_sync_enabled: bool,

    /// Maximum number of blocks to request in a single batch.
    max_block_requests: usize,
}

impl BlockSync {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        block_fetch_horizon: BlockHeightDelta,
        archive: bool,
        state_sync_enabled: bool,
        max_block_requests: usize,
    ) -> Self {
        BlockSync {
            clock,
            network_adapter,
            last_request: None,
            block_fetch_horizon,
            archive,
            state_sync_enabled,
            max_block_requests,
        }
    }

    /// Returns true if State Sync is needed.
    /// Returns false is Block Sync is needed. Maybe requests a few blocks from peers.
    pub fn run(
        &mut self,
        sync_status: &mut SyncStatus,
        chain: &Chain,
        highest_height: BlockHeight,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<bool, near_chain::Error> {
        let _span =
            tracing::debug_span!(target: "sync", "run_sync", sync_type = "BlockSync").entered();
        let head = chain.head()?;
        let header_head = chain.header_head()?;
        let (head_for_state_check, head_is_spice) =
            effective_head_for_state_sync_trigger(chain, &head)?;

        match self.block_sync_due(&head, &head_for_state_check, head_is_spice, &header_head) {
            BlockSyncDue::StateSync => {
                tracing::debug!(target: "sync", "sync: transition to state sync");
                return Ok(true);
            }
            BlockSyncDue::RequestBlock => {
                self.block_sync(chain, highest_height_peers)?;
            }
            BlockSyncDue::WaitForBlock => {
                // Do nothing.
            }
        }

        // start_height is used to report the progress of state sync, e.g. to say that it's 50% complete.
        // This number has no other functional value.
        let start_height = sync_status.start_height().unwrap_or(head.height);

        sync_status.update(SyncStatus::BlockSync {
            start_height,
            current_height: head.height,
            highest_height,
        });
        Ok(false)
    }

    /// Check if state download is required.
    ///
    /// `head_is_spice` is true when the supplied `head` is the SPICE execution
    /// head rather than the consensus head. Under SPICE the execution head can
    /// fall behind the consensus head while consensus continues to advance, so
    /// the lag we actually care about is the executor's lag — and even a
    /// single epoch of executor lag warrants state sync (block sync wouldn't
    /// help: we already have the blocks, we need their state). Under non-SPICE
    /// the same indicator is structurally equivalent and we keep the original
    /// 2-epoch threshold to avoid switching modes unnecessarily.
    fn check_state_needed(&self, head: &Tip, head_is_spice: bool, header_head: &Tip) -> bool {
        if self.archive || !self.state_sync_enabled {
            return false;
        }

        log_assert!(head.height <= header_head.height);

        let prefer_state_sync = if head_is_spice {
            head.epoch_id != header_head.epoch_id
                && head.height.saturating_add(self.block_fetch_horizon) < header_head.height
        } else {
            // Only if the header head is more than one epoch ahead, then consider State Sync.
            // block_fetch_horizon is used for testing to prevent test nodes from switching to State Sync too eagerly.
            head.epoch_id != header_head.epoch_id
                && head.next_epoch_id != header_head.epoch_id
                && head.height.saturating_add(self.block_fetch_horizon) < header_head.height
        };
        if prefer_state_sync {
            tracing::debug!(
                target: "sync",
                head_epoch_id = ?head.epoch_id,
                header_head_epoch_id = ?header_head.epoch_id,
                head_next_epoch_id = ?head.next_epoch_id,
                head_height = head.height,
                header_head_height = header_head.height,
                block_fetch_horizon = self.block_fetch_horizon,
                "switched from block sync to state sync");
        }
        prefer_state_sync
    }

    // Finds the last block on the canonical chain that is in store (processed).
    fn get_last_processed_block(&self, chain: &Chain) -> Result<CryptoHash, near_chain::Error> {
        // TODO: Can this function be replaced with `Chain::get_latest_known()`?
        // The current chain head may not be on the canonical chain.
        // Now we find the most recent block we know on the canonical chain.
        // In practice the forks from the last final block are very short, so it is
        // acceptable to perform this on each request.

        let head = chain.head()?;
        let mut header = chain.get_block_header(&head.last_block_hash)?;
        // First go back until we find the common block
        while match chain.get_block_header_by_height(header.height()) {
            Ok(got_header) => got_header.hash() != header.hash(),
            Err(e) => match e {
                near_chain::Error::DBNotFoundErr(_) => true,
                _ => return Err(e),
            },
        } {
            header = chain.get_block_header(header.prev_hash())?;
        }

        // Then go forward for as long as we know the next block
        let mut hash = *header.hash();
        loop {
            match chain.chain_store().get_next_block_hash(&hash) {
                Ok(got_hash) => {
                    if chain.block_exists(&got_hash) {
                        hash = got_hash;
                    } else {
                        break;
                    }
                }
                Err(e) => match e {
                    near_chain::Error::DBNotFoundErr(_) => break,
                    _ => return Err(e),
                },
            }
        }

        Ok(hash)
    }

    /// Request recent blocks from a randomly chosen peer.
    /// pub for testing
    #[instrument(
        target = "sync",
        level = "debug",
        skip_all,
        fields(head_last_block_hash, head_height, header_head_height, num_requests)
    )]
    pub fn block_sync(
        &mut self,
        chain: &Chain,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<(), near_chain::Error> {
        // Update last request now because we want to update it whether or not
        // the rest of the logic succeeds.
        // TODO: If this code fails we should retry ASAP. Shouldn't we?
        let chain_head = chain.head()?;
        let header_head = chain.header_head()?;
        let span = tracing::Span::current();
        span.record("head_last_block_hash", tracing::field::debug(chain_head.last_block_hash));
        span.record("head_height", chain_head.height);
        span.record("header_head_height", header_head.height);

        self.last_request =
            Some(BlockSyncRequest { head: chain_head.last_block_hash, when: self.clock.now_utc() });

        // The last block on the canonical chain that is processed (is in store).
        let reference_hash = self.get_last_processed_block(chain)?;

        // Assume that peers are configured to keep as many epochs does this
        // node and expect peers to have blocks in the range
        // [gc_stop_height, header_head.last_block_hash].
        let gc_stop_height = chain.runtime_adapter.get_gc_stop_height(&header_head.last_block_hash);

        // Look ahead for max_block_requests block headers and add requests for
        // blocks that we don't have yet.
        let mut next_hash = reference_hash;
        let mut num_requests = 0;
        for _ in 0..self.max_block_requests {
            next_hash = match chain.chain_store().get_next_block_hash(&next_hash) {
                Ok(hash) => hash,
                Err(e) => match e {
                    near_chain::Error::DBNotFoundErr(_) => {
                        tracing::debug!(
                            target: "sync",
                            block_hash = ?next_hash,
                            "next block hash is not found"
                        );
                        break;
                    }
                    _ => return Err(e),
                },
            };
            if let BlockKnowledge::Known(err) = chain.check_block_known(&next_hash) {
                tracing::debug!(
                    target: "sync",
                    block_hash = ?next_hash,
                    ?err,
                    "block is known"
                );
                continue;
            }

            let next_height = chain.get_block_header(&next_hash)?.height();
            let request_from_archival = self.archive && next_height < gc_stop_height;
            // Assume that heads of `highest_height_peers` are ahead of the blocks we're requesting.
            let peer = if request_from_archival {
                // Normal peers are unlikely to have old blocks, request from an archival node.
                let archival_peer_iter = highest_height_peers.iter().filter(|p| p.archival);
                archival_peer_iter.choose(&mut rand::thread_rng())
            } else {
                // All peers are likely to have this block.
                let peer_iter = highest_height_peers.iter();
                peer_iter.choose(&mut rand::thread_rng())
            };

            if let Some(peer) = peer {
                tracing::debug!(
                    target: "sync",
                    block_hash = ?next_hash,
                    block_height = next_height,
                    request_from_archival,
                    peer = ?peer.peer_info.id,
                    num_peers = highest_height_peers.len(),
                    "requested block"
                );
                self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::BlockRequest {
                        hash: next_hash,
                        peer_id: peer.peer_info.id.clone(),
                    },
                ));
                num_requests += 1;
            } else {
                tracing::warn!(
                    target: "sync",
                    block_hash = ?next_hash,
                    block_height = next_height,
                    request_from_archival,
                    "no available peers to request a block from");
            }
        }
        span.record("num_requests", num_requests);
        Ok(())
    }

    /// Request blocks from peers if a request is due (head changed or
    /// timeout elapsed). Does not check whether state sync is needed
    /// and does not update `SyncStatus`.
    pub fn run_v2(
        &mut self,
        chain: &Chain,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<(), near_chain::Error> {
        let head = chain.head()?;
        match self.block_request_due(&head) {
            BlockSyncDue::RequestBlock => {
                self.block_sync(chain, highest_height_peers)?;
            }
            BlockSyncDue::WaitForBlock => {}
            BlockSyncDue::StateSync => unreachable!("block_request_due never returns StateSync"),
        }
        Ok(())
    }

    /// Returns whether block sync should request new blocks, wait, or
    /// yield to state sync. Checks `state_needed` first against
    /// `head_for_state_check` (which under SPICE is the executor head, not
    /// the consensus head — see `effective_head_for_state_sync_trigger`),
    /// then delegates to `block_request_due()` for the timeout / head-freshness
    /// check (which uses the consensus head, since that's what tracks block
    /// download progress).
    fn block_sync_due(
        &self,
        head: &Tip,
        head_for_state_check: &Tip,
        head_is_spice: bool,
        header_head: &Tip,
    ) -> BlockSyncDue {
        if self.check_state_needed(head_for_state_check, head_is_spice, header_head) {
            return BlockSyncDue::StateSync;
        }
        self.block_request_due(head)
    }

    /// Returns whether a new block request is due based on head freshness
    /// and request timeout. Does not check whether state sync is needed.
    fn block_request_due(&self, head: &Tip) -> BlockSyncDue {
        match &self.last_request {
            None => {
                // Request the next block.
                BlockSyncDue::RequestBlock
            }
            Some(request) => {
                // Head got updated, no need to continue waiting for the requested block.
                // TODO: This doesn't work nicely with a node requesting config.max_blocks_requests blocks at a time.
                // TODO: Does receiving a response to one of those requests cancel and restart the other requests?
                let head_got_updated = head.last_block_hash != request.head;
                // Timeout elapsed
                let timeout = self.clock.now_utc() - request.when
                    > Duration::milliseconds(BLOCK_REQUEST_TIMEOUT_MS);
                if head_got_updated || timeout {
                    // Request the next block.
                    BlockSyncDue::RequestBlock
                } else {
                    // Continue waiting for the currently requested block.
                    BlockSyncDue::WaitForBlock
                }
            }
        }
    }
}

/// Whether a new set of blocks needs to be requested.
enum BlockSyncDue {
    /// Request the next block.
    RequestBlock,
    /// The block is already requested, wait for it.
    WaitForBlock,
    /// Too far behind, drop BlockSync and do StateSync instead.
    StateSync,
}

/// Returns the head to compare against `header_head` when deciding whether
/// state sync should fire, plus a flag indicating whether SPICE-specific
/// trigger semantics apply.
///
/// Under non-SPICE epochs the consensus head and the post-execution head are
/// the same value (`chain.head()` only advances when blocks are applied). So
/// the existing `head` is the right comparison point and the original 2-epoch
/// threshold remains correct.
///
/// Under SPICE epochs the consensus head advances independently of execution.
/// A validator that lacks state for a newly-assigned shard sees its
/// `spice_execution_head` stall while the consensus head keeps moving with the
/// network — `head` and `header_head` would stay close, and state sync would
/// never trigger. Using `spice_execution_head` here makes the executor's lag
/// visible to `check_state_needed`. The SPICE flag also relaxes the lag
/// threshold from "2 epochs" to "1 epoch": block sync can't fix executor lag,
/// so as soon as we're a full epoch behind in execution we want state sync.
fn effective_head_for_state_sync_trigger(chain: &Chain, head: &Tip) -> Result<(Tip, bool), Error> {
    let protocol_version = chain.epoch_manager.get_epoch_protocol_version(&head.epoch_id)?;
    if !ProtocolFeature::Spice.enabled(protocol_version) {
        return Ok((head.clone(), false));
    }
    match chain.chain_store().spice_execution_head() {
        Ok(exec_head) => Ok(((*exec_head).clone(), true)),
        // No execution head yet (e.g. fresh node still bootstrapping). Fall
        // back to the consensus head; state sync isn't applicable yet anyway.
        Err(Error::DBNotFoundErr(_)) => Ok((head.clone(), false)),
        Err(err) => Err(err),
    }
}
