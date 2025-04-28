use near_async::messaging::CanSend;
use near_async::time::{Clock, Duration, Utc};
use near_chain::Chain;
use near_chain::{ChainStoreAccess, check_known};
use near_client_primitives::types::SyncStatus;
use near_network::types::PeerManagerMessageRequest;
use near_network::types::{HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter};
use near_o11y::log_assert;
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, BlockHeightDelta};
use rand::seq::IteratorRandom;
use tracing::{debug, warn};

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
}

impl BlockSync {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        block_fetch_horizon: BlockHeightDelta,
        archive: bool,
        state_sync_enabled: bool,
    ) -> Self {
        BlockSync {
            clock,
            network_adapter,
            last_request: None,
            block_fetch_horizon,
            archive,
            state_sync_enabled,
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
        max_block_requests: usize,
    ) -> Result<bool, near_chain::Error> {
        let _span =
            tracing::debug_span!(target: "sync", "run_sync", sync_type = "BlockSync").entered();
        let head = chain.head()?;
        let header_head = chain.header_head()?;

        match self.block_sync_due(&head, &header_head) {
            BlockSyncDue::StateSync => {
                debug!(target: "sync", "Sync: transition to State Sync.");
                return Ok(true);
            }
            BlockSyncDue::RequestBlock => {
                self.block_sync(chain, highest_height_peers, max_block_requests)?;
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

    /// Check if state download is required
    fn check_state_needed(&self, head: &Tip, header_head: &Tip) -> bool {
        if self.archive || !self.state_sync_enabled {
            return false;
        }

        log_assert!(head.height <= header_head.height);

        // Only if the header head is more than one epoch ahead, then consider State Sync.
        // block_fetch_horizon is used for testing to prevent test nodes from switching to State Sync too eagerly.
        let prefer_state_sync = head.epoch_id != header_head.epoch_id
            && head.next_epoch_id != header_head.epoch_id
            && head.height.saturating_add(self.block_fetch_horizon) < header_head.height;
        if prefer_state_sync {
            debug!(
                target: "sync",
                head_epoch_id = ?head.epoch_id,
                header_head_epoch_id = ?header_head.epoch_id,
                head_next_epoch_id = ?head.next_epoch_id,
                head_height = head.height,
                header_head_height = header_head.height,
                block_fetch_horizon = self.block_fetch_horizon,
                "Switched from block sync to state sync");
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
                    if chain.block_exists(&got_hash)? {
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
    pub fn block_sync(
        &mut self,
        chain: &Chain,
        highest_height_peers: &[HighestHeightPeerInfo],
        max_block_requests: usize,
    ) -> Result<(), near_chain::Error> {
        // Update last request now because we want to update it whether or not
        // the rest of the logic succeeds.
        // TODO: If this code fails we should retry ASAP. Shouldn't we?
        let chain_head = chain.head()?;
        self.last_request =
            Some(BlockSyncRequest { head: chain_head.last_block_hash, when: self.clock.now_utc() });

        // The last block on the canonical chain that is processed (is in store).
        let reference_hash = self.get_last_processed_block(chain)?;

        // Look ahead for max_block_requests block headers and add requests for
        // blocks that we don't have yet.
        let mut requests = vec![];
        let mut next_hash = reference_hash;
        for _ in 0..max_block_requests {
            match chain.chain_store().get_next_block_hash(&next_hash) {
                Ok(hash) => next_hash = hash,
                Err(e) => match e {
                    near_chain::Error::DBNotFoundErr(_) => break,
                    _ => return Err(e),
                },
            }
            if let Ok(_) = check_known(chain, &next_hash)? {
                let next_height = chain.get_block_header(&next_hash)?.height();
                requests.push((next_height, next_hash));
            }
        }

        let header_head = chain.header_head()?;
        // Assume that peers are configured to keep as many epochs does this
        // node and expect peers to have blocks in the range
        // [gc_stop_height, header_head.last_block_hash].
        let gc_stop_height = chain.runtime_adapter.get_gc_stop_height(&header_head.last_block_hash);

        let mut num_requests = 0;
        for (height, hash) in requests {
            let request_from_archival = self.archive && height < gc_stop_height;
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
                debug!(
                    target: "sync",
                    head_height = chain_head.height,
                    header_head_height = header_head.height,
                    block_hash = ?hash,
                    block_height = height,
                    request_from_archival,
                    peer = ?peer.peer_info.id,
                    num_peers = highest_height_peers.len(),
                    "Block sync: requested block"
                );
                self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::BlockRequest { hash, peer_id: peer.peer_info.id.clone() },
                ));
                num_requests += 1;
            } else {
                warn!(
                    target: "sync",
                    head_height = chain_head.height,
                    header_head_height = header_head.height,
                    block_hash = ?hash,
                    block_height = height,
                    request_from_archival,
                    "Block sync: No available peers to request a block from");
            }
        }
        debug!(
            target: "sync",
            head_height = chain_head.height,
            header_head_height = header_head.height,
            num_requests,
            "Block sync: requested blocks");
        Ok(())
    }

    /// Checks if we should run block sync and ask for more full blocks.
    /// Block sync is due either if the chain head has changed since the last request
    /// or if time since the last request is > BLOCK_REQUEST_TIMEOUT_MS
    fn block_sync_due(&self, head: &Tip, header_head: &Tip) -> BlockSyncDue {
        if self.check_state_needed(head, header_head) {
            return BlockSyncDue::StateSync;
        }
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
