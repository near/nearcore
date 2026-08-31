use crate::sync::peers::{PeerAdvertisedHead, PeerSelector};
use near_async::messaging::CanSend;
use near_async::time::{Clock, Duration, Utc};
use near_chain::Chain;
use near_chain::ChainStoreAccess;
use near_chain::chain::BlockKnowledge;
use near_network::types::PeerManagerMessageRequest;
use near_network::types::{NetworkRequests, PeerManagerAdapter};
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
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

    /// Archival nodes are not allowed to do State Sync, as they need all state from all blocks.
    archive: bool,

    /// Maximum number of blocks to request in a single batch.
    max_block_requests: usize,
}

impl BlockSync {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        archive: bool,
        max_block_requests: usize,
    ) -> Self {
        BlockSync { clock, network_adapter, last_request: None, archive, max_block_requests }
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
        peers_ahead: &[PeerAdvertisedHead],
        peer_selector: &mut PeerSelector,
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
            // Assume that heads of `peers_ahead` are ahead of the blocks we are requesting.
            let now = self.clock.now_utc();
            let peer = if request_from_archival {
                // Normal peers are unlikely to have old blocks, request from an archival node.
                peer_selector.pick_matching(peers_ahead, now, |peer| peer.archival)
            } else {
                // All peers are likely to have this block.
                peer_selector.pick(peers_ahead, now)
            };

            if let Some(peer) = peer {
                tracing::debug!(
                    target: "sync",
                    block_hash = ?next_hash,
                    block_height = next_height,
                    request_from_archival,
                    peer = ?peer.peer_info.id,
                    num_peers = peers_ahead.len(),
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
    /// timeout elapsed). Does not update `SyncStatus`.
    pub fn run(
        &mut self,
        chain: &Chain,
        peers_ahead: &[PeerAdvertisedHead],
        peer_selector: &mut PeerSelector,
    ) -> Result<(), near_chain::Error> {
        let head = chain.head()?;
        if !self.block_request_due(&head) {
            return Ok(());
        }
        self.block_sync(chain, peers_ahead, peer_selector)
    }

    /// Returns whether a new block request is due based on head freshness
    /// and request timeout.
    fn block_request_due(&self, head: &Tip) -> bool {
        let Some(request) = &self.last_request else {
            // No request yet — issue the first one.
            return true;
        };
        // Head got updated, no need to continue waiting for the requested block.
        // TODO: This doesn't work nicely with a node requesting config.max_blocks_requests blocks at a time.
        // TODO: Does receiving a response to one of those requests cancel and restart the other requests?
        let head_got_updated = head.last_block_hash != request.head;
        let timeout =
            self.clock.now_utc() - request.when > Duration::milliseconds(BLOCK_REQUEST_TIMEOUT_MS);
        head_got_updated || timeout
    }
}
