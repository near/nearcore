use near_async::messaging::CanSend;
use near_async::time::{Clock, Duration, Utc};
use near_chain::Chain;
use near_chain::{check_known, ChainStoreAccess};
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
    fn block_sync(
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

        let mut num_requests = 0;
        for (height, hash) in requests {
            let request_from_archival = header_head.height.saturating_sub(height) > 120000;
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
    fn block_sync_due(&mut self, head: &Tip, header_head: &Tip) -> BlockSyncDue {
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use near_async::messaging::IntoMultiSender;
    use near_chain::test_utils::wait_for_all_blocks_in_processing;
    use near_chain::Provenance;
    use near_chain_configs::GenesisConfig;
    use near_crypto::{KeyType, PublicKey};
    use near_network::test_utils::MockPeerManagerAdapter;
    use near_o11y::testonly::TracingCapture;

    use near_primitives::network::PeerId;
    use near_primitives::utils::MaybeValidated;

    use super::*;
    use crate::test_utils::TestEnv;
    use near_network::types::PeerInfo;

    use std::collections::HashSet;

    /// Helper function for block sync tests
    fn collect_hashes_from_network_adapter(
        network_adapter: &MockPeerManagerAdapter,
    ) -> HashSet<CryptoHash> {
        let mut network_request = network_adapter.requests.write().unwrap();
        network_request
            .drain(..)
            .map(|request| match request {
                PeerManagerMessageRequest::NetworkRequests(NetworkRequests::BlockRequest {
                    hash,
                    ..
                }) => hash,
                _ => panic!("unexpected network request {:?}", request),
            })
            .collect()
    }

    fn check_hashes_from_network_adapter(
        network_adapter: &MockPeerManagerAdapter,
        expected_hashes: Vec<CryptoHash>,
    ) {
        let collected_hashes = collect_hashes_from_network_adapter(network_adapter);
        assert_eq!(collected_hashes, expected_hashes.into_iter().collect::<HashSet<_>>());
    }

    fn create_highest_height_peer_infos(num_peers: usize) -> Vec<HighestHeightPeerInfo> {
        (0..num_peers)
            .map(|_| HighestHeightPeerInfo {
                peer_info: PeerInfo {
                    id: PeerId::new(PublicKey::empty(KeyType::ED25519)),
                    addr: None,
                    account_id: None,
                },
                genesis_id: Default::default(),
                highest_block_height: 0,
                highest_block_hash: Default::default(),
                tracked_shards: vec![],
                archival: false,
            })
            .collect()
    }

    #[test]
    fn test_block_sync() {
        let mut capture = TracingCapture::enable();
        let network_adapter = Arc::new(MockPeerManagerAdapter::default());
        let block_fetch_horizon = 10;
        let max_block_requests = 10;
        let mut block_sync = BlockSync::new(
            Clock::real(),
            network_adapter.as_multi_sender(),
            block_fetch_horizon,
            false,
            true,
        );
        let mut genesis_config = GenesisConfig::test(Clock::real());
        genesis_config.epoch_length = 100;
        let mut env =
            TestEnv::builder(&genesis_config).clients_count(2).mock_epoch_managers().build();
        let mut blocks = vec![];
        for i in 1..5 * max_block_requests + 1 {
            let block = env.clients[0].produce_block(i as u64).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block, Provenance::PRODUCED);
        }
        let block_headers = blocks.iter().map(|b| b.header().clone()).collect::<Vec<_>>();
        let peer_infos = create_highest_height_peer_infos(2);
        let mut challenges = vec![];
        env.clients[1].chain.sync_block_headers(block_headers, &mut challenges).unwrap();
        assert!(challenges.is_empty());

        // fetch three blocks at a time
        for i in 0..3 {
            block_sync.block_sync(&env.clients[1].chain, &peer_infos, max_block_requests).unwrap();

            let expected_blocks: Vec<_> =
                blocks[i * max_block_requests..(i + 1) * max_block_requests].to_vec();
            check_hashes_from_network_adapter(
                &network_adapter,
                expected_blocks.iter().map(|b| *b.hash()).collect(),
            );

            for block in expected_blocks {
                env.process_block(1, block, Provenance::NONE);
            }
        }

        // Now test when the node receives the block out of order
        // fetch the next three blocks
        block_sync.block_sync(&env.clients[1].chain, &peer_infos, max_block_requests).unwrap();
        check_hashes_from_network_adapter(
            &network_adapter,
            (3 * max_block_requests..4 * max_block_requests).map(|h| *blocks[h].hash()).collect(),
        );
        // assumes that we only get block[4*max_block_requests-1]
        let _ = env.clients[1].process_block_test(
            MaybeValidated::from(blocks[4 * max_block_requests - 1].clone()),
            Provenance::NONE,
        );

        // the next block sync should not request block[4*max_block_requests-1] again
        block_sync.block_sync(&env.clients[1].chain, &peer_infos, max_block_requests).unwrap();
        check_hashes_from_network_adapter(
            &network_adapter,
            (3 * max_block_requests..4 * max_block_requests - 1)
                .map(|h| *blocks[h].hash())
                .collect(),
        );

        // Receive all blocks. Should not request more. As an extra
        // complication, pause the processing of one block.
        env.pause_block_processing(&mut capture, blocks[4 * max_block_requests - 1].hash());
        for i in 3 * max_block_requests..5 * max_block_requests {
            let _ = env.clients[1]
                .process_block_test(MaybeValidated::from(blocks[i].clone()), Provenance::NONE);
        }

        block_sync.block_sync(&env.clients[1].chain, &peer_infos, max_block_requests).unwrap();
        let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
        assert!(requested_block_hashes.is_empty(), "{:?}", requested_block_hashes);

        // Now finish paused processing and sanity check that we
        // still are fully synced.
        env.resume_block_processing(blocks[4 * max_block_requests - 1].hash());
        wait_for_all_blocks_in_processing(&mut env.clients[1].chain);
        let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
        assert!(requested_block_hashes.is_empty(), "{:?}", requested_block_hashes);
    }

    #[test]
    fn test_block_sync_archival() {
        let network_adapter = Arc::new(MockPeerManagerAdapter::default());
        let block_fetch_horizon = 10;
        let max_block_requests = 10;
        let mut block_sync = BlockSync::new(
            Clock::real(),
            network_adapter.as_multi_sender(),
            block_fetch_horizon,
            true,
            true,
        );
        let mut genesis_config = GenesisConfig::test(Clock::real());
        genesis_config.epoch_length = 5;
        let mut env =
            TestEnv::builder(&genesis_config).clients_count(2).mock_epoch_managers().build();
        let mut blocks = vec![];
        for i in 1..41 {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block, Provenance::PRODUCED);
        }
        let block_headers = blocks.iter().map(|b| b.header().clone()).collect::<Vec<_>>();
        let peer_infos = create_highest_height_peer_infos(2);
        let mut challenges = vec![];
        env.clients[1].chain.sync_block_headers(block_headers, &mut challenges).unwrap();
        assert!(challenges.is_empty());

        block_sync.block_sync(&env.clients[1].chain, &peer_infos, max_block_requests).unwrap();
        let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
        // We don't have archival peers, and thus cannot request any blocks
        assert_eq!(requested_block_hashes, HashSet::new());

        let mut peer_infos = create_highest_height_peer_infos(2);
        for peer in peer_infos.iter_mut() {
            peer.archival = true;
        }

        block_sync.block_sync(&env.clients[1].chain, &peer_infos, max_block_requests).unwrap();
        let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
        assert_eq!(
            requested_block_hashes,
            blocks.iter().take(max_block_requests).map(|b| *b.hash()).collect::<HashSet<_>>()
        );
    }
}
