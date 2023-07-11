use chrono::{DateTime, Duration, Utc};
use near_async::messaging::CanSend;
use near_chain::Chain;
use near_chain::{check_known, ChainStoreAccess};
use near_client_primitives::types::SyncStatus;
use near_network::types::PeerManagerMessageRequest;
use near_network::types::{HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter};
use near_primitives::hash::CryptoHash;
use near_primitives::static_clock::StaticClock;
use near_primitives::types::{BlockHeight, BlockHeightDelta};
use rand::seq::IteratorRandom;
use tracing::{debug, warn};

/// Maximum number of block requested at once in BlockSync
const MAX_BLOCK_REQUESTS: usize = 5;

const BLOCK_REQUEST_TIMEOUT: i64 = 2;

#[derive(Clone)]
pub struct BlockSyncRequest {
    // head of the chain at the time of the request
    head: CryptoHash,
    // when the block was requested
    when: DateTime<Utc>,
}

/// Helper to track block syncing.
pub struct BlockSync {
    network_adapter: PeerManagerAdapter,
    last_request: Option<BlockSyncRequest>,
    /// How far to fetch blocks vs fetch state.
    block_fetch_horizon: BlockHeightDelta,
    /// Whether to enforce block sync
    archive: bool,
    /// Whether State Sync should be enabled when a node falls far enough behind.
    state_sync_enabled: bool,
}

impl BlockSync {
    pub fn new(
        network_adapter: PeerManagerAdapter,
        block_fetch_horizon: BlockHeightDelta,
        archive: bool,
        state_sync_enabled: bool,
    ) -> Self {
        BlockSync {
            network_adapter,
            last_request: None,
            block_fetch_horizon,
            archive,
            state_sync_enabled,
        }
    }

    /// Runs check if block sync is needed, if it's needed and it's too far - sync state is started instead (returning true).
    /// Otherwise requests recent blocks from peers.
    pub fn run(
        &mut self,
        sync_status: &mut SyncStatus,
        chain: &Chain,
        highest_height: BlockHeight,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<bool, near_chain::Error> {
        let _span = tracing::debug_span!(target: "sync", "run", sync = "BlockSync").entered();
        if self.block_sync_due(chain)? {
            if self.block_sync(chain, highest_height_peers)? {
                debug!(target: "sync", "Sync: transition to State Sync.");
                return Ok(true);
            }
        }

        let head = chain.head()?;
        let start_height = match sync_status.start_height() {
            Some(height) => height,
            None => head.height,
        };
        *sync_status =
            SyncStatus::BodySync { start_height, current_height: head.height, highest_height };
        Ok(false)
    }

    /// Check if state download is required
    fn check_state_needed(&self, chain: &Chain) -> Result<bool, near_chain::Error> {
        let head = chain.head()?;
        let header_head = chain.header_head()?;

        // If latest block is up to date return early.
        // No state download is required, neither any blocks need to be fetched.
        if head.height >= header_head.height {
            return Ok(false);
        }

        // Don't run State Sync if header head is not more than one epoch ahead.
        if head.epoch_id != header_head.epoch_id && head.next_epoch_id != header_head.epoch_id {
            if head.height < header_head.height.saturating_sub(self.block_fetch_horizon)
                && !self.archive
                && self.state_sync_enabled
            {
                // Epochs are different and we are too far from horizon, State Sync is needed
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Returns true if state download is required (last known block is too far).
    /// Otherwise request recent blocks from peers round robin.
    fn block_sync(
        &mut self,
        chain: &Chain,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<bool, near_chain::Error> {
        if self.check_state_needed(chain)? {
            return Ok(true);
        }

        let chain_head = chain.head()?;
        // update last request now because we want to update it whether or not the rest of the logic
        // succeeds
        self.last_request =
            Some(BlockSyncRequest { head: chain_head.last_block_hash, when: StaticClock::utc() });

        // reference_hash is the last block on the canonical chain that is in store (processed)
        let reference_hash = {
            let reference_hash = chain_head.last_block_hash;

            // The current chain head may not be on the canonical chain.
            // Now we find the most recent block we know on the canonical chain.
            // In practice the forks from the last final block are very short, so it is
            // acceptable to perform this on each request
            let header = chain.get_block_header(&reference_hash)?;
            let mut candidate = (header.height(), *header.hash(), *header.prev_hash());

            // First go back until we find the common block
            while match chain.get_block_header_by_height(candidate.0) {
                Ok(header) => header.hash() != &candidate.1,
                Err(e) => match e {
                    near_chain::Error::DBNotFoundErr(_) => true,
                    _ => return Err(e),
                },
            } {
                let prev_header = chain.get_block_header(&candidate.2)?;
                candidate = (prev_header.height(), *prev_header.hash(), *prev_header.prev_hash());
            }

            // Then go forward for as long as we know the next block
            let mut ret_hash = candidate.1;
            loop {
                match chain.store().get_next_block_hash(&ret_hash) {
                    Ok(hash) => {
                        if chain.block_exists(&hash)? {
                            ret_hash = hash;
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

            ret_hash
        };

        // Look ahead for MAX_BLOCK_REQUESTS blocks and add the ones we don't have yet
        let mut requests = vec![];
        let mut next_hash = reference_hash;
        for _ in 0..MAX_BLOCK_REQUESTS {
            match chain.store().get_next_block_hash(&next_hash) {
                Ok(hash) => next_hash = hash,
                Err(e) => match e {
                    near_chain::Error::DBNotFoundErr(_) => break,
                    _ => return Err(e),
                },
            }
            if let Ok(()) = check_known(chain, &next_hash)? {
                let next_height = chain.get_block_header(&next_hash)?.height();
                requests.push((next_height, next_hash));
            }
        }

        let header_head = chain.header_head()?;

        let gc_stop_height = chain.runtime_adapter.get_gc_stop_height(&header_head.last_block_hash);

        for request in requests {
            let (height, hash) = request;
            let request_from_archival = self.archive && height < gc_stop_height;
            let peer = if request_from_archival {
                let archival_peer_iter = highest_height_peers.iter().filter(|p| p.archival);
                archival_peer_iter.choose(&mut rand::thread_rng())
            } else {
                let peer_iter = highest_height_peers.iter();
                peer_iter.choose(&mut rand::thread_rng())
            };

            if let Some(peer) = peer {
                debug!(target: "sync", "Block sync: {}/{} requesting block {} at height {} from {} (out of {} peers)",
                       chain_head.height, header_head.height, hash, height, peer.peer_info.id, highest_height_peers.len());
                self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::BlockRequest { hash, peer_id: peer.peer_info.id.clone() },
                ));
            } else {
                warn!(target: "sync", "Block sync: {}/{} No available {}peers to request block {} from",
                      chain_head.height, header_head.height, if request_from_archival { "archival " } else { "" }, hash);
            }
        }

        Ok(false)
    }

    /// Check if we should run block body sync and ask for more full blocks.
    /// Block sync is due either if the chain head has changed since the last request
    /// or if time since the last request is > BLOCK_REQUEST_TIMEOUT
    fn block_sync_due(&mut self, chain: &Chain) -> Result<bool, near_chain::Error> {
        match &self.last_request {
            None => Ok(true),
            Some(request) => Ok(chain.head()?.last_block_hash != request.head
                || StaticClock::utc() - request.when > Duration::seconds(BLOCK_REQUEST_TIMEOUT)),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use near_chain::test_utils::wait_for_all_blocks_in_processing;
    use near_chain::{ChainGenesis, Provenance};
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
        let mut block_sync =
            BlockSync::new(network_adapter.clone().into(), block_fetch_horizon, false, true);
        let mut chain_genesis = ChainGenesis::test();
        chain_genesis.epoch_length = 100;
        let mut env = TestEnv::builder(chain_genesis).clients_count(2).build();
        let mut blocks = vec![];
        for i in 1..5 * MAX_BLOCK_REQUESTS + 1 {
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
            let is_state_sync = block_sync.block_sync(&env.clients[1].chain, &peer_infos).unwrap();
            assert!(!is_state_sync);

            let expected_blocks: Vec<_> =
                blocks[i * MAX_BLOCK_REQUESTS..(i + 1) * MAX_BLOCK_REQUESTS].to_vec();
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
        let is_state_sync = block_sync.block_sync(&env.clients[1].chain, &peer_infos).unwrap();
        assert!(!is_state_sync);
        check_hashes_from_network_adapter(
            &network_adapter,
            (3 * MAX_BLOCK_REQUESTS..4 * MAX_BLOCK_REQUESTS).map(|h| *blocks[h].hash()).collect(),
        );
        // assumes that we only get block[4*MAX_BLOCK_REQUESTS-1]
        let _ = env.clients[1].process_block_test(
            MaybeValidated::from(blocks[4 * MAX_BLOCK_REQUESTS - 1].clone()),
            Provenance::NONE,
        );
        // the next block sync should not request block[4*MAX_BLOCK_REQUESTS-1] again
        let is_state_sync = block_sync.block_sync(&env.clients[1].chain, &peer_infos).unwrap();
        assert!(!is_state_sync);
        check_hashes_from_network_adapter(
            &network_adapter,
            (3 * MAX_BLOCK_REQUESTS..4 * MAX_BLOCK_REQUESTS - 1)
                .map(|h| *blocks[h].hash())
                .collect(),
        );

        // Receive all blocks. Should not request more. As an extra
        // complication, pause the processing of one block.
        env.pause_block_processing(&mut capture, blocks[4 * MAX_BLOCK_REQUESTS - 1].hash());
        for i in 3 * MAX_BLOCK_REQUESTS..5 * MAX_BLOCK_REQUESTS {
            let _ = env.clients[1]
                .process_block_test(MaybeValidated::from(blocks[i].clone()), Provenance::NONE);
        }
        block_sync.block_sync(&env.clients[1].chain, &peer_infos).unwrap();
        let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
        assert!(requested_block_hashes.is_empty(), "{:?}", requested_block_hashes);

        // Now finish paused processing processing and sanity check that we
        // still are fully synced.
        env.resume_block_processing(blocks[4 * MAX_BLOCK_REQUESTS - 1].hash());
        wait_for_all_blocks_in_processing(&mut env.clients[1].chain);
        let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
        assert!(requested_block_hashes.is_empty(), "{:?}", requested_block_hashes);
    }

    #[test]
    fn test_block_sync_archival() {
        let network_adapter = Arc::new(MockPeerManagerAdapter::default());
        let block_fetch_horizon = 10;
        let mut block_sync =
            BlockSync::new(network_adapter.clone().into(), block_fetch_horizon, true, true);
        let mut chain_genesis = ChainGenesis::test();
        chain_genesis.epoch_length = 5;
        let mut env = TestEnv::builder(chain_genesis).clients_count(2).build();
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
        let is_state_sync = block_sync.block_sync(&env.clients[1].chain, &peer_infos).unwrap();
        assert!(!is_state_sync);
        let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
        // We don't have archival peers, and thus cannot request any blocks
        assert_eq!(requested_block_hashes, HashSet::new());

        let mut peer_infos = create_highest_height_peer_infos(2);
        for peer in peer_infos.iter_mut() {
            peer.archival = true;
        }
        let is_state_sync = block_sync.block_sync(&env.clients[1].chain, &peer_infos).unwrap();
        assert!(!is_state_sync);
        let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
        assert_eq!(
            requested_block_hashes,
            blocks.iter().take(MAX_BLOCK_REQUESTS).map(|b| *b.hash()).collect::<HashSet<_>>()
        );
    }
}
