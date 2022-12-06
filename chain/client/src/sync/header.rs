use std::cmp::min;
use std::sync::Arc;
use std::time::Duration as TimeDuration;

use chrono::{DateTime, Duration};
use rand::seq::SliceRandom;
use rand::thread_rng;
use tracing::{debug, warn};

use near_chain::Chain;
use near_network::types::{HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter};
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::time::{Clock, Utc};
use near_primitives::types::BlockHeight;
use near_primitives::utils::to_timestamp;

use near_client_primitives::types::SyncStatus;
use near_network::types::PeerManagerMessageRequest;
use near_o11y::WithSpanContextExt;

/// Maximum number of block headers send over the network.
pub const MAX_BLOCK_HEADERS: u64 = 512;

/// Maximum number of block header hashes to send as part of a locator.
pub const MAX_BLOCK_HEADER_HASHES: usize = 20;

pub const NS_PER_SECOND: u128 = 1_000_000_000;

/// Helper to keep track of sync headers.
/// Handles major re-orgs by finding closest header that matches and re-downloading headers from that point.
pub struct HeaderSync {
    network_adapter: Arc<dyn PeerManagerAdapter>,
    history_locator: Vec<(BlockHeight, CryptoHash)>,
    prev_header_sync: (DateTime<Utc>, BlockHeight, BlockHeight, BlockHeight),
    syncing_peer: Option<HighestHeightPeerInfo>,
    stalling_ts: Option<DateTime<Utc>>,

    initial_timeout: Duration,
    progress_timeout: Duration,
    stall_ban_timeout: Duration,
    expected_height_per_second: u64,
}

impl HeaderSync {
    pub fn new(
        network_adapter: Arc<dyn PeerManagerAdapter>,
        initial_timeout: TimeDuration,
        progress_timeout: TimeDuration,
        stall_ban_timeout: TimeDuration,
        expected_height_per_second: u64,
    ) -> Self {
        HeaderSync {
            network_adapter,
            history_locator: vec![],
            prev_header_sync: (Clock::utc(), 0, 0, 0),
            syncing_peer: None,
            stalling_ts: None,
            initial_timeout: Duration::from_std(initial_timeout).unwrap(),
            progress_timeout: Duration::from_std(progress_timeout).unwrap(),
            stall_ban_timeout: Duration::from_std(stall_ban_timeout).unwrap(),
            expected_height_per_second,
        }
    }

    pub fn run(
        &mut self,
        sync_status: &mut SyncStatus,
        chain: &Chain,
        highest_height: BlockHeight,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<(), near_chain::Error> {
        let _span = tracing::debug_span!(target: "sync", "run", sync = "HeaderSync").entered();
        let header_head = chain.header_head()?;
        if !self.header_sync_due(sync_status, &header_head, highest_height) {
            return Ok(());
        }

        let enable_header_sync = match sync_status {
            SyncStatus::HeaderSync { .. }
            | SyncStatus::BodySync { .. }
            | SyncStatus::StateSyncDone => true,
            SyncStatus::NoSync | SyncStatus::AwaitingPeers | SyncStatus::EpochSync { .. } => {
                debug!(target: "sync", "Sync: initial transition to Header sync. Header head {} at {}",
                    header_head.last_block_hash, header_head.height,
                );
                self.history_locator.retain(|&x| x.0 == 0);
                true
            }
            SyncStatus::StateSync { .. } => false,
        };

        if enable_header_sync {
            let start_height = match sync_status.start_height() {
                Some(height) => height,
                None => chain.head()?.height,
            };
            *sync_status = SyncStatus::HeaderSync {
                start_height,
                current_height: header_head.height,
                highest_height,
            };
            self.syncing_peer = None;
            if let Some(peer) = highest_height_peers.choose(&mut thread_rng()).cloned() {
                if peer.highest_block_height > header_head.height {
                    self.syncing_peer = self.request_headers(chain, peer);
                }
            }
        }

        Ok(())
    }

    fn compute_expected_height(
        &self,
        old_height: BlockHeight,
        time_delta: Duration,
    ) -> BlockHeight {
        (old_height as u128
            + (time_delta.num_nanoseconds().unwrap() as u128
                * self.expected_height_per_second as u128
                / NS_PER_SECOND)) as u64
    }

    pub(crate) fn header_sync_due(
        &mut self,
        sync_status: &SyncStatus,
        header_head: &Tip,
        highest_height: BlockHeight,
    ) -> bool {
        let now = Clock::utc();
        let (timeout, old_expected_height, prev_height, prev_highest_height) =
            self.prev_header_sync;

        // Received all necessary header, can request more.
        let all_headers_received =
            header_head.height >= min(prev_height + MAX_BLOCK_HEADERS - 4, prev_highest_height);

        // Did we receive as many headers as we expected from the peer? Request more or ban peer.
        let stalling = header_head.height <= old_expected_height && now > timeout;

        // Always enable header sync on initial state transition from NoSync / NoSyncFewBlocksBehind / AwaitingPeers.
        let force_sync = match sync_status {
            SyncStatus::NoSync | SyncStatus::AwaitingPeers => true,
            _ => false,
        };

        if force_sync || all_headers_received || stalling {
            self.prev_header_sync = (
                now + self.initial_timeout,
                self.compute_expected_height(header_head.height, self.initial_timeout),
                header_head.height,
                highest_height,
            );

            if stalling {
                if self.stalling_ts.is_none() {
                    self.stalling_ts = Some(now);
                }
            } else {
                self.stalling_ts = None;
            }

            if all_headers_received {
                self.stalling_ts = None;
            } else {
                if let Some(ref stalling_ts) = self.stalling_ts {
                    if let Some(ref peer) = self.syncing_peer {
                        match sync_status {
                            SyncStatus::HeaderSync { highest_height, .. } => {
                                if now > *stalling_ts + self.stall_ban_timeout
                                    && *highest_height == peer.highest_block_height
                                {
                                    warn!(target: "sync", "Sync: ban a fraudulent peer: {}, claimed height: {}",
                                        peer.peer_info, peer.highest_block_height);
                                    self.network_adapter.do_send(
                                        PeerManagerMessageRequest::NetworkRequests(
                                            NetworkRequests::BanPeer {
                                                peer_id: peer.peer_info.id.clone(),
                                                ban_reason:
                                                    near_network::types::ReasonForBan::HeightFraud,
                                            },
                                        )
                                        .with_span_context(),
                                    );
                                    // This peer is fraudulent, let's skip this beat and wait for
                                    // the next one when this peer is not in the list anymore.
                                    self.syncing_peer = None;
                                    return false;
                                }
                            }
                            _ => (),
                        }
                    }
                }
            }
            self.syncing_peer = None;
            true
        } else {
            // Resetting the timeout as long as we make progress.
            let ns_time_till_timeout =
                (to_timestamp(timeout).saturating_sub(to_timestamp(now))) as u128;
            let remaining_expected_height = (self.expected_height_per_second as u128
                * ns_time_till_timeout
                / NS_PER_SECOND) as u64;
            if header_head.height >= old_expected_height.saturating_sub(remaining_expected_height) {
                let new_expected_height =
                    self.compute_expected_height(header_head.height, self.progress_timeout);
                self.prev_header_sync = (
                    now + self.progress_timeout,
                    new_expected_height,
                    prev_height,
                    prev_highest_height,
                );
            }
            false
        }
    }

    /// Request headers from a given peer to advance the chain.
    fn request_headers(
        &mut self,
        chain: &Chain,
        peer: HighestHeightPeerInfo,
    ) -> Option<HighestHeightPeerInfo> {
        if let Ok(locator) = self.get_locator(chain) {
            debug!(target: "sync", "Sync: request headers: asking {} for headers, {:?}", peer.peer_info.id, locator);
            self.network_adapter.do_send(
                PeerManagerMessageRequest::NetworkRequests(NetworkRequests::BlockHeadersRequest {
                    hashes: locator,
                    peer_id: peer.peer_info.id.clone(),
                })
                .with_span_context(),
            );
            return Some(peer);
        }
        None
    }

    fn get_locator(&mut self, chain: &Chain) -> Result<Vec<CryptoHash>, near_chain::Error> {
        let tip = chain.header_head()?;
        let genesis_height = chain.genesis().height();
        let heights = get_locator_heights(tip.height - genesis_height)
            .into_iter()
            .map(|h| h + genesis_height)
            .collect::<Vec<_>>();

        // For each height we need, we either check if something is close enough from last locator, or go to the db.
        let mut locator: Vec<(u64, CryptoHash)> = vec![(tip.height, tip.last_block_hash)];
        for h in heights {
            if let Some(x) = close_enough(&self.history_locator, h) {
                locator.push(x);
            } else {
                // Walk backwards to find last known hash.
                let last_loc = *locator.last().unwrap();
                if let Ok(header) = chain.get_block_header_by_height(h) {
                    if header.height() != last_loc.0 {
                        locator.push((header.height(), *header.hash()));
                    }
                }
            }
        }
        locator.dedup_by(|a, b| a.0 == b.0);
        debug!(target: "sync", "Sync: locator: {:?}", locator);
        self.history_locator = locator.clone();
        Ok(locator.iter().map(|x| x.1).collect())
    }
}

/// Check if there is a close enough value to provided height in the locator.
fn close_enough(locator: &[(u64, CryptoHash)], height: u64) -> Option<(u64, CryptoHash)> {
    if locator.len() == 0 {
        return None;
    }
    // Check boundaries, if lower than the last.
    if locator.last().unwrap().0 >= height {
        return locator.last().map(|x| *x);
    }
    // Higher than first and first is within acceptable gap.
    if locator[0].0 < height && height.saturating_sub(127) < locator[0].0 {
        return Some(locator[0]);
    }
    for h in locator.windows(2) {
        if height <= h[0].0 && height > h[1].0 {
            if h[0].0 - height < height - h[1].0 {
                return Some(h[0]);
            } else {
                return Some(h[1]);
            }
        }
    }
    None
}

/// Given height stepping back to 0 in powers of 2 steps.
fn get_locator_heights(height: u64) -> Vec<u64> {
    let mut current = height;
    let mut heights = vec![];
    while current > 0 {
        heights.push(current);
        if heights.len() >= MAX_BLOCK_HEADER_HASHES as usize - 1 {
            break;
        }
        let next = 2u64.pow(heights.len() as u32);
        current = if current > next { current - next } else { 0 };
    }
    heights.push(0);
    heights
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::thread;

    use near_chain::test_utils::{
        process_block_sync, setup, setup_with_validators, ValidatorSchedule,
    };
    use near_chain::{BlockProcessingArtifact, Provenance};
    use near_crypto::{KeyType, PublicKey};
    use near_network::test_utils::MockPeerManagerAdapter;
    use near_primitives::block::{Approval, Block, GenesisId};
    use near_primitives::network::PeerId;
    use near_primitives::test_utils::TestBlockBuilder;

    use super::*;
    use near_network::types::{BlockInfo, FullPeerInfo, PeerInfo};
    use near_primitives::merkle::PartialMerkleTree;
    use near_primitives::types::EpochId;
    use near_primitives::validator_signer::InMemoryValidatorSigner;
    use near_primitives::version::PROTOCOL_VERSION;
    use num_rational::Ratio;

    #[test]
    fn test_get_locator_heights() {
        assert_eq!(get_locator_heights(0), vec![0]);
        assert_eq!(get_locator_heights(1), vec![1, 0]);
        assert_eq!(get_locator_heights(2), vec![2, 0]);
        assert_eq!(get_locator_heights(3), vec![3, 1, 0]);
        assert_eq!(get_locator_heights(10), vec![10, 8, 4, 0]);
        assert_eq!(get_locator_heights(100), vec![100, 98, 94, 86, 70, 38, 0]);
        assert_eq!(
            get_locator_heights(1000),
            vec![1000, 998, 994, 986, 970, 938, 874, 746, 490, 0]
        );
        // Locator is still reasonable size even given large height.
        assert_eq!(
            get_locator_heights(10000),
            vec![10000, 9998, 9994, 9986, 9970, 9938, 9874, 9746, 9490, 8978, 7954, 5906, 1810, 0,]
        );
    }

    /// Starts two chains that fork of genesis and checks that they can sync heaaders to the longest.
    #[test]
    fn test_sync_headers_fork() {
        let mock_adapter = Arc::new(MockPeerManagerAdapter::default());
        let mut header_sync = HeaderSync::new(
            mock_adapter.clone(),
            TimeDuration::from_secs(10),
            TimeDuration::from_secs(2),
            TimeDuration::from_secs(120),
            1_000_000_000,
        );
        let (mut chain, _, signer) = setup();
        for _ in 0..3 {
            let prev = chain.get_block(&chain.head().unwrap().last_block_hash).unwrap();
            let block = TestBlockBuilder::new(&prev, signer.clone()).build();
            process_block_sync(
                &mut chain,
                &None,
                block.into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();
        }
        let (mut chain2, _, signer2) = setup();
        for _ in 0..5 {
            let prev = chain2.get_block(&chain2.head().unwrap().last_block_hash).unwrap();
            let block = TestBlockBuilder::new(&prev, signer2.clone()).build();
            process_block_sync(
                &mut chain2,
                &None,
                block.into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();
        }
        let mut sync_status = SyncStatus::NoSync;
        let peer1 = FullPeerInfo {
            peer_info: PeerInfo::random(),
            chain_info: near_network::types::PeerChainInfo {
                genesis_id: GenesisId {
                    chain_id: "unittest".to_string(),
                    hash: *chain.genesis().hash(),
                },
                tracked_shards: vec![],
                archival: false,
                last_block: Some(BlockInfo {
                    height: chain2.head().unwrap().height,
                    hash: chain2.head().unwrap().last_block_hash,
                }),
            },
        };
        let head = chain.head().unwrap();
        assert!(header_sync
            .run(
                &mut sync_status,
                &mut chain,
                head.height,
                &[<FullPeerInfo as Into<Option<_>>>::into(peer1.clone()).unwrap()]
            )
            .is_ok());
        assert!(sync_status.is_syncing());
        // Check that it queried last block, and then stepped down to genesis block to find common block with the peer.

        let item = mock_adapter.pop().unwrap().as_network_requests();
        assert_eq!(
            item,
            NetworkRequests::BlockHeadersRequest {
                hashes: [3, 1, 0]
                    .iter()
                    .map(|i| *chain.get_block_by_height(*i).unwrap().hash())
                    .collect(),
                peer_id: peer1.peer_info.id
            }
        );
    }

    /// Sets up `HeaderSync` with particular tolerance for slowness, and makes sure that a peer that
    /// sends headers below the threshold gets banned, and the peer that sends them faster doesn't get
    /// banned.
    /// Also makes sure that if `header_sync_due` is checked more frequently than the `progress_timeout`
    /// the peer doesn't get banned. (specifically, that the expected height downloaded gets properly
    /// adjusted for time passed)
    #[test]
    fn test_slow_header_sync() {
        let network_adapter = Arc::new(MockPeerManagerAdapter::default());
        let highest_height = 1000;

        // Setup header_sync with expectation of 25 headers/second
        let mut header_sync = HeaderSync::new(
            network_adapter.clone(),
            TimeDuration::from_secs(1),
            TimeDuration::from_secs(1),
            TimeDuration::from_secs(3),
            25,
        );

        let set_syncing_peer = |header_sync: &mut HeaderSync| {
            header_sync.syncing_peer = Some(HighestHeightPeerInfo {
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
            });
            header_sync.syncing_peer.as_mut().unwrap().highest_block_height = highest_height;
        };
        set_syncing_peer(&mut header_sync);

        let vs = ValidatorSchedule::new().block_producers_per_epoch(vec![vec![
            "test0", "test1", "test2", "test3", "test4",
        ]
        .iter()
        .map(|x| x.parse().unwrap())
        .collect()]);
        let (chain, _, signers) = setup_with_validators(vs, 1000, 100);
        let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();

        let mut last_block = &genesis;
        let mut all_blocks = vec![];
        let mut block_merkle_tree = PartialMerkleTree::default();
        for i in 0..61 {
            let current_height = 3 + i * 5;

            let approvals = [None, None, Some("test3"), Some("test4")]
                .iter()
                .map(|account_id| {
                    account_id.map(|account_id| {
                        let signer = InMemoryValidatorSigner::from_seed(
                            account_id.parse().unwrap(),
                            KeyType::ED25519,
                            account_id,
                        );
                        Approval::new(
                            *last_block.hash(),
                            last_block.header().height(),
                            current_height,
                            &signer,
                        )
                        .signature
                    })
                })
                .collect();
            let (epoch_id, next_epoch_id) =
                if last_block.header().prev_hash() == &CryptoHash::default() {
                    (last_block.header().next_epoch_id().clone(), EpochId(*last_block.hash()))
                } else {
                    (
                        last_block.header().epoch_id().clone(),
                        last_block.header().next_epoch_id().clone(),
                    )
                };
            let block = Block::produce(
                PROTOCOL_VERSION,
                PROTOCOL_VERSION,
                last_block.header(),
                current_height,
                last_block.header().block_ordinal() + 1,
                last_block.chunks().iter().cloned().collect(),
                epoch_id,
                next_epoch_id,
                None,
                approvals,
                Ratio::new(0, 1),
                0,
                100,
                Some(0),
                vec![],
                vec![],
                &*signers[3],
                *last_block.header().next_bp_hash(),
                block_merkle_tree.root(),
                None,
            );
            block_merkle_tree.insert(*block.hash());

            all_blocks.push(block);

            last_block = &all_blocks[all_blocks.len() - 1];
        }

        let mut last_added_block_ord = 0;
        // First send 30 heights every second for a while and make sure it doesn't get
        // banned
        for _iter in 0..12 {
            let block = &all_blocks[last_added_block_ord];
            let current_height = block.header().height();
            set_syncing_peer(&mut header_sync);
            header_sync.header_sync_due(
                &SyncStatus::HeaderSync {
                    start_height: current_height,
                    current_height,
                    highest_height,
                },
                &Tip::from_header(block.header()),
                highest_height,
            );

            last_added_block_ord += 3;

            thread::sleep(TimeDuration::from_millis(500));
        }
        // 6 blocks / second is fast enough, we should not have banned the peer
        assert!(network_adapter.requests.read().unwrap().is_empty());

        // Now the same, but only 20 heights / sec
        for _iter in 0..12 {
            let block = &all_blocks[last_added_block_ord];
            let current_height = block.header().height();
            set_syncing_peer(&mut header_sync);
            header_sync.header_sync_due(
                &SyncStatus::HeaderSync {
                    start_height: current_height,
                    current_height,
                    highest_height,
                },
                &Tip::from_header(block.header()),
                highest_height,
            );

            last_added_block_ord += 2;

            thread::sleep(TimeDuration::from_millis(500));
        }
        // This time the peer should be banned, because 4 blocks/s is not fast enough
        let ban_peer = network_adapter.requests.write().unwrap().pop_back().unwrap();

        if let NetworkRequests::BanPeer { .. } = ban_peer.as_network_requests() {
            /* expected */
        } else {
            assert!(false);
        }
    }
}
