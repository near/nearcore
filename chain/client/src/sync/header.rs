use std::cmp::min;
use std::time::Duration as TimeDuration;

use chrono::{DateTime, Duration, Utc};
use near_async::messaging::CanSend;
use near_chain::{Chain, ChainStoreAccess};
use near_client_primitives::types::SyncStatus;
use near_network::types::PeerManagerMessageRequest;
use near_network::types::{HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter};
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::static_clock::StaticClock;
use near_primitives::types::BlockHeight;
use near_primitives::utils::to_timestamp;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tracing::{debug, warn};

/// Maximum number of block headers send over the network.
pub const MAX_BLOCK_HEADERS: u64 = 512;

/// Maximum number of block header hashes to send as part of a locator.
pub const MAX_BLOCK_HEADER_HASHES: usize = 20;

pub const NS_PER_SECOND: u128 = 1_000_000_000;

/// Helper to keep track of sync headers.
/// Handles major re-orgs by finding closest header that matches and re-downloading headers from that point.
pub struct HeaderSync {
    network_adapter: PeerManagerAdapter,
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
        network_adapter: PeerManagerAdapter,
        initial_timeout: TimeDuration,
        progress_timeout: TimeDuration,
        stall_ban_timeout: TimeDuration,
        expected_height_per_second: u64,
    ) -> Self {
        HeaderSync {
            network_adapter,
            prev_header_sync: (StaticClock::utc(), 0, 0, 0),
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
        let now = StaticClock::utc();
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
                                    self.network_adapter.send(
                                        PeerManagerMessageRequest::NetworkRequests(
                                            NetworkRequests::BanPeer {
                                                peer_id: peer.peer_info.id.clone(),
                                                ban_reason:
                                                    near_network::types::ReasonForBan::HeightFraud,
                                            },
                                        ),
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
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::BlockHeadersRequest {
                    hashes: locator,
                    peer_id: peer.peer_info.id.clone(),
                },
            ));
            return Some(peer);
        }
        None
    }

    // The remote side will return MAX_BLOCK_HEADERS headers, starting from the first hash in
    // the returned "locator" list that is on their canonical chain.
    //
    // The locator allows us to start syncing from a reasonably recent common ancestor. Since
    // we don't know which fork the remote side is on, we include a few hashes. The first one
    // we include is the tip of our chain, and the next one is 2 blocks back (on the same chain,
    // by number of blocks (or in other words, by ordinals), not by height), then 4 blocks
    // back, then 8 blocks back, etc, until we reach the most recent final block. The reason
    // why we stop at the final block is because the consensus guarantees us that the final
    // blocks observed by all nodes are on the same fork.
    fn get_locator(&mut self, chain: &Chain) -> Result<Vec<CryptoHash>, near_chain::Error> {
        let store = chain.store();
        let tip = store.header_head()?;
        // We could just get the ordinal from the header, but it's off by one: #8177.
        let tip_ordinal = store.get_block_merkle_tree(&tip.last_block_hash)?.size();
        let final_head = store.final_head()?;
        let final_head_ordinal = store.get_block_merkle_tree(&final_head.last_block_hash)?.size();
        let ordinals = get_locator_ordinals(final_head_ordinal, tip_ordinal);
        let mut locator: Vec<CryptoHash> = vec![];
        for ordinal in &ordinals {
            let block_hash = store.get_block_hash_from_ordinal(*ordinal)?;
            locator.push(block_hash);
        }
        debug!(target: "sync", "Sync: locator: {:?} ordinals: {:?}", locator, ordinals);
        Ok(locator)
    }
}

/// Step back from highest to lowest ordinal, in powers of 2 steps, limited by MAX_BLOCK_HEADERS
/// heights per step, and limited by MAX_BLOCK_HEADER_HASHES steps in total.
fn get_locator_ordinals(lowest_ordinal: u64, highest_ordinal: u64) -> Vec<u64> {
    let mut current = highest_ordinal;
    let mut ordinals = vec![];
    let mut step = 2;
    while current > lowest_ordinal && ordinals.len() < MAX_BLOCK_HEADER_HASHES as usize - 1 {
        ordinals.push(current);
        if current <= lowest_ordinal + step {
            break;
        }
        current -= step;
        // Do not step back more than MAX_BLOCK_HEADERS, as the gap in between would not
        // allow us to sync to a more recent block.
        step = min(step * 2, MAX_BLOCK_HEADERS);
    }
    ordinals.push(lowest_ordinal);
    ordinals
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::thread;

    use near_chain::test_utils::{
        process_block_sync, setup, setup_with_validators_and_start_time, ValidatorSchedule,
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
    use near_primitives::version::PROTOCOL_VERSION;
    use num_rational::Ratio;

    #[test]
    fn test_get_locator_ordinals() {
        assert_eq!(get_locator_ordinals(0, 0), vec![0]);
        assert_eq!(get_locator_ordinals(0, 1), vec![1, 0]);
        assert_eq!(get_locator_ordinals(0, 2), vec![2, 0]);
        assert_eq!(get_locator_ordinals(0, 3), vec![3, 1, 0]);
        assert_eq!(get_locator_ordinals(0, 10), vec![10, 8, 4, 0]);
        assert_eq!(get_locator_ordinals(0, 100), vec![100, 98, 94, 86, 70, 38, 0]);
        assert_eq!(
            get_locator_ordinals(0, 1000),
            vec![1000, 998, 994, 986, 970, 938, 874, 746, 490, 0]
        );
        // Locator is still reasonable size even given large height.
        assert_eq!(
            get_locator_ordinals(0, 10000),
            vec![
                10000, 9998, 9994, 9986, 9970, 9938, 9874, 9746, 9490, 8978, 8466, 7954, 7442,
                6930, 6418, 5906, 5394, 4882, 4370, 0
            ]
        );
        assert_eq!(get_locator_ordinals(100, 100), vec![100]);
        assert_eq!(get_locator_ordinals(100, 101), vec![101, 100]);
        assert_eq!(get_locator_ordinals(100, 102), vec![102, 100]);
        assert_eq!(get_locator_ordinals(100, 103), vec![103, 101, 100]);
        assert_eq!(get_locator_ordinals(100, 110), vec![110, 108, 104, 100]);
        assert_eq!(get_locator_ordinals(100, 200), vec![200, 198, 194, 186, 170, 138, 100]);
        assert_eq!(
            get_locator_ordinals(20000, 21000),
            vec![21000, 20998, 20994, 20986, 20970, 20938, 20874, 20746, 20490, 20000]
        );
        assert_eq!(
            get_locator_ordinals(20000, 30000),
            vec![
                30000, 29998, 29994, 29986, 29970, 29938, 29874, 29746, 29490, 28978, 28466, 27954,
                27442, 26930, 26418, 25906, 25394, 24882, 24370, 20000
            ]
        );
    }

    /// Starts two chains that fork of genesis and checks that they can sync headers to the longest.
    #[test]
    fn test_sync_headers_fork() {
        let mock_adapter = Arc::new(MockPeerManagerAdapter::default());
        let mut header_sync = HeaderSync::new(
            mock_adapter.clone().into(),
            TimeDuration::from_secs(10),
            TimeDuration::from_secs(2),
            TimeDuration::from_secs(120),
            1_000_000_000,
        );
        let (mut chain, _, _, signer) = setup();
        for _ in 0..3 {
            let prev = chain.get_block(&chain.head().unwrap().last_block_hash).unwrap();
            // Have gaps in the chain, so we don't have final blocks (i.e. last final block is
            // genesis). Otherwise we violate consensus invariants.
            let block = TestBlockBuilder::new(&prev, signer.clone())
                .height(prev.header().height() + 2)
                .build();
            process_block_sync(
                &mut chain,
                &None,
                block.into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();
        }
        let (mut chain2, _, _, signer2) = setup();
        for _ in 0..5 {
            let prev = chain2.get_block(&chain2.head().unwrap().last_block_hash).unwrap();
            // Have gaps in the chain, so we don't have final blocks (i.e. last final block is
            // genesis). Otherwise we violate consensus invariants.
            let block = TestBlockBuilder::new(&prev, signer2.clone())
                .height(prev.header().height() + 2)
                .build();
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
                // chain is 6 -> 4 -> 2 -> 0.
                hashes: [6, 2, 0]
                    .iter()
                    .map(|i| *chain.get_block_by_height(*i).unwrap().hash())
                    .collect(),
                peer_id: peer1.peer_info.id
            }
        );
    }

    #[test]
    fn test_sync_headers_fork_from_final_block() {
        let mock_adapter = Arc::new(MockPeerManagerAdapter::default());
        let mut header_sync = HeaderSync::new(
            mock_adapter.clone().into(),
            TimeDuration::from_secs(10),
            TimeDuration::from_secs(2),
            TimeDuration::from_secs(120),
            1_000_000_000,
        );
        let (mut chain, _, _, signer) = setup();
        let (mut chain2, _, _, signer2) = setup();
        for chain in [&mut chain, &mut chain2] {
            // Both chains share a common final block at height 3.
            for _ in 0..5 {
                let prev = chain.get_block(&chain.head().unwrap().last_block_hash).unwrap();
                let block = TestBlockBuilder::new(&prev, signer.clone()).build();
                process_block_sync(
                    chain,
                    &None,
                    block.into(),
                    Provenance::PRODUCED,
                    &mut BlockProcessingArtifact::default(),
                )
                .unwrap();
            }
        }
        for _ in 0..7 {
            let prev = chain.get_block(&chain.head().unwrap().last_block_hash).unwrap();
            // Test with huge gaps to make sure we are still able to find locators.
            let block = TestBlockBuilder::new(&prev, signer.clone())
                .height(prev.header().height() + 1000)
                .build();
            process_block_sync(
                &mut chain,
                &None,
                block.into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();
        }
        for _ in 0..3 {
            let prev = chain2.get_block(&chain2.head().unwrap().last_block_hash).unwrap();
            // Test with huge gaps, but 3 blocks here produce a higher height than the 7 blocks
            // above.
            let block = TestBlockBuilder::new(&prev, signer2.clone())
                .height(prev.header().height() + 3100)
                .build();
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
                // chain is 7005 -> 6005 -> 5005 -> 4005 -> 3005 -> 2005 -> 1005 -> 5 -> 4 -> 3 -> 2 -> 1 -> 0
                // where 3 is final.
                hashes: [7005, 5005, 1005, 3]
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
            network_adapter.clone().into(),
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

        let (chain, _, _, signer) = setup();
        let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();

        let mut last_block = &genesis;
        let mut all_blocks = vec![];
        for i in 0..61 {
            let current_height = 3 + i * 5;
            let block =
                TestBlockBuilder::new(last_block, signer.clone()).height(current_height).build();
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

    #[test]
    fn test_sync_from_very_behind() {
        let mock_adapter = Arc::new(MockPeerManagerAdapter::default());
        let mut header_sync = HeaderSync::new(
            mock_adapter.clone().into(),
            TimeDuration::from_secs(10),
            TimeDuration::from_secs(2),
            TimeDuration::from_secs(120),
            1_000_000_000,
        );

        let vs = ValidatorSchedule::new()
            .block_producers_per_epoch(vec![vec!["test0".parse().unwrap()]]);
        let genesis_time = StaticClock::utc();
        // Don't bother with epoch switches. It's not relevant.
        let (mut chain, _, _, _) =
            setup_with_validators_and_start_time(vs.clone(), 10000, 100, genesis_time);
        let (mut chain2, _, _, signers2) =
            setup_with_validators_and_start_time(vs, 10000, 100, genesis_time);
        // Set up the second chain with 2000+ blocks.
        let mut block_merkle_tree = PartialMerkleTree::default();
        block_merkle_tree.insert(*chain.genesis().hash()); // for genesis block
        for _ in 0..(4 * MAX_BLOCK_HEADERS + 10) {
            let last_block = chain2.get_block(&chain2.head().unwrap().last_block_hash).unwrap();
            let this_height = last_block.header().height() + 1;
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
                this_height,
                last_block.header().block_ordinal() + 1,
                last_block.chunks().iter().cloned().collect(),
                epoch_id,
                next_epoch_id,
                None,
                signers2
                    .iter()
                    .map(|signer| {
                        Some(
                            Approval::new(
                                *last_block.hash(),
                                last_block.header().height(),
                                this_height,
                                signer.as_ref(),
                            )
                            .signature,
                        )
                    })
                    .collect(),
                Ratio::new(0, 1),
                0,
                100,
                Some(0),
                vec![],
                vec![],
                &*signers2[0],
                *last_block.header().next_bp_hash(),
                block_merkle_tree.root(),
                None,
            );
            block_merkle_tree.insert(*block.hash());
            chain2.process_block_header(block.header(), &mut Vec::new()).unwrap(); // just to validate
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
        // It should be done in 5 iterations, but give it 10 iterations just in case it would
        // get into an infinite loop because of some bug and cause the test to hang.
        for _ in 0..10 {
            let header_head = chain.header_head().unwrap();
            if header_head.last_block_hash == chain2.header_head().unwrap().last_block_hash {
                // sync is done.
                break;
            }
            assert!(header_sync
                .run(
                    &mut sync_status,
                    &mut chain,
                    header_head.height,
                    &[<FullPeerInfo as Into<Option<_>>>::into(peer1.clone()).unwrap()]
                )
                .is_ok());
            match sync_status {
                SyncStatus::HeaderSync { .. } => {}
                _ => panic!("Unexpected sync status: {:?}", sync_status),
            }
            let message = match mock_adapter.pop() {
                Some(message) => message.as_network_requests(),
                None => {
                    panic!("No message was sent; current height: {}", header_head.height);
                }
            };
            match message {
                NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                    assert_eq!(peer_id, peer1.peer_info.id);
                    let headers = chain2.retrieve_headers(hashes, MAX_BLOCK_HEADERS, None).unwrap();
                    assert!(!headers.is_empty(), "No headers were returned");
                    match chain.sync_block_headers(headers, &mut Vec::new()) {
                        Ok(_) => {}
                        Err(e) => {
                            panic!("Error inserting headers: {:?}", e);
                        }
                    }
                }
                _ => panic!("Unexpected network message: {:?}", message),
            }
            if chain.header_head().unwrap().height <= header_head.height {
                panic!(
                    "Syncing is not making progress. Head was not updated from {}",
                    header_head.height
                );
            }
        }
        let new_tip = chain.header_head().unwrap();
        assert_eq!(new_tip.last_block_hash, chain2.head().unwrap().last_block_hash);
    }
}
