mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration as TimeDuration;

    use near_chain::{Chain, RuntimeAdapter};
    use near_chain::{ChainGenesis, Provenance};
    use near_client::sync::{get_locator_heights, BlockSync, HeaderSync};
    use near_client_primitives::types::{
        DownloadStatus, ShardSyncDownload, ShardSyncStatus, SyncStatus,
    };
    use near_crypto::{KeyType, PublicKey};
    use near_network::routing::EdgeInfo;
    use near_network::test_utils::MockNetworkAdapter;
    use near_network::types::PeerChainInfoV2;
    use near_network::types::{AccountOrPeerIdOrHash, NetworkResponses, ReasonForBan};
    use near_network::PeerInfo;
    use near_network::{FullPeerInfo, NetworkAdapter, NetworkRequests};
    use near_primitives::block::Tip;
    use near_primitives::block::{Approval, Block, GenesisId};
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::merkle::PartialMerkleTree;
    use near_primitives::network::PeerId;
    use near_primitives::num_rational::Ratio;
    use near_primitives::types::EpochId;
    use near_primitives::validator_signer::InMemoryValidatorSigner;
    use near_primitives::version::PROTOCOL_VERSION;
    use std::collections::HashSet;
    use testlib::chain_test_utils::{setup, setup_with_validators};
    use testlib::client_test_utils::TestEnv;

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
        let mock_adapter = Arc::new(MockNetworkAdapter::default());
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
            let block = Block::empty(prev, &*signer);
            chain
                .process_block(&None, block, Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
                .unwrap();
        }
        let (mut chain2, _, signer2) = setup();
        for _ in 0..5 {
            let prev = chain2.get_block(&chain2.head().unwrap().last_block_hash).unwrap();
            let block = Block::empty(&prev, &*signer2);
            chain2
                .process_block(&None, block, Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
                .unwrap();
        }
        let mut sync_status = SyncStatus::NoSync;
        let peer1 = FullPeerInfo {
            peer_info: PeerInfo::random(),
            chain_info: PeerChainInfoV2 {
                genesis_id: GenesisId {
                    chain_id: "unittest".to_string(),
                    hash: *chain.genesis().hash(),
                },
                height: chain2.head().unwrap().height,
                tracked_shards: vec![],
                archival: false,
            },
            edge_info: EdgeInfo::default(),
        };
        let head = chain.head().unwrap();
        assert!(header_sync
            .run(&mut sync_status, &mut chain, head.height, &vec![peer1.clone()])
            .is_ok());
        assert!(sync_status.is_syncing());
        // Check that it queried last block, and then stepped down to genesis block to find common block with the peer.
        assert_eq!(
            mock_adapter.pop().unwrap(),
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
        let network_adapter = Arc::new(MockNetworkAdapter::default());
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
            header_sync.syncing_peer = Some(FullPeerInfo {
                peer_info: PeerInfo {
                    id: PeerId::new(PublicKey::empty(KeyType::ED25519)),
                    addr: None,
                    account_id: None,
                },
                chain_info: Default::default(),
                edge_info: Default::default(),
            });
            header_sync.syncing_peer.as_mut().unwrap().chain_info.height = highest_height;
        };
        set_syncing_peer(&mut header_sync);

        let (mut chain, _, signers) = setup_with_validators(
            vec!["test0", "test1", "test2", "test3", "test4"]
                .iter()
                .map(|x| x.to_string())
                .collect(),
            1,
            1,
            1000,
            100,
        );
        let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap().clone();

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
                            account_id,
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
                &last_block.header(),
                current_height,
                #[cfg(feature = "protocol_feature_block_header_v3")]
                (last_block.header().block_ordinal() + 1),
                last_block.chunks().iter().cloned().collect(),
                epoch_id,
                next_epoch_id,
                #[cfg(feature = "protocol_feature_block_header_v3")]
                None,
                approvals,
                Ratio::new(0, 1),
                0,
                100,
                Some(0),
                vec![],
                vec![],
                &*signers[3],
                last_block.header().next_bp_hash().clone(),
                block_merkle_tree.root(),
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
                &SyncStatus::HeaderSync { current_height, highest_height },
                &Tip::from_header(&block.header()),
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
                &SyncStatus::HeaderSync { current_height, highest_height },
                &Tip::from_header(&block.header()),
                highest_height,
            );

            last_added_block_ord += 2;

            thread::sleep(TimeDuration::from_millis(500));
        }
        // This time the peer should be banned, because 4 blocks/s is not fast enough
        let ban_peer = network_adapter.requests.write().unwrap().pop_back().unwrap();
        if let NetworkRequests::BanPeer { .. } = ban_peer {
            /* expected */
        } else {
            assert!(false);
        }
    }

    /// Helper function for block sync tests
    fn collect_hashes_from_network_adapter(
        network_adapter: Arc<MockNetworkAdapter>,
    ) -> HashSet<CryptoHash> {
        let mut requested_block_hashes = HashSet::new();
        let mut network_request = network_adapter.requests.write().unwrap();
        while let Some(request) = network_request.pop_back() {
            match request {
                NetworkRequests::BlockRequest { hash, .. } => {
                    requested_block_hashes.insert(hash);
                }
                _ => panic!("unexpected network request {:?}", request),
            }
        }
        requested_block_hashes
    }

    fn create_peer_infos(num_peers: usize) -> Vec<FullPeerInfo> {
        (0..num_peers)
            .map(|_| FullPeerInfo {
                peer_info: PeerInfo {
                    id: PeerId::new(PublicKey::empty(KeyType::ED25519)),
                    addr: None,
                    account_id: None,
                },
                chain_info: Default::default(),
                edge_info: Default::default(),
            })
            .collect()
    }

    #[test]
    fn test_block_sync() {
        let network_adapter = Arc::new(MockNetworkAdapter::default());
        let block_fetch_horizon = 10;
        let mut block_sync = BlockSync::new(network_adapter.clone(), block_fetch_horizon, false);
        let mut chain_genesis = ChainGenesis::test();
        chain_genesis.epoch_length = 100;
        let mut env = TestEnv::new(chain_genesis, 2, 1);
        let mut blocks = vec![];
        for i in 1..21 {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block, Provenance::PRODUCED);
        }
        let block_headers = blocks.iter().map(|b| b.header().clone()).collect::<Vec<_>>();
        let peer_infos = create_peer_infos(2);
        env.clients[1].chain.sync_block_headers(block_headers, |_| unreachable!()).unwrap();

        for block in blocks.iter().take(5) {
            let is_state_sync =
                block_sync.block_sync(&mut env.clients[1].chain, &peer_infos).unwrap();
            assert!(!is_state_sync);

            let requested_block_hashes =
                collect_hashes_from_network_adapter(network_adapter.clone());
            assert_eq!(
                requested_block_hashes,
                [block].iter().map(|x| *x.hash()).collect::<HashSet<_>>()
            );

            env.process_block(1, block.clone(), Provenance::NONE);
        }

        // Receive all blocks. Should not request more.
        for i in 5..21 {
            env.process_block(1, blocks[i - 1].clone(), Provenance::NONE);
        }
        block_sync.block_sync(&mut env.clients[1].chain, &peer_infos).unwrap();
        let requested_block_hashes = collect_hashes_from_network_adapter(network_adapter.clone());
        assert!(requested_block_hashes.is_empty());
    }

    #[test]
    fn test_block_sync_archival() {
        let network_adapter = Arc::new(MockNetworkAdapter::default());
        let block_fetch_horizon = 10;
        let mut block_sync = BlockSync::new(network_adapter.clone(), block_fetch_horizon, true);
        let mut chain_genesis = ChainGenesis::test();
        chain_genesis.epoch_length = 5;
        let mut env = TestEnv::new(chain_genesis, 2, 1);
        let mut blocks = vec![];
        for i in 1..31 {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block, Provenance::PRODUCED);
        }
        let block_headers = blocks.iter().map(|b| b.header().clone()).collect::<Vec<_>>();
        let peer_infos = create_peer_infos(2);
        env.clients[1].chain.sync_block_headers(block_headers, |_| unreachable!()).unwrap();
        let is_state_sync = block_sync.block_sync(&mut env.clients[1].chain, &peer_infos).unwrap();
        assert!(!is_state_sync);
        let requested_block_hashes = collect_hashes_from_network_adapter(network_adapter.clone());
        // We don't have archival peers, and thus cannot request any blocks
        assert_eq!(requested_block_hashes, HashSet::new());

        let mut peer_infos = create_peer_infos(2);
        for peer in peer_infos.iter_mut() {
            peer.chain_info.archival = true;
        }
        let is_state_sync = block_sync.block_sync(&mut env.clients[1].chain, &peer_infos).unwrap();
        assert!(!is_state_sync);
        let requested_block_hashes = collect_hashes_from_network_adapter(network_adapter.clone());
        assert_eq!(
            requested_block_hashes,
            blocks.iter().take(1).map(|b| *b.hash()).collect::<HashSet<_>>()
        );
    }
}
