mod tests {
    use near_network::test_utils::MockNetworkAdapter;
    use near_network::types::PartialEncodedChunkForwardMsg;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::utils::MaybeValidated;
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::test_utils::create_test_store;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use testlib::chain_test_utils::KeyValueRuntime;
    use testlib::chunks_test_utils::{ChunkForwardingTestFixture, SealsManagerTestFixture};

    use near_chunks::{
        ChunkRequestInfo, ProcessPartialEncodedChunkResult, Seal, SealsManager, ShardsManager,
        CHUNK_REQUEST_RETRY_MS, NUM_PARTS_REQUESTED_IN_SEAL,
    };
    use near_network::NetworkRequests;
    use near_primitives::block::Tip;
    use near_primitives::sharding::{ChunkHash, PartialEncodedChunkV2};
    use near_primitives::types::EpochId;
    #[cfg(feature = "expensive_tests")]
    use {
        crate::ACCEPTING_SEAL_PERIOD_MS, near_chain::ChainStore, near_chain::RuntimeAdapter,
        near_crypto::KeyType, near_logger_utils::init_test_logger,
        near_primitives::merkle::merklize, near_primitives::sharding::ReedSolomonWrapper,
        near_primitives::validator_signer::InMemoryValidatorSigner,
    };

    /// should not request partial encoded chunk from self
    #[test]
    fn test_request_partial_encoded_chunk_from_self() {
        let runtime_adapter = Arc::new(KeyValueRuntime::new(create_test_store()));
        let network_adapter = Arc::new(MockNetworkAdapter::default());
        let mut shards_manager =
            ShardsManager::new(Some("test".to_string()), runtime_adapter, network_adapter.clone());
        shards_manager.requested_partial_encoded_chunks.insert(
            ChunkHash(hash(&[1])),
            ChunkRequestInfo {
                height: 0,
                parent_hash: Default::default(),
                shard_id: 0,
                added: Instant::now(),
                last_requested: Instant::now(),
            },
        );
        std::thread::sleep(Duration::from_millis(2 * CHUNK_REQUEST_RETRY_MS));
        shards_manager.resend_chunk_requests(&Tip {
            height: 0,
            last_block_hash: CryptoHash::default(),
            prev_block_hash: CryptoHash::default(),
            epoch_id: EpochId::default(),
            next_epoch_id: EpochId::default(),
        });

        // For the chunks that would otherwise be requested from self we expect a request to be
        // sent to any peer tracking shard
        if let NetworkRequests::PartialEncodedChunkRequest { target, .. } =
            network_adapter.requests.read().unwrap()[0].clone()
        {
            assert!(target.account_id == None);
        } else {
            println!("{:?}", network_adapter.requests.read().unwrap());
            assert!(false);
        };
    }

    #[cfg(feature = "expensive_tests")]
    #[test]
    fn test_seal_removal() {
        init_test_logger();
        let runtime_adapter = Arc::new(KeyValueRuntime::new_with_validators(
            create_test_store(),
            vec![vec![
                "test".to_string(),
                "test1".to_string(),
                "test2".to_string(),
                "test3".to_string(),
            ]],
            1,
            1,
            5,
        ));
        let network_adapter = Arc::new(MockNetworkAdapter::default());
        let mut chain_store = ChainStore::new(create_test_store(), 0);
        let mut shards_manager = ShardsManager::new(
            Some("test".to_string()),
            runtime_adapter.clone(),
            network_adapter.clone(),
        );
        let signer = InMemoryValidatorSigner::from_seed("test", KeyType::ED25519, "test");
        let mut rs = ReedSolomonWrapper::new(4, 10);
        let (encoded_chunk, proof) = shards_manager
            .create_encoded_shard_chunk(
                CryptoHash::default(),
                CryptoHash::default(),
                CryptoHash::default(),
                1,
                0,
                0,
                0,
                0,
                vec![],
                vec![],
                &vec![],
                merklize(&runtime_adapter.build_receipts_hashes(&vec![])).0,
                CryptoHash::default(),
                &signer,
                &mut rs,
                PROTOCOL_VERSION,
            )
            .unwrap();
        let header = encoded_chunk.cloned_header();
        shards_manager.requested_partial_encoded_chunks.insert(
            header.chunk_hash(),
            ChunkRequestInfo {
                height: header.height_created(),
                parent_hash: header.prev_block_hash(),
                shard_id: header.shard_id(),
                last_requested: Instant::now(),
                added: Instant::now(),
            },
        );
        shards_manager
            .request_partial_encoded_chunk(
                header.height_created(),
                &header.prev_block_hash(),
                header.shard_id(),
                &header.chunk_hash(),
                false,
                false,
                false,
            )
            .unwrap();
        let partial_encoded_chunk1 =
            encoded_chunk.create_partial_encoded_chunk(vec![0, 1], vec![], &proof);
        let partial_encoded_chunk2 =
            encoded_chunk.create_partial_encoded_chunk(vec![2, 3, 4], vec![], &proof);
        std::thread::sleep(Duration::from_millis(ACCEPTING_SEAL_PERIOD_MS as u64 + 100));
        for partial_encoded_chunk in vec![partial_encoded_chunk1, partial_encoded_chunk2] {
            let pec_v2 = partial_encoded_chunk.into();
            shards_manager
                .process_partial_encoded_chunk(
                    MaybeValidated::NotValidated(&pec_v2),
                    &mut chain_store,
                    &mut rs,
                    PROTOCOL_VERSION,
                )
                .unwrap();
        }
    }

    #[test]
    fn test_get_seal() {
        let fixture = SealsManagerTestFixture::default();
        let mut seals_manager = fixture.create_seals_manager();

        let seal_assert = |seals_manager: &mut SealsManager| {
            let seal = seals_manager
                .get_seal(
                    &fixture.mock_chunk_hash,
                    &fixture.mock_parent_hash,
                    fixture.mock_height,
                    fixture.mock_shard_id,
                )
                .unwrap();
            let demur = match seal {
                Seal::Active(demur) => demur,
                Seal::Past => panic!("Expected ActiveSealDemur"),
            };
            assert_eq!(demur.part_ords.len(), NUM_PARTS_REQUESTED_IN_SEAL);
            assert_eq!(demur.height, fixture.mock_height);
            assert_eq!(demur.chunk_producer, fixture.mock_chunk_producer);
        };

        // SealsManger::get_seal should:

        // 1. return a new seal when one does not exist
        assert!(seals_manager.active_demurs.is_empty());
        seal_assert(&mut seals_manager);
        assert_eq!(seals_manager.active_demurs.len(), 1);

        // 2. return the same seal when it is already created
        seal_assert(&mut seals_manager);
        assert_eq!(seals_manager.active_demurs.len(), 1);
    }

    #[test]
    fn test_approve_chunk() {
        let fixture = SealsManagerTestFixture::default();
        let mut seals_manager = fixture.create_seals_manager();

        // SealsManager::approve_chunk should indicate all parts were retrieved and
        // move the seal into the past seals map.
        fixture.create_seal(&mut seals_manager);
        seals_manager.approve_chunk(fixture.mock_height, &fixture.mock_chunk_hash);
        assert!(seals_manager.active_demurs.is_empty());
        assert!(seals_manager.should_trust_chunk_producer(&fixture.mock_chunk_producer));
        assert!(seals_manager
            .past_seals
            .get(&fixture.mock_height)
            .unwrap()
            .contains(&fixture.mock_chunk_hash));
    }

    // TODO(#3180): seals are disabled in single shard setting
    /*#[test]
    fn test_track_seals() {
        let fixture = SealsManagerTestFixture::default();
        let mut seals_manager = fixture.create_seals_manager();

        // create a seal with old timestamp
        fixture.create_expired_seal(
            &mut seals_manager,
            &fixture.mock_chunk_hash,
            &fixture.mock_parent_hash,
            fixture.mock_height,
        );

        // SealsManager::track_seals should:

        // 1. mark the chunk producer as faulty if the parts were not retrieved and
        //    move the seal into the past seals map
        seals_manager.track_seals();
        assert!(!seals_manager.should_trust_chunk_producer(&fixture.mock_chunk_producer));
        assert!(seals_manager.active_demurs.is_empty());
        assert!(seals_manager
            .past_seals
            .get(&fixture.mock_height)
            .unwrap()
            .contains(&fixture.mock_chunk_hash));

        // 2. remove seals older than the USED_SEAL_HEIGHT_HORIZON
        fixture.create_expired_seal(
            &mut seals_manager,
            &fixture.mock_distant_chunk_hash,
            &fixture.mock_distant_block_hash,
            fixture.mock_height + PAST_SEAL_HEIGHT_HORIZON + 1,
        );
        seals_manager.track_seals();
        assert!(seals_manager.active_demurs.is_empty());
        assert!(seals_manager.past_seals.get(&fixture.mock_height).is_none());
    }*/

    #[test]
    fn test_chunk_forwarding() {
        // When ShardsManager receives parts it owns, it should forward them to the shard trackers
        // and not request any parts (yet).
        let mut fixture = ChunkForwardingTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_chunk_part_owner.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
        );
        let partial_encoded_chunk = fixture.make_partial_encoded_chunk(&fixture.mock_part_ords);
        let result = shards_manager
            .process_partial_encoded_chunk(
                MaybeValidated::NotValidated(&partial_encoded_chunk),
                &mut fixture.chain_store,
                &mut fixture.rs,
                PROTOCOL_VERSION,
            )
            .unwrap();
        match result {
            ProcessPartialEncodedChunkResult::NeedMorePartsOrReceipts => shards_manager
                .request_chunk_single(&fixture.mock_chunk_header, None, PROTOCOL_VERSION),

            _ => panic!("Expected to need more parts!"),
        }
        let count_forwards_and_requests = |fixture: &ChunkForwardingTestFixture| -> (usize, usize) {
            let mut forwards_count = 0;
            let mut requests_count = 0;
            fixture.mock_network.requests.read().unwrap().iter().for_each(|r| match r {
                NetworkRequests::PartialEncodedChunkForward { .. } => forwards_count += 1,
                NetworkRequests::PartialEncodedChunkRequest { .. } => requests_count += 1,
                _ => (),
            });
            (forwards_count, requests_count)
        };

        let (forwards_count, requests_count) = count_forwards_and_requests(&fixture);
        assert!(forwards_count > 0);
        assert_eq!(requests_count, 0);

        // After some time, we should send requests if we have not been forwarded the parts
        // we need.
        std::thread::sleep(Duration::from_millis(2 * CHUNK_REQUEST_RETRY_MS));
        let head = Tip {
            height: 0,
            last_block_hash: Default::default(),
            prev_block_hash: Default::default(),
            epoch_id: Default::default(),
            next_epoch_id: Default::default(),
        };
        shards_manager.resend_chunk_requests(&head);
        let (_, requests_count) = count_forwards_and_requests(&fixture);
        assert!(requests_count > 0);
    }

    #[test]
    fn test_receive_forward_before_header() {
        // When a node receives a chunk forward before the chunk header, it should store
        // the forward and use it when it receives the header
        let mut fixture = ChunkForwardingTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
        );
        let (most_parts, other_parts) = {
            let mut most_parts = fixture.mock_chunk_parts.clone();
            let n = most_parts.len();
            let other_parts = most_parts.split_off(n - (n / 4));
            (most_parts, other_parts)
        };
        let forward = PartialEncodedChunkForwardMsg::from_header_and_parts(
            &fixture.mock_chunk_header,
            most_parts,
        );
        shards_manager.insert_forwarded_chunk(forward);
        let partial_encoded_chunk = PartialEncodedChunkV2 {
            header: fixture.mock_chunk_header.clone(),
            parts: other_parts,
            receipts: Vec::new(),
        };
        let result = shards_manager
            .process_partial_encoded_chunk(
                MaybeValidated::NotValidated(&partial_encoded_chunk),
                &mut fixture.chain_store,
                &mut fixture.rs,
                PROTOCOL_VERSION,
            )
            .unwrap();

        match result {
            ProcessPartialEncodedChunkResult::HaveAllPartsAndReceipts(_) => (),
            other_result => panic!("Expected HaveAllPartsAndReceipts, but got {:?}", other_result),
        }
        // No requests should have been sent since all the required parts were contained in the
        // forwarded parts.
        assert!(fixture
            .mock_network
            .requests
            .read()
            .unwrap()
            .iter()
            .find(|r| match r {
                NetworkRequests::PartialEncodedChunkRequest { .. } => true,
                _ => false,
            })
            .is_none());
    }
}
