#[cfg(feature = "nightly")]
mod tests {
    use crate::ChainStoreAccess;
    use crate::test_utils::setup;
    use near_async::time::{Duration, FakeClock, Utc};
    use near_epoch_manager::EpochManagerAdapter;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::epoch_info::ChunkProducerKickoutState;
    use near_primitives::errors::EpochError;
    use near_primitives::hash::CryptoHash;
    use near_primitives::merkle::PartialMerkleTree;
    use near_primitives::stateless_validation::ChunkProductionKey;
    use near_primitives::test_utils::TestBlockBuilder;
    use near_primitives::utils::get_block_shard_id;
    use near_store::DBCol;

    /// Verify that the ChunkProducers column holds the identity kickout state
    /// for the genesis block.
    #[test]
    fn test_kickout_state_populated_at_genesis() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        let (chain, epoch_manager, _, _) = setup(clock.clock());

        let genesis_hash = chain.genesis().hash();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(genesis_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

        // For each shard, the identity kickout state should be stored in the DB.
        for shard_id in shard_layout.shard_ids() {
            let key = get_block_shard_id(genesis_hash, shard_id);
            let value: Option<ChunkProducerKickoutState> =
                chain.chain_store().store().get_ser(DBCol::ChunkProducers, &key);
            assert_eq!(
                value,
                Some(ChunkProducerKickoutState::identity()),
                "identity kickout state should be stored for genesis block, shard {shard_id}"
            );
        }
    }

    /// Verify that the kickout state is also seeded under CryptoHash::default(),
    /// the anchor of chunks at height genesis+1.
    #[test]
    fn test_kickout_state_seeded_under_default_hash() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        let (chain, epoch_manager, _, _) = setup(clock.clock());

        let default_hash = CryptoHash::default();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&default_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

        for shard_id in shard_layout.shard_ids() {
            let key = get_block_shard_id(&default_hash, shard_id);
            let value: Option<ChunkProducerKickoutState> =
                chain.chain_store().store().get_ser(DBCol::ChunkProducers, &key);
            assert_eq!(
                value,
                Some(ChunkProducerKickoutState::identity()),
                "identity kickout state should be seeded under the default hash, shard {shard_id}"
            );
        }
    }

    /// Verify that the ChunkProducers column is populated after processing a block.
    #[test]
    fn test_kickout_state_populated_after_block_processing() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        // Process a block on top of genesis.
        let prev = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        clock.advance(Duration::milliseconds(1));
        let block = TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer).build();
        let block_hash = *block.hash();
        chain.process_block_test(block).unwrap();

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&block_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

        // The kickout state as of this block should be stored under (block_hash, shard_id).
        for shard_id in shard_layout.shard_ids() {
            let key = get_block_shard_id(&block_hash, shard_id);
            let value: Option<ChunkProducerKickoutState> =
                chain.chain_store().store().get_ser(DBCol::ChunkProducers, &key);
            assert_eq!(
                value,
                Some(ChunkProducerKickoutState::identity()),
                "kickout state should be stored after block processing, shard {shard_id}"
            );
        }
    }

    /// Verify that the ChunkProducers column is populated after header sync
    /// (a different code path from block processing).
    #[test]
    fn test_kickout_state_populated_after_header_sync() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        // Build blocks but don't process them — we'll sync just the headers.
        let mut blocks = vec![chain.get_block(&chain.genesis().hash().clone()).unwrap()];
        let mut block_merkle_tree = PartialMerkleTree::default();
        for i in 0..3 {
            clock.advance(Duration::milliseconds(1));
            blocks.push(
                TestBlockBuilder::from_prev_block(clock.clock(), &blocks[i], signer.clone())
                    .block_merkle_tree(&mut block_merkle_tree)
                    .build(),
            );
        }

        // Sync only the headers (skip genesis which is already known).
        chain
            .sync_block_headers(blocks[1..].iter().map(|b| b.header().clone().into()).collect())
            .unwrap();

        // Verify the kickout state is stored for each synced header.
        for block in &blocks[1..] {
            let header_hash = block.hash();
            let epoch_id = epoch_manager.get_epoch_id_from_prev_block(header_hash).unwrap();
            let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
            for shard_id in shard_layout.shard_ids() {
                let key = get_block_shard_id(header_hash, shard_id);
                let value: Option<ChunkProducerKickoutState> =
                    chain.chain_store().store().get_ser(DBCol::ChunkProducers, &key);
                assert!(
                    value.is_some(),
                    "kickout state should be stored after header sync, height {}, shard {shard_id}",
                    block.header().height()
                );
            }
        }
    }

    /// Parity: with the identity kickout state, the anchored lookup
    /// (get_chunk_producer_info_from_prev_block) must agree with the static
    /// CPK-based computation for every shard of every processed block.
    #[test]
    fn test_anchored_lookup_matches_static_sampling() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        // Build a small chain.
        let mut block_hashes = vec![*chain.genesis().hash()];
        for _ in 0..3 {
            let prev_hash = *chain.head_header().unwrap().hash();
            let prev = chain.get_block(&prev_hash).unwrap();
            clock.advance(Duration::milliseconds(1));
            let block =
                TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer.clone()).build();
            block_hashes.push(*block.hash());
            chain.process_block_test(block).unwrap();
        }

        for prev_block_hash in &block_hashes {
            let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash).unwrap();
            let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
            let height = epoch_manager.get_block_info(prev_block_hash).unwrap().height() + 1;

            for shard_id in shard_layout.shard_ids() {
                let anchored = epoch_manager
                    .get_chunk_producer_info_from_prev_block(prev_block_hash, shard_id)
                    .unwrap();
                let cpk = ChunkProductionKey { epoch_id, height_created: height, shard_id };
                let from_computation = epoch_manager.get_chunk_producer_info(&cpk).unwrap();
                assert_eq!(
                    anchored.account_id(),
                    from_computation.account_id(),
                    "anchored and computation-based lookups should agree, shard {shard_id}"
                );
            }
        }
    }

    /// Skip-bug regression: the anchored resolver must sample at the SIGNED
    /// height, not at anchor.height + 2. Simulate skipped heights by resolving
    /// heights beyond anchor.height + 2 against the same anchor.
    #[test]
    fn test_anchored_lookup_uses_signed_height() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        let prev = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        clock.advance(Duration::milliseconds(1));
        let block = TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer).build();
        let anchor_hash = *block.hash();
        chain.process_block_test(block).unwrap();

        let anchor_height = epoch_manager.get_block_info(&anchor_hash).unwrap().height();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&anchor_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();

        // Heights with 0..3 skipped blocks between the anchor and the chunk.
        for height in anchor_height + 2..anchor_height + 5 {
            for shard_id in shard_layout.shard_ids() {
                let anchored = epoch_manager
                    .get_chunk_producer_info_anchored(&anchor_hash, &epoch_id, height, shard_id)
                    .unwrap();
                let expected_id =
                    epoch_info.sample_chunk_producer(&shard_layout, shard_id, height).unwrap();
                let expected = epoch_info.get_validator(expected_id);
                assert_eq!(
                    anchored.account_id(),
                    expected.account_id(),
                    "anchored lookup must sample at the signed height {height}, shard {shard_id}"
                );
            }
        }
    }

    /// Verify that the anchored lookup errors on a DB miss when EarlyKickout is
    /// enabled for the anchor's epoch.
    #[test]
    fn test_anchored_lookup_errors_on_db_miss() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        // Process a block so the epoch manager knows about it.
        let prev = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        clock.advance(Duration::milliseconds(1));
        let block = TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer).build();
        let block_hash = *block.hash();
        chain.process_block_test(block).unwrap();

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&block_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

        // Succeeds while the anchor rows are present.
        for shard_id in shard_layout.shard_ids() {
            assert!(
                epoch_manager
                    .get_chunk_producer_info_from_prev_block(&block_hash, shard_id)
                    .is_ok(),
                "should succeed when DB is populated, shard {shard_id}"
            );
        }

        // Delete the anchor's kickout state rows to simulate a miss.
        // from_prev_block(&block_hash) anchors on the block's prev (genesis).
        let anchor_hash = *chain.genesis().hash();
        {
            let mut store_update = chain.chain_store().store().store_update();
            for shard_id in shard_layout.shard_ids() {
                let key = get_block_shard_id(&anchor_hash, shard_id);
                store_update.delete(DBCol::ChunkProducers, &key);
            }
            store_update.commit();
        }

        // Should return ChunkProducerNotInDB on miss when EarlyKickout is enabled.
        for shard_id in shard_layout.shard_ids() {
            let err = epoch_manager
                .get_chunk_producer_info_from_prev_block(&block_hash, shard_id)
                .unwrap_err();
            assert!(
                matches!(err, EpochError::ChunkProducerNotInDB(_, _)),
                "expected ChunkProducerNotInDB, got {err:?}"
            );
        }
    }

    /// Verify that an unknown anchor surfaces as MissingBlock (the witness path
    /// treats this as "behind, drop").
    #[test]
    fn test_anchored_lookup_unknown_anchor_is_missing_block() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        let (chain, epoch_manager, _, _) = setup(clock.clock());

        let genesis_hash = *chain.genesis().hash();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_id = shard_layout.shard_ids().next().unwrap();

        let unknown_anchor = CryptoHash::hash_bytes(b"unknown_anchor");
        let err = epoch_manager
            .get_chunk_producer_info_anchored(&unknown_anchor, &epoch_id, 3, shard_id)
            .unwrap_err();
        assert!(
            matches!(err, EpochError::MissingBlock(_)),
            "expected MissingBlock for unknown anchor, got {err:?}"
        );
    }
}
