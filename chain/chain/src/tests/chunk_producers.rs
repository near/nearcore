#[cfg(feature = "nightly")]
mod tests {
    use crate::ChainStoreAccess;
    use crate::test_utils::setup;
    use near_async::time::{Duration, FakeClock, Utc};
    use near_epoch_manager::EpochManagerAdapter;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::merkle::PartialMerkleTree;
    use near_primitives::stateless_validation::ChunkProductionKey;
    use near_primitives::test_utils::TestBlockBuilder;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::utils::get_block_shard_id;
    use near_store::DBCol;

    /// Verify that the ChunkProducers column is populated for the genesis block.
    #[test]
    fn test_chunk_producers_populated_at_genesis() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        let (chain, epoch_manager, _, _) = setup(clock.clock());

        let genesis_hash = chain.genesis().hash();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(genesis_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

        // For each shard, the chunk producer should be stored in the DB.
        for shard_id in shard_layout.shard_ids() {
            let key = get_block_shard_id(genesis_hash, shard_id);
            let value: Option<ValidatorStake> =
                chain.chain_store().store().get_ser(DBCol::ChunkProducers, &key);
            assert!(
                value.is_some(),
                "chunk producer should be stored for genesis block, shard {shard_id}"
            );
        }
    }

    /// Verify that the ChunkProducers column is populated after processing a block.
    #[test]
    fn test_chunk_producers_populated_after_block_processing() {
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

        // The chunk producer for height block_height+1 should be stored under (block_hash, shard_id).
        for shard_id in shard_layout.shard_ids() {
            let key = get_block_shard_id(&block_hash, shard_id);
            let value: Option<ValidatorStake> =
                chain.chain_store().store().get_ser(DBCol::ChunkProducers, &key);
            assert!(
                value.is_some(),
                "chunk producer should be stored after block processing, shard {shard_id}"
            );
        }
    }

    /// Verify that saved chunk producers match what epoch_info.sample_chunk_producer returns.
    #[test]
    fn test_chunk_producers_match_sampling() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        // Build a small chain.
        for _ in 0..3 {
            let prev_hash = *chain.head_header().unwrap().hash();
            let prev = chain.get_block(&prev_hash).unwrap();
            clock.advance(Duration::milliseconds(1));
            let block =
                TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer.clone()).build();
            chain.process_block_test(block).unwrap();
        }

        // For each block, verify saved chunk producers match deterministic sampling.
        let head = chain.head().unwrap();
        let block = chain.get_block(&head.last_block_hash).unwrap();
        let prev_block_hash = block.header().prev_hash();

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
        let height = chain.get_block_header(prev_block_hash).unwrap().height() + 1;

        for shard_id in shard_layout.shard_ids() {
            let key = get_block_shard_id(prev_block_hash, shard_id);
            let stored: ValidatorStake = chain
                .chain_store()
                .store()
                .get_ser(DBCol::ChunkProducers, &key)
                .expect("chunk producer should be stored");

            let expected_validator_id =
                epoch_info.sample_chunk_producer(&shard_layout, shard_id, height).unwrap();
            let expected = epoch_info.get_validator(expected_validator_id);

            assert_eq!(
                stored.account_id(),
                expected.account_id(),
                "stored chunk producer should match deterministic sampling for shard {shard_id}"
            );
        }
    }

    /// Verify that the ChunkProducers column is populated after header sync
    /// (a different code path from block processing).
    #[test]
    fn test_chunk_producers_populated_after_header_sync() {
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

        // Verify chunk producers are stored for each synced header.
        for block in &blocks[1..] {
            let header_hash = block.hash();
            let epoch_id = epoch_manager.get_epoch_id_from_prev_block(header_hash).unwrap();
            let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
            for shard_id in shard_layout.shard_ids() {
                let key = get_block_shard_id(header_hash, shard_id);
                let value: Option<ValidatorStake> =
                    chain.chain_store().store().get_ser(DBCol::ChunkProducers, &key);
                assert!(
                    value.is_some(),
                    "chunk producer should be stored after header sync, height {}, shard {shard_id}",
                    block.header().height()
                );
            }
        }
    }

    /// Verify that get_chunk_producer_info_db reads from DB and matches
    /// the result of get_chunk_producer_info (CPK-based computation).
    #[test]
    fn test_get_chunk_producer_info_db_reads_from_db() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        // Process a block so the DB is populated.
        let prev = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        clock.advance(Duration::milliseconds(1));
        let block = TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer).build();
        let block_hash = *block.hash();
        chain.process_block_test(block).unwrap();

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&block_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let block_info = epoch_manager.get_block_info(&block_hash).unwrap();
        let height = block_info.height() + 1;

        for shard_id in shard_layout.shard_ids() {
            // get_chunk_producer_info_db reads from DB (strict when EarlyKickout enabled).
            let from_db = epoch_manager.get_chunk_producer_info_db(&block_hash, shard_id).unwrap();
            // get_chunk_producer_info uses CPK-based computation.
            let cpk = ChunkProductionKey { epoch_id, height_created: height, shard_id };
            let from_computation = epoch_manager.get_chunk_producer_info(&cpk).unwrap();
            assert_eq!(
                from_db.account_id(),
                from_computation.account_id(),
                "DB-backed and computation-based lookups should agree, shard {shard_id}"
            );
        }
    }

    /// Verify that get_chunk_producer_info_db errors on DB miss when EarlyKickout is enabled.
    #[test]
    fn test_get_chunk_producer_info_db_errors_on_db_miss() {
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

        // Succeeds when DB is populated.
        for shard_id in shard_layout.shard_ids() {
            assert!(
                epoch_manager.get_chunk_producer_info_db(&block_hash, shard_id).is_ok(),
                "should succeed when DB is populated, shard {shard_id}"
            );
        }

        // Delete chunk producers from DB to simulate a miss.
        {
            let mut store_update = chain.chain_store().store().store_update();
            for shard_id in shard_layout.shard_ids() {
                let key = get_block_shard_id(&block_hash, shard_id);
                store_update.delete(DBCol::ChunkProducers, &key);
            }
            store_update.commit();
        }

        // Should error on miss when EarlyKickout is enabled.
        for shard_id in shard_layout.shard_ids() {
            assert!(
                epoch_manager.get_chunk_producer_info_db(&block_hash, shard_id).is_err(),
                "should error on DB miss, shard {shard_id}"
            );
        }
    }
}
