#[cfg(feature = "nightly")]
mod tests {
    use crate::ChainStoreAccess;
    use crate::test_utils::{setup, setup_with_tx_validity_period};
    use near_async::time::{Duration, FakeClock, Utc};
    use near_crypto::{KeyType, PublicKey};
    use near_epoch_manager::EpochManagerAdapter;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::errors::EpochError;
    use near_primitives::hash::CryptoHash;
    use near_primitives::merkle::PartialMerkleTree;
    use near_primitives::stateless_validation::ChunkProductionKey;
    use near_primitives::test_utils::TestBlockBuilder;
    use near_primitives::types::Balance;
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

        // The chunk producer anchored at the block (chunk at block_height + 2)
        // should be stored under (block_hash, shard_id).
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

    /// Verify that saved chunk producers match what epoch_info.sample_chunk_producer
    /// returns at the anchored height (anchor height + 2).
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

        // For an anchor block, verify saved chunk producers match deterministic
        // sampling at anchor height + 2.
        let head = chain.head().unwrap();
        let block = chain.get_block(&head.last_block_hash).unwrap();
        let anchor_hash = block.header().prev_hash();

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(anchor_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
        let height = chain.get_block_header(anchor_hash).unwrap().height() + 2;

        for shard_id in shard_layout.shard_ids() {
            let key = get_block_shard_id(anchor_hash, shard_id);
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
            let epoch_sync_data_hash = if blocks[i].header().is_genesis() {
                epoch_manager.compute_epoch_sync_data_hash(blocks[i].hash()).unwrap()
            } else {
                None
            };
            blocks.push(
                TestBlockBuilder::from_prev_block(clock.clock(), &blocks[i], signer.clone())
                    .block_merkle_tree(&mut block_merkle_tree)
                    .epoch_sync_data_hash(epoch_sync_data_hash)
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

    /// Verify that get_chunk_producer_info_from_prev_block resolves through the
    /// grandparent anchor's DB row, not the parent's: overwrite the anchor row
    /// with a sentinel validator and observe it returned.
    #[test]
    fn test_resolution_reads_anchor_db_row() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        // Build two blocks: b1 (anchor), b2 (parent of the resolved chunk).
        let mut hashes = Vec::new();
        for _ in 0..2 {
            let prev_hash = *chain.head_header().unwrap().hash();
            let prev = chain.get_block(&prev_hash).unwrap();
            clock.advance(Duration::milliseconds(1));
            let block =
                TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer.clone()).build();
            hashes.push(*block.hash());
            chain.process_block_test(block).unwrap();
        }
        let (anchor_hash, parent_hash) = (hashes[0], hashes[1]);

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&parent_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_id = shard_layout.shard_ids().next().unwrap();

        // Overwrite the anchor row with a sentinel validator.
        let sentinel = ValidatorStake::new(
            "sentinel".parse().unwrap(),
            PublicKey::empty(KeyType::ED25519),
            Balance::from_yoctonear(1),
        );
        // ChunkProducers is insert-only: delete the row, then insert the sentinel.
        let mut update = chain.chain_store().store().store_update();
        update.delete(DBCol::ChunkProducers, &get_block_shard_id(&anchor_hash, shard_id));
        update.commit();
        let mut update = chain.chain_store().store().store_update();
        update.insert_ser(
            DBCol::ChunkProducers,
            &get_block_shard_id(&anchor_hash, shard_id),
            &sentinel,
        );
        update.commit();

        let resolved =
            epoch_manager.get_chunk_producer_info_from_prev_block(&parent_hash, shard_id).unwrap();
        assert_eq!(
            resolved.account_id().as_str(),
            "sentinel",
            "resolution must read the DB row keyed by the grandparent anchor"
        );

        // The parent's own row must NOT be consulted for this chunk.
        let parent_row: Option<ValidatorStake> = chain
            .chain_store()
            .store()
            .get_ser(DBCol::ChunkProducers, &get_block_shard_id(&parent_hash, shard_id));
        assert!(parent_row.is_some(), "parent row exists (seeded for later chunks)");
        assert_ne!(parent_row.unwrap().account_id().as_str(), "sentinel");
    }

    /// A chunk whose grandparent anchor belongs to the previous epoch (first <=2
    /// blocks of an epoch) must resolve via the canonical sampler, ignoring the
    /// anchor's DB row.
    #[test]
    // TestBlockBuilder does not maintain spice's prev_last_certified_block_epoch_id
    // across epoch boundaries, so header validation rejects the boundary block.
    #[cfg_attr(feature = "protocol_feature_spice", ignore)]
    fn test_cross_epoch_anchor_resolves_canonically() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) =
            setup_with_tx_validity_period(clock.clock(), 10, 5);

        // Build enough blocks to cross an epoch boundary. `TestBlockBuilder`
        // copies the parent's epoch by default, so set the epoch ids explicitly.
        let mut hashes = vec![*chain.genesis().hash()];
        for _ in 0..12 {
            let prev_hash = *chain.head_header().unwrap().hash();
            let prev = chain.get_block(&prev_hash).unwrap();
            let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_hash).unwrap();
            let next_epoch_id =
                epoch_manager.get_next_epoch_id_from_prev_block(&prev_hash).unwrap();
            clock.advance(Duration::milliseconds(1));
            let block = TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer.clone())
                .epoch_id(epoch_id)
                .next_epoch_id(next_epoch_id)
                .build();
            hashes.push(*block.hash());
            chain.process_block_test(block).unwrap();
        }

        let sentinel = ValidatorStake::new(
            "sentinel".parse().unwrap(),
            PublicKey::empty(KeyType::ED25519),
            Balance::from_yoctonear(1),
        );

        // Find (anchor, parent) pairs straddling an epoch boundary.
        let mut cross_epoch_pairs = 0;
        for pair in hashes.windows(2) {
            let (anchor_hash, parent_hash) = (pair[0], pair[1]);
            let Ok(anchor_epoch) = epoch_manager.get_epoch_id(&anchor_hash) else {
                continue;
            };
            let chunk_epoch = epoch_manager.get_epoch_id_from_prev_block(&parent_hash).unwrap();
            if anchor_epoch == chunk_epoch {
                continue;
            }
            cross_epoch_pairs += 1;

            let shard_layout = epoch_manager.get_shard_layout(&chunk_epoch).unwrap();
            let shard_id = shard_layout.shard_ids().next().unwrap();
            // Poison the anchor row (insert-only column: delete then insert);
            // the cross-epoch arm must never read it.
            let mut update = chain.chain_store().store().store_update();
            update.delete(DBCol::ChunkProducers, &get_block_shard_id(&anchor_hash, shard_id));
            update.commit();
            let mut update = chain.chain_store().store().store_update();
            update.insert_ser(
                DBCol::ChunkProducers,
                &get_block_shard_id(&anchor_hash, shard_id),
                &sentinel,
            );
            update.commit();

            let resolved = epoch_manager
                .get_chunk_producer_info_from_prev_block(&parent_hash, shard_id)
                .unwrap();
            let height = chain.get_block_header(&parent_hash).unwrap().height() + 1;
            let canonical = epoch_manager
                .get_chunk_producer_info(&ChunkProductionKey {
                    epoch_id: chunk_epoch,
                    height_created: height,
                    shard_id,
                })
                .unwrap();
            assert_eq!(
                resolved.account_id(),
                canonical.account_id(),
                "cross-epoch anchor must resolve canonically",
            );
            assert_ne!(resolved.account_id().as_str(), "sentinel");
        }
        assert!(cross_epoch_pairs > 0, "test must exercise at least one epoch boundary");
    }

    /// Chunks at genesis + 1 have no real grandparent and resolve canonically.
    #[test]
    fn test_low_height_resolves_canonically() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        let (chain, epoch_manager, _, _) = setup(clock.clock());

        let genesis_hash = *chain.genesis().hash();
        let genesis_height = chain.genesis().height();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

        assert_eq!(
            epoch_manager.grandparent_anchor(&genesis_hash).unwrap(),
            None,
            "genesis parent implies no grandparent anchor"
        );
        for shard_id in shard_layout.shard_ids() {
            // No grandparent: must not error, must match the canonical sampler.
            let resolved = epoch_manager
                .get_chunk_producer_info_from_prev_block(&genesis_hash, shard_id)
                .unwrap();
            let canonical = epoch_manager
                .get_chunk_producer_info(&ChunkProductionKey {
                    epoch_id,
                    height_created: genesis_height + 1,
                    shard_id,
                })
                .unwrap();
            assert_eq!(resolved.account_id(), canonical.account_id());

            // The wire sentinel (default hash) also routes to the canonical sampler.
            let resolved = epoch_manager
                .get_chunk_producer_info_anchored(
                    Some(&CryptoHash::default()),
                    &epoch_id,
                    genesis_height + 1,
                    shard_id,
                )
                .unwrap();
            assert_eq!(resolved.account_id(), canonical.account_id());
        }
    }

    /// Verify that resolution errors on a missing anchor DB row when EarlyKickout
    /// is enabled (strict read).
    #[test]
    fn test_resolution_errors_on_anchor_db_miss() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        // Build two blocks; chunks on top of the second anchor at the first.
        let mut hashes = Vec::new();
        for _ in 0..2 {
            let prev_hash = *chain.head_header().unwrap().hash();
            let prev = chain.get_block(&prev_hash).unwrap();
            clock.advance(Duration::milliseconds(1));
            let block =
                TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer.clone()).build();
            hashes.push(*block.hash());
            chain.process_block_test(block).unwrap();
        }
        let (anchor_hash, parent_hash) = (hashes[0], hashes[1]);

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&parent_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

        // Succeeds while the anchor rows are present.
        for shard_id in shard_layout.shard_ids() {
            assert!(
                epoch_manager
                    .get_chunk_producer_info_from_prev_block(&parent_hash, shard_id)
                    .is_ok(),
                "should succeed when DB is populated, shard {shard_id}"
            );
        }

        // Delete the anchor rows to simulate a miss.
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
                .get_chunk_producer_info_from_prev_block(&parent_hash, shard_id)
                .unwrap_err();
            assert!(
                matches!(err, EpochError::ChunkProducerNotInDB(_, _)),
                "expected ChunkProducerNotInDB, got {err:?}"
            );
        }
    }

    /// An unprocessed anchor surfaces as MissingBlock (node two or more blocks
    /// behind the chunk).
    #[test]
    fn test_resolution_errors_on_unprocessed_anchor() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        let (chain, epoch_manager, _, _) = setup(clock.clock());

        let genesis_hash = *chain.genesis().hash();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_id = shard_layout.shard_ids().next().unwrap();

        let unknown_anchor = CryptoHash::hash_bytes(b"unprocessed_anchor");
        let err = epoch_manager
            .get_chunk_producer_info_anchored(Some(&unknown_anchor), &epoch_id, 3, shard_id)
            .unwrap_err();
        assert!(
            matches!(err, EpochError::MissingBlock(_)),
            "expected MissingBlock for unprocessed anchor, got {err:?}"
        );
    }
}

/// With EarlyKickout disabled (stable build), resolution must be byte-identical
/// to the legacy CPK computation.
#[cfg(not(feature = "nightly"))]
mod stable_tests {
    use crate::test_utils::setup;
    use near_async::time::{Duration, FakeClock, Utc};
    use near_epoch_manager::EpochManagerAdapter;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::stateless_validation::ChunkProductionKey;
    use near_primitives::test_utils::TestBlockBuilder;

    #[test]
    fn test_resolution_matches_legacy_computation_when_feature_off() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        let prev = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        clock.advance(Duration::milliseconds(1));
        let block = TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer).build();
        let block_hash = *block.hash();
        let block_height = block.header().height();
        chain.process_block_test(block).unwrap();

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&block_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        for shard_id in shard_layout.shard_ids() {
            let resolved = epoch_manager
                .get_chunk_producer_info_from_prev_block(&block_hash, shard_id)
                .unwrap();
            let legacy = epoch_manager
                .get_chunk_producer_info(&ChunkProductionKey {
                    epoch_id,
                    height_created: block_height + 1,
                    shard_id,
                })
                .unwrap();
            assert_eq!(resolved.account_id(), legacy.account_id());
        }
    }
}
