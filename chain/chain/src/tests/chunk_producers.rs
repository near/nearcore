#[cfg(feature = "nightly")]
mod tests {
    use crate::Chain;
    use crate::ChainStoreAccess;
    use crate::garbage_collection::GCMode;
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
    use std::collections::HashSet;

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

    /// Saved chunk producers match the sampler at the anchored height (anchor + 2).
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

            // With no misses the blacklist is empty, so the write path's
            // `sample_chunk_producer_excluding(&empty)` must equal `sample_chunk_producer`.
            let empty = HashSet::new();
            let excluding_id = epoch_info
                .sample_chunk_producer_excluding(&shard_layout, shard_id, height, &empty)
                .unwrap();
            assert_eq!(
                excluding_id, expected_validator_id,
                "empty-blacklist sampling must match default sampling for shard {shard_id}"
            );
        }
    }

    /// ChunkProducers is populated after header sync (a path distinct from block processing).
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

    /// Resolution reads the grandparent anchor's DB row, not the parent's.
    #[test]
    fn test_resolution_reads_anchor_db_row() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

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

        let sentinel = ValidatorStake::new(
            "sentinel".parse().unwrap(),
            PublicKey::empty(KeyType::ED25519),
            Balance::from_yoctonear(1),
        );
        // ChunkProducers is insert-only: delete then insert.
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

    /// A skipped slot between grandparent and parent does not change resolution. The chunk's
    /// height_created exceeds anchor.height + 2, yet both self-select and witness resolution read
    /// DB[anchor] (keyed by hash, height-independent) and return the same producer.
    #[test]
    fn test_resolution_consistent_across_skipped_slot() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        // Grandparent anchor G, built consecutively on genesis.
        let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        clock.advance(Duration::milliseconds(1));
        let anchor =
            TestBlockBuilder::from_prev_block(clock.clock(), &genesis, signer.clone()).build();
        let anchor_hash = *anchor.hash();
        let anchor_height = anchor.header().height();
        chain.process_block_test(anchor).unwrap();

        // Parent P at G.height + 2: slot G.height + 1 is skipped (no block there).
        let anchor_block = chain.get_block(&anchor_hash).unwrap();
        clock.advance(Duration::milliseconds(1));
        let parent = TestBlockBuilder::from_prev_block(clock.clock(), &anchor_block, signer)
            .height(anchor_height + 2)
            .build();
        let parent_hash = *parent.hash();
        let parent_height = parent.header().height();
        chain.process_block_test(parent).unwrap();

        // Chunk built on P: height_created = P.height + 1 = G.height + 3 > anchor.height + 2.
        let height_created = parent_height + 1;
        assert_eq!(height_created, anchor_height + 3, "skipped slot G.height + 1");

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&parent_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_id = shard_layout.shard_ids().next().unwrap();

        // Poison DB[anchor] with a sentinel the sampler would never produce. Both
        // self-select and witness resolution must return it, proving each reads
        // DB[anchor] keyed by hash and ignores height_created — which the skipped slot
        // pushed to anchor.height + 3, two slots past the anchor's own sampled height.
        // A single-validator setup makes sampler-based guards vacuous; the sentinel does
        // not, so it is the decisive check that resolution is DB-keyed, not height-resampled.
        let sentinel = ValidatorStake::new(
            "sentinel".parse().unwrap(),
            PublicKey::empty(KeyType::ED25519),
            Balance::from_yoctonear(1),
        );
        // ChunkProducers is insert-only: delete then insert.
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

        let self_select =
            epoch_manager.get_chunk_producer_info_from_prev_block(&parent_hash, shard_id).unwrap();
        let witness = epoch_manager
            .get_chunk_producer_info_anchored(
                Some(&anchor_hash),
                &epoch_id,
                height_created,
                shard_id,
            )
            .unwrap();
        assert_eq!(
            self_select.account_id().as_str(),
            "sentinel",
            "self-select must read DB[anchor] across a skipped slot, not resample by height",
        );
        assert_eq!(
            witness.account_id().as_str(),
            "sentinel",
            "witness resolution must read DB[anchor] across a skipped slot, not resample by height",
        );
    }

    /// A cross-epoch grandparent anchor resolves via the canonical sampler, ignoring its DB row.
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

    /// Hot GC deletes a below-boundary anchor's ChunkProducers rows while retaining rows for
    /// anchors that later blocks still resolve against.
    #[test]
    fn test_chunk_producers_garbage_collected_with_block() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        let mut hashes = vec![*chain.genesis().hash()];
        for _ in 0..9 {
            let prev_hash = *chain.head_header().unwrap().hash();
            let prev = chain.get_block(&prev_hash).unwrap();
            clock.advance(Duration::milliseconds(1));
            let block =
                TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer.clone()).build();
            hashes.push(*block.hash());
            chain.process_block_test(block).unwrap();
        }

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&hashes[1]).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_id = shard_layout.shard_ids().next().unwrap();

        let row = |chain: &Chain, anchor: &CryptoHash| -> Option<ValidatorStake> {
            chain
                .chain_store()
                .store()
                .get_ser(DBCol::ChunkProducers, &get_block_shard_id(anchor, shard_id))
        };

        for anchor in &hashes {
            assert!(row(&chain, anchor).is_some(), "row should exist before GC for {anchor}");
        }

        // GCMode::Canonical clears block_hash.prev, so passing hashes[5] clears anchor hashes[4].
        let cleared_anchor = hashes[4];
        let input_hash = hashes[5];
        let tries = chain.runtime_adapter.get_tries();
        let mut store_update = chain.mut_chain_store().store_update();
        store_update
            .clear_block_data(epoch_manager.as_ref(), input_hash, GCMode::Canonical(tries))
            .unwrap();
        store_update.commit().unwrap();

        assert!(
            row(&chain, &cleared_anchor).is_none(),
            "below-boundary anchor row must be deleted"
        );
        assert!(row(&chain, &input_hash).is_some(), "canonical GC clears prev, not the input hash");

        // The chunk built on hashes[7] anchors on its grandparent hashes[6].
        assert!(row(&chain, &hashes[6]).is_some(), "above-boundary anchor row must be retained");
        assert!(
            epoch_manager.get_chunk_producer_info_from_prev_block(&hashes[7], shard_id).is_ok(),
            "retained anchor must still resolve"
        );
    }

    /// GCMode::Fork deletes a fork block's ChunkProducers rows too. The delete lives in the
    /// shared clear_block_data body, so Fork and Canonical run the same line today; this is a
    /// regression guard in case the two ever stop sharing it.
    #[test]
    fn test_chunk_producers_garbage_collected_on_fork() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        // Canonical chain: genesis + blocks 1..=3 (head = height 3).
        let mut hashes = vec![*chain.genesis().hash()];
        for _ in 0..3 {
            let prev_hash = *chain.head_header().unwrap().hash();
            let prev = chain.get_block(&prev_hash).unwrap();
            clock.advance(Duration::milliseconds(1));
            let block =
                TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer.clone()).build();
            hashes.push(*block.hash());
            chain.process_block_test(block).unwrap();
        }

        // Fork sibling with a body at height 3 (built on canonical block 2, distinct clock tick
        // so its hash differs from block 3). It is a leaf, so its refcount is 0 and it can be
        // cleared as a fork.
        let parent = chain.get_block(&hashes[2]).unwrap();
        clock.advance(Duration::milliseconds(1));
        let fork = TestBlockBuilder::from_prev_block(clock.clock(), &parent, signer).build();
        let fork_hash = *fork.hash();
        assert_eq!(fork.header().height(), 3, "fork sibling must be at height 3");
        assert_ne!(fork_hash, hashes[3], "fork must differ from canonical block 3");
        chain.process_block_test(fork).unwrap();

        // Confirm the fork is off the canonical chain (head did not switch to it), so
        // GCMode::Fork applies.
        assert_ne!(
            chain.head().unwrap().last_block_hash,
            fork_hash,
            "fork must not be the canonical head"
        );

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&hashes[1]).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_id = shard_layout.shard_ids().next().unwrap();
        let row = |chain: &Chain| -> Option<ValidatorStake> {
            chain
                .chain_store()
                .store()
                .get_ser(DBCol::ChunkProducers, &get_block_shard_id(&fork_hash, shard_id))
        };
        assert!(row(&chain).is_some(), "fork's row should exist before GC");

        // GCMode::Fork clears the given block directly (no prev reassignment).
        let tries = chain.runtime_adapter.get_tries();
        let mut store_update = chain.mut_chain_store().store_update();
        store_update
            .clear_block_data(epoch_manager.as_ref(), fork_hash, GCMode::Fork(tries))
            .unwrap();
        store_update.commit().unwrap();

        assert!(row(&chain).is_none(), "fork block's ChunkProducers rows must be deleted");
    }

    /// clear_head_block_data (the undo-block path) deletes ChunkProducers exactly where it
    /// deletes BlockInfo: for the body head only. Header-only anchors above the body head keep
    /// both their BlockInfo and their ChunkProducers rows, so the rows stay valid and survive a
    /// header re-sync. Mirroring BlockInfo lets re-processing the body head re-seed its row
    /// cleanly.
    #[test]
    fn test_chunk_producers_garbage_collected_on_undo_block() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        // Build a chain sharing one merkle tree so the header-only blocks validate on sync.
        let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        let mut blocks = vec![genesis];
        let mut block_merkle_tree = PartialMerkleTree::default();
        for i in 0..5 {
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

        // Process bodies for heights 1..=3 (body head becomes height 3).
        for block in &blocks[1..=3] {
            chain.process_block_test(block.clone()).unwrap();
        }
        // Sync only the headers for heights 4..=5 (header_head advances past the body head).
        chain
            .sync_block_headers(blocks[4..=5].iter().map(|b| b.header().clone().into()).collect())
            .unwrap();

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(blocks[1].hash()).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

        let row = |chain: &Chain, anchor: &CryptoHash, shard_id| -> Option<ValidatorStake> {
            chain
                .chain_store()
                .store()
                .get_ser(DBCol::ChunkProducers, &get_block_shard_id(anchor, shard_id))
        };

        for anchor in [blocks[3].hash(), blocks[4].hash(), blocks[5].hash()] {
            for shard_id in shard_layout.shard_ids() {
                assert!(row(&chain, anchor, shard_id).is_some(), "row should exist for {anchor}");
            }
        }

        let mut store_update = chain.mut_chain_store().store_update();
        store_update.clear_head_block_data(epoch_manager.as_ref()).unwrap();
        store_update.commit().unwrap();

        // Body head (blocks[3]) row is deleted alongside its BlockInfo.
        for shard_id in shard_layout.shard_ids() {
            assert!(
                row(&chain, blocks[3].hash(), shard_id).is_none(),
                "undo-block must delete ChunkProducers for the body head {}",
                blocks[3].hash()
            );
        }
        // Header-only anchors keep their BlockInfo, so their ChunkProducers rows must stay
        // paired (retained).
        for anchor in [blocks[4].hash(), blocks[5].hash()] {
            for shard_id in shard_layout.shard_ids() {
                assert!(
                    row(&chain, anchor, shard_id).is_some(),
                    "undo-block must retain ChunkProducers for header-only anchor {anchor}"
                );
            }
        }

        // Re-syncing the headers must keep the header-only rows valid (cache-independent: the
        // rows were never deleted). Do not assert the body head re-seeds in-process:
        // record_block_info_impl gates on has_block_info, which reads the epoch-manager
        // blocks_info LRU cache before the store; clear_head_block_data deletes BlockInfo only
        // from the store, so has_block_info(body_head) may still be true and the seeder is
        // skipped. tools/undo-block runs a fresh EpochManager (empty cache) and re-seeds
        // correctly.
        chain
            .sync_block_headers(blocks[3..=5].iter().map(|b| b.header().clone().into()).collect())
            .unwrap();
        for anchor in [blocks[4].hash(), blocks[5].hash()] {
            for shard_id in shard_layout.shard_ids() {
                assert!(
                    row(&chain, anchor, shard_id).is_some(),
                    "header re-sync must keep ChunkProducers for header-only anchor {anchor}"
                );
            }
        }
    }

    /// Normal canonical GC deletes ChunkProducers rows for header-only hashes (fork
    /// siblings, synced-ahead headers never given a body). Those rows are seeded per header
    /// but are not covered by clear_block_data (which only clears block-body hashes); the
    /// clear_chunk_data_and_headers sweep must delete them before HeaderHashesByHeight is
    /// dropped, else they orphan permanently once the height index is gone.
    #[test]
    fn test_chunk_producers_garbage_collected_for_header_only_fork() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

        let mut blocks = vec![chain.get_block(&chain.genesis().hash().clone()).unwrap()];
        for i in 0..6 {
            let prev = blocks[i].clone();
            clock.advance(Duration::milliseconds(1));
            let block =
                TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer.clone()).build();
            blocks.push(block.clone());
            chain.process_block_test(block).unwrap();
        }

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(blocks[1].hash()).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_id = shard_layout.shard_ids().next().unwrap();

        // Header-only fork sibling at height 2, built on canonical block 1 with a distinct clock
        // tick so it has a different hash than canonical block 2. No body is ever processed.
        clock.advance(Duration::milliseconds(1));
        let fork = TestBlockBuilder::from_prev_block(clock.clock(), &blocks[1], signer).build();
        let fork_hash = *fork.hash();
        assert_eq!(fork.header().height(), 2, "fork sibling must be at height 2");
        assert_ne!(fork_hash, *blocks[2].hash(), "fork must differ from canonical block 2");

        // Mirror what header sync writes: the header (adds fork_hash to HeaderHashesByHeight[2])
        // plus a ChunkProducers row keyed by (fork_hash, shard_id).
        let mut store_update = chain.mut_chain_store().store_update();
        store_update.save_block_header(fork.header().clone()).unwrap();
        store_update.commit().unwrap();
        let sentinel = ValidatorStake::new(
            "fork-cp".parse().unwrap(),
            PublicKey::empty(KeyType::ED25519),
            Balance::from_yoctonear(1),
        );
        let mut store_update = chain.chain_store().store().store_update();
        store_update.insert_ser(
            DBCol::ChunkProducers,
            &get_block_shard_id(&fork_hash, shard_id),
            &sentinel,
        );
        store_update.commit();

        let fork_row = |chain: &Chain| -> Option<ValidatorStake> {
            chain
                .chain_store()
                .store()
                .get_ser(DBCol::ChunkProducers, &get_block_shard_id(&fork_hash, shard_id))
        };
        assert!(fork_row(&chain).is_some(), "fork's row should exist before the sweep");

        // Drive the header sweep directly over heights chunk_tail(0)..4 (exclusive), covering
        // height 2. In TestBlockBuilder chains min_chunk_height stays 0 (blocks reuse the genesis
        // chunk), so a full clear_data never advances the sweep; calling it directly is the
        // deterministic way to exercise the header-only deletion.
        let mut store_update = chain.mut_chain_store().store_update();
        store_update.clear_chunk_data_and_headers(4).unwrap();
        store_update.commit().unwrap();

        assert!(
            fork_row(&chain).is_none(),
            "header-only fork's ChunkProducers row must be deleted by the header sweep"
        );
    }

    /// Resolution errors on a missing anchor DB row under EarlyKickout (strict read).
    #[test]
    fn test_resolution_errors_on_anchor_db_miss() {
        init_test_logger();
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        clock.advance(Duration::milliseconds(3444));
        let (mut chain, epoch_manager, _, signer) = setup(clock.clock());

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

        for shard_id in shard_layout.shard_ids() {
            assert!(
                epoch_manager
                    .get_chunk_producer_info_from_prev_block(&parent_hash, shard_id)
                    .is_ok(),
                "should succeed when DB is populated, shard {shard_id}"
            );
        }

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

    /// An unprocessed anchor surfaces as MissingBlock (node two or more blocks behind).
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

/// With EarlyKickout disabled (stable), resolution must match the legacy ChunkProductionKey computation.
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
