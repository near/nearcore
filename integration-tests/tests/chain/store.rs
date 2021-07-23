mod tests {
    use std::sync::Arc;

    use borsh::BorshSerialize;
    use cached::Cached;
    use strum::IntoEnumIterator;

    use near_crypto::KeyType;
    use near_primitives::block::{Block, Tip};
    #[cfg(feature = "expensive_tests")]
    use near_primitives::epoch_manager::block_info::BlockInfo;
    use near_primitives::errors::InvalidTxError;
    use near_primitives::hash::hash;
    use near_primitives::types::{BlockHeight, EpochId, GCCount, NumBlocks};
    use near_primitives::utils::index_to_bytes;
    use near_primitives::validator_signer::InMemoryValidatorSigner;
    use near_store::test_utils::create_test_store;
    use near_store::DBCol;
    #[cfg(feature = "expensive_tests")]
    use {crate::store_validator::StoreValidator, near_chain_configs::GenesisConfig};

    use near_chain::store::{ChainStoreAccess, GCMode};
    use near_chain::types::ChainGenesis;
    use near_chain::{Chain, DoomslugThresholdMode};
    use testlib::chain_test_utils::KeyValueRuntime;

    fn get_chain() -> Chain {
        get_chain_with_epoch_length(10)
    }

    fn get_chain_with_epoch_length(epoch_length: NumBlocks) -> Chain {
        let store = create_test_store();
        let chain_genesis = ChainGenesis::test();
        let validators = vec![vec!["test1"]];
        let runtime_adapter = Arc::new(KeyValueRuntime::new_with_validators(
            store.clone(),
            validators
                .into_iter()
                .map(|inner| inner.into_iter().map(Into::into).collect())
                .collect(),
            1,
            1,
            epoch_length,
        ));
        Chain::new(runtime_adapter, &chain_genesis, DoomslugThresholdMode::NoApprovals).unwrap()
    }

    #[test]
    fn test_tx_validity_long_fork() {
        let transaction_validity_period = 5;
        let mut chain = get_chain();
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let short_fork = vec![Block::empty_with_height(&genesis, 1, &*signer.clone())];
        let mut store_update = chain.mut_store().store_update();
        store_update.save_block_header(short_fork[0].header().clone()).unwrap();
        store_update.commit().unwrap();

        let short_fork_head = short_fork[0].header().clone();
        assert!(chain
            .mut_store()
            .check_transaction_validity_period(
                &short_fork_head,
                &genesis.hash(),
                transaction_validity_period
            )
            .is_ok());
        let mut long_fork = vec![];
        let mut prev_block = genesis.clone();
        for i in 1..(transaction_validity_period + 3) {
            let mut store_update = chain.mut_store().store_update();
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            store_update
                .update_height_if_not_challenged(block.header().height(), *block.hash())
                .unwrap();
            long_fork.push(block);
            store_update.commit().unwrap();
        }
        let valid_base_hash = long_fork[1].hash();
        let cur_header = &long_fork.last().unwrap().header();
        assert!(chain
            .mut_store()
            .check_transaction_validity_period(
                cur_header,
                &valid_base_hash,
                transaction_validity_period
            )
            .is_ok());
        let invalid_base_hash = long_fork[0].hash();
        assert_eq!(
            chain.mut_store().check_transaction_validity_period(
                cur_header,
                &invalid_base_hash,
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
    }

    #[test]
    fn test_tx_validity_normal_case() {
        let transaction_validity_period = 5;
        let mut chain = get_chain();
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let mut blocks = vec![];
        let mut prev_block = genesis.clone();
        for i in 1..(transaction_validity_period + 2) {
            let mut store_update = chain.mut_store().store_update();
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            store_update
                .update_height_if_not_challenged(block.header().height(), *block.hash())
                .unwrap();
            blocks.push(block);
            store_update.commit().unwrap();
        }
        let valid_base_hash = blocks[1].hash();
        let cur_header = &blocks.last().unwrap().header();
        assert!(chain
            .mut_store()
            .check_transaction_validity_period(
                cur_header,
                &valid_base_hash,
                transaction_validity_period
            )
            .is_ok());
        let new_block = Block::empty_with_height(
            &blocks.last().unwrap(),
            transaction_validity_period + 3,
            &*signer.clone(),
        );
        let mut store_update = chain.mut_store().store_update();
        store_update.save_block_header(new_block.header().clone()).unwrap();
        store_update
            .update_height_if_not_challenged(new_block.header().height(), *new_block.hash())
            .unwrap();
        store_update.commit().unwrap();
        assert_eq!(
            chain.mut_store().check_transaction_validity_period(
                &new_block.header(),
                &valid_base_hash,
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
    }

    #[test]
    fn test_tx_validity_off_by_one() {
        let transaction_validity_period = 5;
        let mut chain = get_chain();
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let mut short_fork = vec![];
        let mut prev_block = genesis.clone();
        for i in 1..(transaction_validity_period + 2) {
            let mut store_update = chain.mut_store().store_update();
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            short_fork.push(block);
            store_update.commit().unwrap();
        }

        let short_fork_head = short_fork.last().unwrap().header().clone();
        assert_eq!(
            chain.mut_store().check_transaction_validity_period(
                &short_fork_head,
                &genesis.hash(),
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
        let mut long_fork = vec![];
        let mut prev_block = genesis.clone();
        for i in 1..(transaction_validity_period * 5) {
            let mut store_update = chain.mut_store().store_update();
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            long_fork.push(block);
            store_update.commit().unwrap();
        }
        let long_fork_head = &long_fork.last().unwrap().header();
        assert_eq!(
            chain.mut_store().check_transaction_validity_period(
                long_fork_head,
                &genesis.hash(),
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
    }

    #[test]
    fn test_cache_invalidation() {
        let mut chain = get_chain();
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let block1 = Block::empty_with_height(&genesis, 1, &*signer.clone());
        let mut block2 = block1.clone();
        block2.mut_header().get_mut().inner_lite.epoch_id = EpochId(hash(&[1, 2, 3]));
        block2.mut_header().resign(&*signer);

        let mut store_update = chain.mut_store().store_update();
        store_update.chain_store_cache_update.height_to_hashes.insert(1, Some(hash(&[1])));
        store_update
            .chain_store_cache_update
            .blocks
            .insert(*block1.header().hash(), block1.clone());
        store_update.commit().unwrap();

        let block_hash = chain.mut_store().height.cache_get(&index_to_bytes(1)).cloned();
        let epoch_id_to_hash =
            chain.mut_store().block_hash_per_height.cache_get(&index_to_bytes(1)).cloned();

        let mut store_update = chain.mut_store().store_update();
        store_update.chain_store_cache_update.height_to_hashes.insert(1, Some(hash(&[2])));
        store_update
            .chain_store_cache_update
            .blocks
            .insert(*block2.header().hash(), block2.clone());
        store_update.commit().unwrap();

        let block_hash1 = chain.mut_store().height.cache_get(&index_to_bytes(1)).cloned();
        let epoch_id_to_hash1 =
            chain.mut_store().block_hash_per_height.cache_get(&index_to_bytes(1)).cloned();

        assert_ne!(block_hash, block_hash1);
        assert_ne!(epoch_id_to_hash, epoch_id_to_hash1);
    }

    /// Test that garbage collection works properly. The blocks behind gc head should be garbage
    /// collected while the blocks that are ahead of it should not.
    #[test]
    fn test_clear_old_data() {
        let mut chain = get_chain_with_epoch_length(1);
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let mut prev_block = genesis.clone();
        let mut blocks = vec![prev_block.clone()];
        for i in 1..15 {
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            blocks.push(block.clone());
            let mut store_update = chain.mut_store().store_update();
            store_update.save_block(block.clone());
            store_update.inc_block_refcount(block.header().prev_hash()).unwrap();
            store_update.save_head(&Tip::from_header(block.header())).unwrap();
            store_update.save_block_header(block.header().clone()).unwrap();
            store_update
                .chain_store_cache_update
                .height_to_hashes
                .insert(i, Some(*block.header().hash()));
            store_update.save_next_block_hash(&prev_block.hash(), *block.hash());
            store_update.commit().unwrap();

            prev_block = block.clone();
        }

        chain.epoch_length = 1;
        let trie = chain.runtime_adapter.get_tries();
        assert!(chain.clear_data(trie, 100).is_ok());

        // epoch didn't change so no data is garbage collected.
        for i in 0..15 {
            println!("height = {} hash = {}", i, blocks[i].hash());
            if i < 8 {
                assert!(chain.get_block(&blocks[i].hash()).is_err());
                assert!(chain
                    .mut_store()
                    .get_all_block_hashes_by_height(i as BlockHeight)
                    .is_err());
            } else {
                assert!(chain.get_block(&blocks[i].hash()).is_ok());
                assert!(chain.mut_store().get_all_block_hashes_by_height(i as BlockHeight).is_ok());
            }
        }

        let gced_cols = [
            DBCol::ColBlock,
            DBCol::ColOutgoingReceipts,
            DBCol::ColIncomingReceipts,
            DBCol::ColBlockInfo,
            DBCol::ColBlocksToCatchup,
            DBCol::ColChallengedBlocks,
            DBCol::ColStateDlInfos,
            DBCol::ColBlockExtra,
            DBCol::ColBlockPerHeight,
            DBCol::ColNextBlockHashes,
            DBCol::ColNextBlockWithNewChunk,
            DBCol::ColChunkPerHeightShard,
            DBCol::ColBlockRefCount,
            DBCol::ColOutcomeIds,
            DBCol::ColChunkExtra,
        ];
        for col in DBCol::iter() {
            println!("current column is {:?}", col);
            if gced_cols.contains(&col) {
                // only genesis block includes new chunk.
                let count = if col == DBCol::ColOutcomeIds { Some(1) } else { Some(8) };
                assert_eq!(
                    chain
                        .store()
                        .store
                        .get_ser::<GCCount>(
                            DBCol::ColGCCount,
                            &col.try_to_vec().expect("Failed to serialize DBCol")
                        )
                        .unwrap(),
                    count
                );
            } else {
                assert_eq!(
                    chain
                        .store()
                        .store
                        .get_ser::<GCCount>(
                            DBCol::ColGCCount,
                            &col.try_to_vec().expect("Failed to serialize DBCol")
                        )
                        .unwrap(),
                    None
                );
            }
        }
    }

    #[test]
    fn test_clear_old_data_fixed_height() {
        let mut chain = get_chain();
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let mut prev_block = genesis.clone();
        let mut blocks = vec![prev_block.clone()];
        for i in 1..10 {
            let mut store_update = chain.mut_store().store_update();

            let block = Block::empty_with_height(&prev_block, i, &*signer);
            blocks.push(block.clone());
            store_update.save_block(block.clone());
            store_update.inc_block_refcount(block.header().prev_hash()).unwrap();
            store_update.save_head(&Tip::from_header(&block.header())).unwrap();
            store_update.save_block_header(block.header().clone()).unwrap();
            store_update
                .chain_store_cache_update
                .height_to_hashes
                .insert(i, Some(*block.header().hash()));
            store_update.save_next_block_hash(&prev_block.hash(), *block.hash());
            store_update.commit().unwrap();

            prev_block = block.clone();
        }

        assert!(chain.get_block(&blocks[4].hash()).is_ok());
        assert!(chain.get_block(&blocks[5].hash()).is_ok());
        assert!(chain.get_block(&blocks[6].hash()).is_ok());
        assert!(chain.get_block_header(&blocks[5].hash()).is_ok());
        assert_eq!(
            chain
                .mut_store()
                .get_all_block_hashes_by_height(5)
                .unwrap()
                .values()
                .flatten()
                .collect::<Vec<_>>(),
            vec![blocks[5].hash()]
        );
        assert!(chain.mut_store().get_next_block_hash(&blocks[5].hash()).is_ok());

        let trie = chain.runtime_adapter.get_tries();
        let mut store_update = chain.mut_store().store_update();
        assert!(store_update.clear_block_data(*blocks[5].hash(), GCMode::Canonical(trie)).is_ok());
        store_update.commit().unwrap();

        assert!(chain.get_block(blocks[4].hash()).is_err());
        assert!(chain.get_block(blocks[5].hash()).is_ok());
        assert!(chain.get_block(blocks[6].hash()).is_ok());
        // block header should be available
        assert!(chain.get_block_header(blocks[4].hash()).is_ok());
        assert!(chain.get_block_header(blocks[5].hash()).is_ok());
        assert!(chain.get_block_header(blocks[6].hash()).is_ok());
        assert!(chain.mut_store().get_all_block_hashes_by_height(4).is_err());
        assert!(chain.mut_store().get_all_block_hashes_by_height(5).is_ok());
        assert!(chain.mut_store().get_all_block_hashes_by_height(6).is_ok());
        assert!(chain.mut_store().get_next_block_hash(blocks[4].hash()).is_err());
        assert!(chain.mut_store().get_next_block_hash(blocks[5].hash()).is_ok());
        assert!(chain.mut_store().get_next_block_hash(blocks[6].hash()).is_ok());
    }

    /// Test that `gc_blocks_limit` works properly
    #[cfg(feature = "expensive_tests")]
    #[test]
    fn test_clear_old_data_too_many_heights() {
        for i in 1..5 {
            println!("gc_blocks_limit == {:?}", i);
            test_clear_old_data_too_many_heights_common(i);
        }
        test_clear_old_data_too_many_heights_common(25);
        test_clear_old_data_too_many_heights_common(50);
        test_clear_old_data_too_many_heights_common(87);
    }

    #[cfg(feature = "expensive_tests")]
    fn test_clear_old_data_too_many_heights_common(gc_blocks_limit: NumBlocks) {
        let mut chain = get_chain_with_epoch_length(1);
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let mut prev_block = genesis.clone();
        let mut blocks = vec![prev_block.clone()];
        {
            let mut store_update = chain.mut_store().store_update().store().store_update();
            let block_info = BlockInfo::default();
            store_update
                .set_ser(DBCol::ColBlockInfo, genesis.hash().as_ref(), &block_info)
                .unwrap();
            store_update.commit().unwrap();
        }
        for i in 1..1000 {
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            blocks.push(block.clone());

            let mut store_update = chain.mut_store().store_update();
            store_update.save_block(block.clone());
            store_update.inc_block_refcount(block.header().prev_hash()).unwrap();
            store_update.save_head(&Tip::from_header(&block.header())).unwrap();
            store_update.save_block_header(block.header().clone()).unwrap();
            {
                let mut store_update = store_update.store().store_update();
                let block_info = BlockInfo::default();
                store_update
                    .set_ser(DBCol::ColBlockInfo, block.hash().as_ref(), &block_info)
                    .unwrap();
                store_update.commit().unwrap();
            }
            store_update
                .chain_store_cache_update
                .height_to_hashes
                .insert(i, Some(*block.header().hash()));
            store_update.save_next_block_hash(&prev_block.hash(), *block.hash());
            store_update.commit().unwrap();

            prev_block = block.clone();
        }

        let trie = chain.runtime_adapter.get_tries();

        for iter in 0..10 {
            println!("ITERATION #{:?}", iter);
            assert!(chain.clear_data(trie.clone(), gc_blocks_limit).is_ok());

            // epoch didn't change so no data is garbage collected.
            for i in 0..1000 {
                if i < (iter + 1) * gc_blocks_limit as usize {
                    assert!(chain.get_block(&blocks[i].hash()).is_err());
                    assert!(chain
                        .mut_store()
                        .get_all_block_hashes_by_height(i as BlockHeight)
                        .is_err());
                } else {
                    assert!(chain.get_block(&blocks[i].hash()).is_ok());
                    assert!(chain
                        .mut_store()
                        .get_all_block_hashes_by_height(i as BlockHeight)
                        .is_ok());
                }
            }
            let mut genesis = GenesisConfig::default();
            genesis.genesis_height = 0;
            let mut store_validator = StoreValidator::new(
                None,
                genesis.clone(),
                chain.runtime_adapter.clone(),
                chain.store().owned_store(),
            );
            store_validator.validate();
            println!("errors = {:?}", store_validator.errors);
            assert!(!store_validator.is_failed());
        }
    }
}
