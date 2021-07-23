mod tests {
    use std::sync::Arc;

    use near_chain::store_validator::validate;
    use near_chain::store_validator::validate::StoreValidatorError;
    use near_chain::store_validator::StoreValidator;
    use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
    use near_chain_configs::GenesisConfig;
    use near_primitives::hash::CryptoHash;
    use near_store::test_utils::create_test_store;
    use near_store::DBCol;
    use testlib::chain_test_utils::KeyValueRuntime;

    fn init() -> (Chain, StoreValidator) {
        let store = create_test_store();
        let chain_genesis = ChainGenesis::test();
        let runtime_adapter = Arc::new(KeyValueRuntime::new(store.clone()));
        let mut genesis = GenesisConfig::default();
        genesis.genesis_height = 0;
        let chain =
            Chain::new(runtime_adapter.clone(), &chain_genesis, DoomslugThresholdMode::NoApprovals)
                .unwrap();
        (chain, StoreValidator::new(None, genesis.clone(), runtime_adapter, store))
    }

    #[test]
    fn test_io_error() {
        let (mut chain, mut sv) = init();
        let mut store_update = chain.store().owned_store().store_update();
        assert!(sv.validate_col(DBCol::ColBlock).is_ok());
        store_update
            .set_ser::<Vec<u8>>(
                DBCol::ColBlock,
                chain.get_block_by_height(0).unwrap().hash().as_ref(),
                &vec![123],
            )
            .unwrap();
        store_update.commit().unwrap();
        match sv.validate_col(DBCol::ColBlock) {
            Err(StoreValidatorError::IOError(_)) => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn test_db_corruption() {
        let (chain, mut sv) = init();
        let mut store_update = chain.store().owned_store().store_update();
        assert!(sv.validate_col(DBCol::ColTrieChanges).is_ok());
        store_update.set_ser::<Vec<u8>>(DBCol::ColTrieChanges, "567".as_ref(), &vec![123]).unwrap();
        store_update.commit().unwrap();
        match sv.validate_col(DBCol::ColTrieChanges) {
            Err(StoreValidatorError::DBCorruption(_)) => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn test_db_not_found() {
        let (mut chain, mut sv) = init();
        let block = chain.get_block_by_height(0).unwrap();
        assert!(validate::block_header_exists(&mut sv, &block.hash(), block).is_ok());
        match validate::block_header_exists(&mut sv, &CryptoHash::default(), block) {
            Err(StoreValidatorError::DBNotFound { .. }) => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn test_discrepancy() {
        let (mut chain, mut sv) = init();
        let block_header = chain.get_header_by_height(0).unwrap();
        assert!(validate::block_header_hash_validity(&mut sv, block_header.hash(), block_header)
            .is_ok());
        match validate::block_header_hash_validity(&mut sv, &CryptoHash::default(), block_header) {
            Err(StoreValidatorError::Discrepancy { .. }) => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn test_validation_failed() {
        let (_chain, mut sv) = init();
        assert!(validate::block_height_cmp_tail_final(&mut sv).is_ok());
        sv.inner.block_heights_less_tail.push(CryptoHash::default());
        assert!(validate::block_height_cmp_tail_final(&mut sv).is_ok());
        sv.inner.block_heights_less_tail.push(CryptoHash::default());
        match validate::block_height_cmp_tail_final(&mut sv) {
            Err(StoreValidatorError::ValidationFailed { .. }) => {}
            _ => assert!(false),
        }
    }
}
