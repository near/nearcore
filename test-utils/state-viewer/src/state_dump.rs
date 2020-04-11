use near_chain::RuntimeAdapter;
use near_chain_configs::{Genesis, GenesisConfig};
use near_primitives::block::BlockHeader;
use near_primitives::state_record::StateRecord;
use near_primitives::types::{AccountInfo, StateRoot};
use near_store::TrieIterator;
use neard::NightshadeRuntime;
use std::collections::HashMap;

pub fn state_dump(
    runtime: NightshadeRuntime,
    state_roots: Vec<StateRoot>,
    last_block_header: BlockHeader,
    genesis_config: &GenesisConfig,
) -> Genesis {
    println!("Generating genesis from state data");
    let genesis_height = last_block_header.inner_lite.height + 1;
    let block_producers = runtime
        .get_epoch_block_producers_ordered(
            &last_block_header.inner_lite.epoch_id,
            &last_block_header.hash,
        )
        .unwrap();
    let validators = block_producers
        .into_iter()
        .filter_map(|(info, is_slashed)| {
            if !is_slashed {
                Some((info.account_id, (info.public_key, info.stake)))
            } else {
                None
            }
        })
        .collect::<HashMap<_, _>>();

    let mut records = vec![];
    for state_root in &state_roots {
        let trie = TrieIterator::new(&runtime.trie, &state_root).unwrap();
        for item in trie {
            let (key, value) = item.unwrap();
            if let Some(mut sr) = StateRecord::from_raw_key_value(key, value) {
                if let StateRecord::Account { account_id, account } = &mut sr {
                    if account.locked > 0 {
                        if let Some((_, stake)) = validators.get(account_id) {
                            account.amount = account.amount + account.locked - *stake;
                            account.locked = *stake;
                        } else {
                            account.amount += account.locked;
                            account.locked = 0;
                        }
                    }
                }
                records.push(sr);
            }
        }
    }

    let mut genesis_config = genesis_config.clone();
    genesis_config.genesis_height = genesis_height;
    genesis_config.validators = validators
        .into_iter()
        .map(|(account_id, (public_key, amount))| AccountInfo { account_id, public_key, amount })
        .collect();
    Genesis::new(genesis_config, records.into())
}

#[cfg(test)]
mod test {
    use crate::state_dump::state_dump;
    use near_chain::{ChainGenesis, RuntimeAdapter};
    use near_chain_configs::Genesis;
    use near_client::test_utils::TestEnv;
    use near_crypto::{InMemorySigner, KeyType};
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::types::NumBlocks;
    use near_store::test_utils::create_test_store;
    use near_store::Store;
    use neard::config::GenesisExt;
    use neard::config::TESTING_INIT_STAKE;
    use neard::genesis_validate::validate_genesis;
    use neard::NightshadeRuntime;
    use std::collections::HashSet;
    use std::iter::FromIterator;
    use std::path::Path;
    use std::sync::Arc;

    fn setup(epoch_length: NumBlocks) -> (Arc<Store>, Genesis, TestEnv) {
        let mut genesis = Genesis::test(vec!["test0", "test1"], 1);
        genesis.config.num_block_producer_seats = 2;
        genesis.config.num_block_producer_seats_per_shard = vec![2];
        genesis.config.epoch_length = epoch_length;
        let store = create_test_store();
        let nightshade_runtime = NightshadeRuntime::new(
            Path::new("."),
            store.clone(),
            Arc::new(genesis.clone()),
            vec![],
            vec![],
        );
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(nightshade_runtime)];
        let mut chain_genesis = ChainGenesis::test();
        chain_genesis.epoch_length = epoch_length;
        chain_genesis.gas_limit = genesis.config.gas_limit;
        let env = TestEnv::new_with_runtime(chain_genesis, 1, 2, runtimes);
        (store, genesis, env)
    }

    /// Test that we preserve the validators from the epoch of the state dump.
    #[test]
    fn test_dump_state_preserve_validators() {
        let epoch_length = 4;
        let (store, genesis, mut env) = setup(epoch_length);
        let genesis_hash = env.clients[0].chain.genesis().hash();
        let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
        let tx = SignedTransaction::stake(
            1,
            "test1".to_string(),
            &signer,
            TESTING_INIT_STAKE,
            signer.public_key.clone(),
            genesis_hash,
        );
        env.clients[0].process_tx(tx, false);
        for i in 1..=epoch_length * 2 + 1 {
            env.produce_block(0, i);
        }
        let head = env.clients[0].chain.head().unwrap();
        let last_block_hash = head.last_block_hash;
        let cur_epoch_id = head.epoch_id;
        let block_producers = env.clients[0]
            .runtime_adapter
            .get_epoch_block_producers_ordered(&cur_epoch_id, &last_block_hash)
            .unwrap();
        assert_eq!(
            block_producers.into_iter().map(|(r, _)| r.account_id).collect::<HashSet<_>>(),
            HashSet::from_iter(vec!["test0".to_string(), "test1".to_string()])
        );
        let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap().clone();
        let mut state_roots = vec![];
        for chunk in last_block.chunks.iter() {
            state_roots.push(chunk.inner.prev_state_root.clone());
        }
        let runtime = NightshadeRuntime::new(
            Path::new("."),
            store.clone(),
            Arc::new(genesis.clone()),
            vec![],
            vec![],
        );
        let new_genesis =
            state_dump(runtime, state_roots, last_block.header.clone(), &genesis.config);
        assert_eq!(new_genesis.config.validators.len(), 2);
        validate_genesis(&new_genesis);
    }

    /// Test that we return locked tokens for accounts that are not validators.
    #[test]
    fn test_dump_state_return_locked() {
        let epoch_length = 4;
        let (store, genesis, mut env) = setup(epoch_length);
        let genesis_hash = env.clients[0].chain.genesis().hash();
        let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
        let tx = SignedTransaction::stake(
            1,
            "test1".to_string(),
            &signer,
            TESTING_INIT_STAKE,
            signer.public_key.clone(),
            genesis_hash,
        );
        env.clients[0].process_tx(tx, false);
        for i in 1..=epoch_length + 1 {
            env.produce_block(0, i);
        }
        let head = env.clients[0].chain.head().unwrap();
        let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap().clone();
        let mut state_roots = vec![];
        for chunk in last_block.chunks.iter() {
            state_roots.push(chunk.inner.prev_state_root.clone());
        }
        let runtime = NightshadeRuntime::new(
            Path::new("."),
            store.clone(),
            Arc::new(genesis.clone()),
            vec![],
            vec![],
        );
        let new_genesis =
            state_dump(runtime, state_roots, last_block.header.clone(), &genesis.config);
        assert_eq!(
            new_genesis
                .config
                .validators
                .clone()
                .into_iter()
                .map(|r| r.account_id)
                .collect::<Vec<_>>(),
            vec!["test0".to_string()]
        );
        validate_genesis(&new_genesis);
    }
}
