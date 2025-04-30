/// Tests which check correctness of background flat storage creation.
use assert_matches::assert_matches;
use itertools::Itertools;
use near_async::time::Clock;
use near_chain_configs::Genesis;
use near_client::ProcessTxResponse;
use near_crypto::InMemorySigner;
use near_o11y::testonly::init_test_logger;
use near_primitives::errors::StorageError;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::transaction::SignedTransaction;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::ShardId;
use near_primitives_core::types::BlockHeight;
use near_store::adapter::StoreAdapter;
use near_store::test_utils::create_test_store;
use near_store::trie::AccessOptions;
use near_store::{KeyLookupMode, Store};

use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;

const START_HEIGHT: BlockHeight = 7;

/// Setup environment with one Near client for testing.
fn setup_env(genesis: &Genesis, store: Store) -> TestEnv {
    TestEnv::builder(&genesis.config).stores(vec![store]).nightshade_runtimes(genesis).build()
}

/// Tests the flat storage iterator. Running on a chain with 3 shards, and couple blocks produced.
#[test]
fn test_flat_storage_iter() {
    init_test_logger();
    let boundary_accounts = vec!["test0".parse().unwrap(), "test1".parse().unwrap()];
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 0);

    let genesis = Genesis::from_accounts(
        Clock::real(),
        vec!["test0".parse().unwrap()],
        1,
        shard_layout.clone(),
    );

    let store = create_test_store().flat_store();

    let mut env = setup_env(&genesis, store.store());
    for height in 1..START_HEIGHT {
        env.produce_block(0, height);
    }

    let [s0, s1, s2] = shard_layout.shard_ids().collect_vec()[..] else {
        panic!("Expected 3 shards in the shard layout!");
    };

    for shard_index in 0..3 {
        let shard_id = shard_layout.get_shard_id(shard_index).unwrap();
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        let items: Vec<_> = store.iter(shard_uid).collect();

        if ![s0, s1, s2].contains(&shard_id) {
            panic!("Unexpected shard ID: {shard_id}");
        }

        if shard_id == s0 {
            // Two entries - one for 'near' system account, the other for the contract.
            // (with newer protocol: +1 for BandwidthSchedulerState)
            let expected = 3;
            assert_eq!(expected, items.len());
            assert_eq!(
                TrieKey::Account { account_id: "near".parse().unwrap() }.to_vec(),
                items[0].as_ref().unwrap().0.to_vec()
            );
        }
        if shard_id == s1 {
            // Two entries - one for account, the other for contract.
            // (with newer protocol: +1 for BandwidthSchedulerState)
            let expected = 3;
            assert_eq!(expected, items.len());
            assert_eq!(
                TrieKey::Account { account_id: "test0".parse().unwrap() }.to_vec(),
                items[0].as_ref().unwrap().0.to_vec()
            );
        }
        if shard_id == s2 {
            // Test1 account was not created yet - so no entries.
            // (with newer protocol: +1 for BandwidthSchedulerState)
            let expected = 1;
            assert_eq!(expected, items.len());
        }
    }
}

#[test]
/// Initializes flat storage, then creates a Trie to read the flat storage
/// exactly at the flat head block.
/// Add another block to the flat state, which moves flat head and makes the
/// state of the previous flat head inaccessible.
fn test_not_supported_block() {
    init_test_logger();
    let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let shard_layout = ShardLayout::single_shard();
    let shard_uid = shard_layout.shard_uids().next().unwrap();
    let store = create_test_store();

    let mut env = setup_env(&genesis, store);
    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let genesis_hash = *env.clients[0].chain.genesis().hash();

    // Produce blocks up to `START_HEIGHT`.
    for height in 1..START_HEIGHT {
        env.produce_block(0, height);
        let tx = SignedTransaction::send_money(
            height,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            1,
            genesis_hash,
        );
        assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    }

    let flat_head_height = START_HEIGHT - 4;
    // Trie key which must exist in the storage.
    let trie_key_bytes =
        near_primitives::trie_key::TrieKey::Account { account_id: "test0".parse().unwrap() }
            .to_vec();
    // Create trie, which includes creating chunk view, and get `ValueRef`s
    // for post state roots for blocks `START_HEIGHT - 3` and `START_HEIGHT - 2`.
    // After creating the first trie, produce block `START_HEIGHT` which moves flat storage
    // head 1 block further and invalidates it.
    let mut get_ref_results = vec![];
    for height in flat_head_height..START_HEIGHT - 1 {
        let block_hash = env.clients[0].chain.get_block_hash_by_height(height).unwrap();
        let state_root = *env.clients[0]
            .chain
            .get_chunk_extra(
                &block_hash,
                &ShardUId::from_shard_id_and_layout(ShardId::new(0), &shard_layout),
            )
            .unwrap()
            .state_root();

        let trie = env.clients[0]
            .runtime_adapter
            .get_trie_for_shard(shard_uid.shard_id(), &block_hash, state_root, true)
            .unwrap();
        if height == flat_head_height {
            env.produce_block(0, START_HEIGHT);
        }
        get_ref_results.push(trie.get_optimized_ref(
            &trie_key_bytes,
            KeyLookupMode::MemOrFlatOrTrie,
            AccessOptions::DEFAULT,
        ));
    }

    // The first result should be FlatStorageError, because we can't read from first chunk view anymore.
    // But the node must not panic as this is normal behavior.
    // Ideally it should be tested on chain level, but there is no easy way to
    // postpone applying chunks reliably.
    assert_matches!(get_ref_results[0], Err(StorageError::FlatStorageBlockNotSupported(_)));
    // For the second result chunk view is valid, so result is Ok.
    assert_matches!(get_ref_results[1], Ok(Some(_)));
}
