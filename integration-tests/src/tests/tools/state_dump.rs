use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;
use near_chain::{ChainStoreAccess, Provenance};
use near_chain_configs::genesis_validate::validate_genesis;
use near_chain_configs::test_utils::TESTING_INIT_STAKE;
use near_chain_configs::{Genesis, GenesisChangeConfig, MutableConfigValue};
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyFile, KeyType, PublicKey, SecretKey};
use near_epoch_manager::EpochManager;
use near_o11y::testonly::init_test_logger;
use near_primitives::account::id::AccountId;
use near_primitives::hash::CryptoHash;
use near_primitives::state_record::StateRecord;
use near_primitives::transaction::{Action, DeployContractAction, SignedTransaction};
use near_primitives::types::{Balance, BlockHeight, BlockHeightDelta, NumBlocks, ProtocolVersion};
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives_core::account::id::AccountIdRef;
use near_state_viewer::state_dump;
use near_store::Store;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use nearcore::NightshadeRuntime;
use nearcore::config::{Config, NearConfig};

fn setup(
    epoch_length: NumBlocks,
    protocol_version: ProtocolVersion,
    test_resharding: bool,
) -> (Store, Genesis, TestEnv, NearConfig) {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.num_block_producer_seats = 2;
    genesis.config.num_block_producer_seats_per_shard = vec![2];
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = protocol_version;
    genesis.config.use_production_config = test_resharding;

    let env = if test_resharding {
        TestEnv::builder(&genesis.config)
            .validator_seats(2)
            .use_state_snapshots()
            .real_stores()
            .nightshade_runtimes(&genesis)
            .build()
    } else {
        TestEnv::builder(&genesis.config).validator_seats(2).nightshade_runtimes(&genesis).build()
    };

    let near_config = NearConfig::new(
        Config::default(),
        genesis.clone(),
        KeyFile {
            account_id: "test".parse().unwrap(),
            public_key: PublicKey::empty(KeyType::ED25519),
            secret_key: SecretKey::from_random(KeyType::ED25519),
        },
        MutableConfigValue::new(
            Some(Arc::new(InMemoryValidatorSigner::from_random(
                "test".parse().unwrap(),
                KeyType::ED25519,
            ))),
            "validator_signer",
        ),
    )
    .unwrap();

    let store = env.clients[0].chain.chain_store().store();
    (store, genesis, env, near_config)
}

/// Produces blocks, avoiding the potential failure where the client is not the
/// block producer for each subsequent height (this can happen when a new validator
/// is staked since they will also have heights where they should produce the block instead).
fn safe_produce_blocks(
    env: &mut TestEnv,
    initial_height: BlockHeight,
    num_blocks: BlockHeightDelta,
) {
    let mut h = initial_height;
    for _ in 1..=num_blocks {
        let mut block = None;
        // `env.clients[0]` may not be the block producer at `h`,
        // loop until we find a height env.clients[0] should produce.
        while block.is_none() {
            block = env.clients[0].produce_block(h).unwrap();
            h += 1;
        }
        env.process_block(0, block.unwrap(), Provenance::PRODUCED);
    }
}

/// Test that we preserve the validators from the epoch of the state dump.
#[test]
fn test_dump_state_preserve_validators() {
    let epoch_length = 4;
    let (store, genesis, mut env, near_config) = setup(epoch_length, PROTOCOL_VERSION, false);
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::stake(
        1,
        "test1".parse().unwrap(),
        &signer,
        TESTING_INIT_STAKE,
        signer.public_key(),
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

    safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1);

    let head = env.clients[0].chain.head().unwrap();
    let cur_epoch_id = head.epoch_id;
    let block_producers =
        env.clients[0].epoch_manager.get_epoch_block_producers_ordered(&cur_epoch_id).unwrap();
    assert_eq!(
        block_producers.into_iter().map(|r| r.take_account_id()).collect::<HashSet<_>>(),
        HashSet::from_iter(vec!["test0".parse().unwrap(), "test1".parse().unwrap()])
    );
    let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
    let state_roots: Vec<CryptoHash> =
        last_block.chunks().iter_deprecated().map(|chunk| chunk.prev_state_root()).collect();
    initialize_genesis_state(store.clone(), &genesis, None);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime =
        NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());
    let records_file = tempfile::NamedTempFile::new().unwrap();
    let new_near_config = state_dump(
        epoch_manager.as_ref(),
        runtime,
        &state_roots,
        last_block.header().clone(),
        &near_config,
        Some(records_file.path()),
        &GenesisChangeConfig::default(),
    );
    let new_genesis = new_near_config.genesis;
    assert_eq!(new_genesis.config.validators.len(), 2);
    validate_genesis(&new_genesis).unwrap();
}

/// Test that we respect the specified account ID list in dump_state.
#[test]
fn test_dump_state_respect_select_account_ids() {
    let epoch_length = 4;
    let (store, genesis, mut env, near_config) = setup(epoch_length, PROTOCOL_VERSION, false);
    let genesis_hash = *env.clients[0].chain.genesis().hash();

    let signer0 = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let tx00 = SignedTransaction::from_actions(
        1,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer0,
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::backwards_compatible_rs_contract().to_vec(),
        })],
        genesis_hash,
        0,
    );
    let tx01 = SignedTransaction::stake(
        1,
        "test0".parse().unwrap(),
        &signer0,
        TESTING_INIT_STAKE,
        signer0.public_key(),
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx00, false, false), ProcessTxResponse::ValidTx);
    assert_eq!(env.rpc_handlers[0].process_tx(tx01, false, false), ProcessTxResponse::ValidTx);

    let signer1 = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx1 = SignedTransaction::stake(
        1,
        "test1".parse().unwrap(),
        &signer1,
        TESTING_INIT_STAKE,
        signer1.public_key(),
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx1, false, false), ProcessTxResponse::ValidTx);

    safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1);

    let head = env.clients[0].chain.head().unwrap();
    let cur_epoch_id = head.epoch_id;
    let block_producers =
        env.clients[0].epoch_manager.get_epoch_block_producers_ordered(&cur_epoch_id).unwrap();
    assert_eq!(
        block_producers.into_iter().map(|r| r.take_account_id()).collect::<HashSet<_>>(),
        HashSet::from_iter(vec!["test0".parse().unwrap(), "test1".parse().unwrap()]),
    );
    let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
    let state_roots: Vec<CryptoHash> =
        last_block.chunks().iter_deprecated().map(|chunk| chunk.prev_state_root()).collect();
    initialize_genesis_state(store.clone(), &genesis, None);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime =
        NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());
    let select_account_ids = vec!["test0".parse().unwrap()];
    let new_near_config = state_dump(
        epoch_manager.as_ref(),
        runtime,
        &state_roots,
        last_block.header().clone(),
        &near_config,
        None,
        &GenesisChangeConfig::default().with_select_account_ids(Some(select_account_ids.clone())),
    );
    let new_genesis = new_near_config.genesis;
    let mut expected_accounts: HashSet<AccountId> =
        new_genesis.config.validators.iter().map(|v| v.account_id.clone()).collect();
    expected_accounts.extend(select_account_ids);
    expected_accounts.insert(new_genesis.config.protocol_treasury_account.clone());
    let mut actual_accounts: HashSet<AccountId> = HashSet::new();
    new_genesis.for_each_record(|record| {
        if let StateRecord::Account { account_id, .. } = record {
            actual_accounts.insert(account_id.clone());
        }
    });
    assert_eq!(expected_accounts, actual_accounts);
    validate_genesis(&new_genesis).unwrap();
}

/// Test that we preserve the validators from the epoch of the state dump.
#[test]
fn test_dump_state_preserve_validators_in_memory() {
    let epoch_length = 4;
    let (store, genesis, mut env, near_config) = setup(epoch_length, PROTOCOL_VERSION, false);
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::stake(
        1,
        "test1".parse().unwrap(),
        &signer,
        TESTING_INIT_STAKE,
        signer.public_key(),
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

    safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1);

    let head = env.clients[0].chain.head().unwrap();
    let cur_epoch_id = head.epoch_id;
    let block_producers =
        env.clients[0].epoch_manager.get_epoch_block_producers_ordered(&cur_epoch_id).unwrap();
    assert_eq!(
        block_producers.into_iter().map(|r| r.take_account_id()).collect::<HashSet<_>>(),
        HashSet::from_iter(vec!["test0".parse().unwrap(), "test1".parse().unwrap()])
    );
    let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
    let state_roots: Vec<CryptoHash> =
        last_block.chunks().iter_deprecated().map(|chunk| chunk.prev_state_root()).collect();
    initialize_genesis_state(store.clone(), &genesis, None);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime =
        NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());
    let new_near_config = state_dump(
        epoch_manager.as_ref(),
        runtime,
        &state_roots,
        last_block.header().clone(),
        &near_config,
        None,
        &GenesisChangeConfig::default(),
    );
    let new_genesis = new_near_config.genesis;
    assert_eq!(new_genesis.config.validators.len(), 2);
    validate_genesis(&new_genesis).unwrap();
}

/// Test that we return locked tokens for accounts that are not validators.
#[test]
fn test_dump_state_return_locked() {
    let epoch_length = 4;
    let (store, genesis, mut env, near_config) = setup(epoch_length, PROTOCOL_VERSION, false);
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::stake(
        1,
        "test1".parse().unwrap(),
        &signer,
        TESTING_INIT_STAKE,
        signer.public_key(),
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    for i in 1..=epoch_length + 1 {
        env.produce_block(0, i);
    }

    let head = env.clients[0].chain.head().unwrap();
    let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
    let state_roots: Vec<CryptoHash> =
        last_block.chunks().iter_deprecated().map(|chunk| chunk.prev_state_root()).collect();
    initialize_genesis_state(store.clone(), &genesis, None);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime =
        NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());

    let records_file = tempfile::NamedTempFile::new().unwrap();
    let new_near_config = state_dump(
        epoch_manager.as_ref(),
        runtime,
        &state_roots,
        last_block.header().clone(),
        &near_config,
        Some(records_file.path()),
        &GenesisChangeConfig::default(),
    );
    let new_genesis = new_near_config.genesis;
    assert_eq!(
        new_genesis.config.validators.clone().into_iter().map(|r| r.account_id).collect::<Vec<_>>(),
        vec!["test0"]
    );
    validate_genesis(&new_genesis).unwrap();
}

// TODO(congestion_control) - integration with resharding
#[ignore]
#[test]
fn test_dump_state_shard_upgrade() {
    use near_client::test_utils::run_catchup;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::version::ProtocolFeature::SimpleNightshade;

    let epoch_length = 4;
    let (store, genesis, mut env, near_config) =
        setup(epoch_length, SimpleNightshade.protocol_version() - 1, true);
    for i in 1..=2 * epoch_length + 1 {
        let mut block = env.clients[0].produce_block(i).unwrap().unwrap();
        block.mut_header().set_latest_protocol_version(SimpleNightshade.protocol_version());
        env.process_block(0, block, Provenance::PRODUCED);
        run_catchup(&mut env.clients[0], &[]).unwrap();
    }
    let head = env.clients[0].chain.head().unwrap();
    assert_eq!(
        env.clients[0].epoch_manager.get_shard_layout(&head.epoch_id).unwrap(),
        ShardLayout::get_simple_nightshade_layout(),
    );
    let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();

    let state_roots: Vec<CryptoHash> =
        last_block.chunks().iter_deprecated().map(|chunk| chunk.prev_state_root()).collect();
    initialize_genesis_state(store.clone(), &genesis, None);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime =
        NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());
    let records_file = tempfile::NamedTempFile::new().unwrap();
    let new_near_config = state_dump(
        epoch_manager.as_ref(),
        runtime,
        &state_roots,
        last_block.header().clone(),
        &near_config,
        Some(records_file.path()),
        &GenesisChangeConfig::default(),
    );
    let new_genesis = new_near_config.genesis;

    assert_eq!(new_genesis.config.shard_layout, ShardLayout::get_simple_nightshade_layout());
    assert_eq!(new_genesis.config.num_block_producer_seats_per_shard, vec![2; 4]);
    assert_eq!(new_genesis.config.avg_hidden_validator_seats_per_shard, vec![0; 4]);
}

/// If the node does not track a shard, state dump will not give the correct result.
#[test]
#[should_panic(expected = "MissingTrieValue")]
fn test_dump_state_not_track_shard() {
    let epoch_length = 4;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.num_block_producer_seats = 2;
    genesis.config.num_block_producer_seats_per_shard = vec![2];
    genesis.config.epoch_length = epoch_length;
    let store1 = create_test_store();
    let store2 = create_test_store();
    initialize_genesis_state(store1.clone(), &genesis, None);
    initialize_genesis_state(store2.clone(), &genesis, None);
    let epoch_manager1 = EpochManager::new_arc_handle(store1.clone(), &genesis.config, None);
    let epoch_manager2 = EpochManager::new_arc_handle(store2.clone(), &genesis.config, None);
    let runtime1 = NightshadeRuntime::test(
        Path::new("."),
        store1.clone(),
        &genesis.config,
        epoch_manager1.clone(),
    );
    let runtime2 = NightshadeRuntime::test(
        Path::new("."),
        store2.clone(),
        &genesis.config,
        epoch_manager2.clone(),
    );
    let mut env = TestEnv::builder(&genesis.config)
        .clients_count(2)
        .stores(vec![store1, store2])
        .epoch_managers(vec![epoch_manager1, epoch_manager2.clone()])
        .runtimes(vec![runtime1, runtime2.clone()])
        .build();
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::send_money(
        1,
        "test1".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        1,
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

    let mut blocks = vec![];
    for i in 1..epoch_length {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        for j in 0..2 {
            let provenance = if j == 0 { Provenance::PRODUCED } else { Provenance::NONE };
            env.process_block(j, block.clone(), provenance);
        }
        blocks.push(block);
    }

    let near_config = NearConfig::new(
        Config::default(),
        genesis,
        KeyFile {
            account_id: "test".parse().unwrap(),
            public_key: PublicKey::empty(KeyType::ED25519),
            secret_key: SecretKey::from_random(KeyType::ED25519),
        },
        MutableConfigValue::new(
            Some(Arc::new(InMemoryValidatorSigner::from_random(
                "test".parse().unwrap(),
                KeyType::ED25519,
            ))),
            "validator_signer",
        ),
    )
    .unwrap();

    let last_block = blocks.pop().unwrap();
    let state_roots = last_block
        .chunks()
        .iter_deprecated()
        .map(|chunk| chunk.prev_state_root())
        .collect::<Vec<_>>();

    let records_file = tempfile::NamedTempFile::new().unwrap();
    let _ = state_dump(
        epoch_manager2.as_ref(),
        runtime2,
        &state_roots,
        last_block.header().clone(),
        &near_config,
        Some(records_file.path()),
        &GenesisChangeConfig::default(),
    );
}

#[test]
fn test_dump_state_with_delayed_receipt() {
    init_test_logger();

    let epoch_length = 4;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.num_block_producer_seats = 2;
    genesis.config.num_block_producer_seats_per_shard = vec![2];
    genesis.config.epoch_length = epoch_length;
    let store = create_test_store();
    initialize_genesis_state(store.clone(), &genesis, None);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let nightshade_runtime = NightshadeRuntime::test(
        Path::new("."),
        store.clone(),
        &genesis.config,
        epoch_manager.clone(),
    );
    let mut env = TestEnv::builder(&genesis.config)
        .validator_seats(2)
        .stores(vec![store.clone()])
        .epoch_managers(vec![epoch_manager])
        .runtimes(vec![nightshade_runtime])
        .build();
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::stake(
        1,
        "test1".parse().unwrap(),
        &signer,
        TESTING_INIT_STAKE,
        signer.public_key(),
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

    safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1);

    let near_config = NearConfig::new(
        Config::default(),
        genesis.clone(),
        KeyFile {
            account_id: "test".parse().unwrap(),
            public_key: PublicKey::empty(KeyType::ED25519),
            secret_key: SecretKey::from_random(KeyType::ED25519),
        },
        MutableConfigValue::new(
            Some(Arc::new(InMemoryValidatorSigner::from_random(
                "test".parse().unwrap(),
                KeyType::ED25519,
            ))),
            "validator_signer",
        ),
    )
    .unwrap();
    let head = env.clients[0].chain.head().unwrap();
    let cur_epoch_id = head.epoch_id;
    let block_producers =
        env.clients[0].epoch_manager.get_epoch_block_producers_ordered(&cur_epoch_id).unwrap();
    assert_eq!(
        block_producers.into_iter().map(|r| r.take_account_id()).collect::<HashSet<_>>(),
        HashSet::from_iter(vec!["test0".parse().unwrap(), "test1".parse().unwrap()])
    );
    let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
    let state_roots: Vec<CryptoHash> =
        last_block.chunks().iter_deprecated().map(|chunk| chunk.prev_state_root()).collect();
    initialize_genesis_state(store.clone(), &genesis, None);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime =
        NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());
    let records_file = tempfile::NamedTempFile::new().unwrap();
    let new_near_config = state_dump(
        epoch_manager.as_ref(),
        runtime,
        &state_roots,
        last_block.header().clone(),
        &near_config,
        Some(records_file.path()),
        &GenesisChangeConfig::default(),
    );
    let new_genesis = new_near_config.genesis;

    assert_eq!(new_genesis.config.validators.len(), 2);
    validate_genesis(&new_genesis).unwrap();
}

#[test]
fn test_dump_state_respect_select_whitelist_validators() {
    let epoch_length = 4;
    let (store, genesis, mut env, near_config) = setup(epoch_length, PROTOCOL_VERSION, false);

    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::stake(
        1,
        "test1".parse().unwrap(),
        &signer,
        TESTING_INIT_STAKE,
        signer.public_key(),
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

    safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1);

    let head = env.clients[0].chain.head().unwrap();
    let cur_epoch_id = head.epoch_id;
    let block_producers =
        env.clients[0].epoch_manager.get_epoch_block_producers_ordered(&cur_epoch_id).unwrap();
    assert_eq!(
        block_producers.into_iter().map(|r| r.take_account_id()).collect::<HashSet<_>>(),
        HashSet::from_iter(vec!["test0".parse().unwrap(), "test1".parse().unwrap()]),
    );

    let whitelist_validators =
        vec!["test1".parse().unwrap(), "non_validator_account".parse().unwrap()];

    let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
    let state_roots: Vec<CryptoHash> =
        last_block.chunks().iter_deprecated().map(|chunk| chunk.prev_state_root()).collect();
    initialize_genesis_state(store.clone(), &genesis, None);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime =
        NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());
    let new_near_config = state_dump(
        epoch_manager.as_ref(),
        runtime,
        &state_roots,
        last_block.header().clone(),
        &near_config,
        None,
        &GenesisChangeConfig::default().with_whitelist_validators(Some(whitelist_validators)),
    );
    let new_genesis = new_near_config.genesis;

    assert_eq!(
        new_genesis
            .config
            .validators
            .iter()
            .map(|x| x.account_id.clone())
            .collect::<Vec<AccountId>>(),
        vec!["test1"]
    );

    let mut stake = HashMap::<AccountId, Balance>::new();
    new_genesis.for_each_record(|record| {
        if let StateRecord::Account { account_id, account } = record {
            stake.insert(account_id.clone(), account.locked());
        }
    });

    assert_eq!(
        stake.get(AccountIdRef::new_or_panic("test0")).unwrap_or(&(0 as Balance)),
        &(0 as Balance)
    );

    validate_genesis(&new_genesis).unwrap();
}
