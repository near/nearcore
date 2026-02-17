use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::task::Poll;

use itertools::Itertools;
use near_async::messaging::{CanSend as _, Handler as _};
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain::spice_core::get_last_certified_block_header;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::{GetBlock, ProcessTxRequest, Query, QueryError};
use near_client_primitives::types::GetBlockError;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, BlockId, BlockReference, Finality, ShardId};
use near_primitives::utils::get_block_shard_id_rev;
use near_primitives::views::QueryRequest;
use near_store::DBCol;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use parking_lot::Mutex;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{
    create_account_id, create_validators_spec, validators_spec_clients,
    validators_spec_clients_with_rpc,
};
use crate::utils::get_node_data;
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::{TransactionRunner, get_anchor_hash};

use super::spice_utils::delay_endorsements_propagation;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_chain() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let accounts: Vec<AccountId> =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect_vec();

    let num_block_producers = 4;
    let num_validators = 5;

    let block_and_chunk_producers =
        accounts.iter().take(num_block_producers).cloned().collect_vec();
    let validators_only: Vec<AccountId> =
        (0..num_validators).map(|i| format!("validator{i}").parse().unwrap()).collect_vec();

    let clients =
        block_and_chunk_producers.iter().cloned().chain(validators_only.clone()).collect_vec();

    let epoch_length = 10;
    // With each block producer tracking more than one shard we are more likely to find bugs of
    // chunks being processed inconsistently.
    // More than one shard in total allows testing cross-shard communications.
    let shard_layout =
        ShardLayout::multi_shard_custom(vec![accounts[accounts.len() / 2].clone()], 1);
    let validators_spec = ValidatorsSpec::desired_roles(
        &block_and_chunk_producers.iter().map(|a| a.as_str()).collect_vec(),
        &validators_only.iter().map(|a| a.as_str()).collect_vec(),
    );

    const INITIAL_BALANCE: Balance = Balance::from_near(1_000_000);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout.clone())
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_BALANCE)
        .genesis_height(10000)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let TestLoopEnv { mut test_loop, node_datas, shared_state } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    let client_handles =
        node_datas.iter().map(|data| data.client_sender.actor_handle()).collect_vec();

    // TODO(spice): Should be able to use execute_money_transfers here eventually. It shouldn't reach into
    // runtime directly though, or do it correctly with separated execution and chunks.

    let client = &test_loop.data.get(&client_handles[0]).client;
    let epoch_manager = client.epoch_manager.clone();

    let get_balance = |test_loop_data: &mut TestLoopData, account: &AccountId, epoch_id| {
        let shard_id = shard_layout.account_id_to_shard_id(account);
        let cp =
            &epoch_manager.get_epoch_chunk_producers_for_shard(&epoch_id, shard_id).unwrap()[0];
        let node = TestLoopNode::from(get_node_data(&node_datas, cp));
        node.view_account_query(test_loop_data, account).unwrap().amount
    };

    let epoch_id = client.chain.head().unwrap().epoch_id;
    for account in &accounts {
        let got_balance = get_balance(&mut test_loop.data, account, epoch_id);
        assert_eq!(got_balance, INITIAL_BALANCE);
    }

    let (sent_txs, balance_changes) = schedule_send_money_txs(&node_datas, &accounts, &test_loop);

    let mut observed_txs = HashSet::new();
    test_loop.run_until(
        |test_loop_data| {
            let clients = client_handles
                .iter()
                .map(|handle| &test_loop_data.get(&handle).client)
                .collect_vec();

            let head = clients[0].chain.final_head().unwrap();
            let block = clients[0].chain.get_block(&head.last_block_hash).unwrap();

            let height = head.height;
            let chunk_mask = block.header().chunk_mask();
            assert_eq!(chunk_mask, vec![true; chunk_mask.len()]);

            for chunk in block.chunks().iter() {
                for client in &clients {
                    let Ok(chunk) = client.chain.get_chunk(&chunk.chunk_hash()) else {
                        continue;
                    };
                    for tx in chunk.to_transactions() {
                        observed_txs.insert(tx.get_hash());
                    }
                    assert_eq!(chunk.prev_outgoing_receipts(), vec![]);
                }
            }

            height > 10030
        },
        Duration::seconds(35),
    );

    assert_eq!(*sent_txs.lock(), observed_txs);

    let client = &test_loop.data.get(&client_handles[0]).client;
    let epoch_id = client.chain.head().unwrap().epoch_id;

    assert!(!balance_changes.is_empty());
    for (account, balance_change) in &balance_changes {
        let got_balance = get_balance(&mut test_loop.data, account, epoch_id);
        let want_balance = Balance::from_yoctonear(
            (INITIAL_BALANCE.as_yoctonear() as i128 + balance_change).try_into().unwrap(),
        );
        assert_eq!(got_balance, want_balance);
        assert_ne!(*balance_change, 0);
    }

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_chain_with_delayed_execution() {
    init_test_logger();

    let sender = create_account_id("sender");
    let receiver = create_account_id("receiver");

    let num_producers = 2;
    let num_validators = 0;
    let validators_spec = create_validators_spec(num_producers, num_validators);
    let clients = validators_spec_clients(&validators_spec);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .add_user_account_simple(sender.clone(), Balance::from_near(10))
        .add_user_account_simple(receiver.clone(), Balance::from_near(0))
        .build();

    let producer_account = clients[0].clone();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build();

    let execution_delay = 4;
    // We delay endorsements to simulate slow execution validation causing execution to lag behind.
    delay_endorsements_propagation(&mut env, execution_delay);

    env = env.warmup();

    let node = TestLoopNode::for_account(&env.node_datas, &producer_account);
    let tx = SignedTransaction::send_money(
        1,
        sender.clone(),
        receiver.clone(),
        &create_user_test_signer(&sender),
        Balance::from_near(1),
        node.head(env.test_loop_data()).last_block_hash,
    );
    node.run_tx(&mut env.test_loop, tx, Duration::seconds(10));

    let view_account_result = node.view_account_query(&env.test_loop.data, &receiver).unwrap();
    assert_eq!(view_account_result.amount, Balance::from_near(1));

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Sets up a spice env with delayed endorsements so execution lags behind
/// consensus, and runs until there is a gap of at least 3 blocks.
fn setup_spice_env_with_execution_delay() -> (TestLoopEnv, TestLoopNode<'static>) {
    let num_producers = 2;
    let num_validators = 0;
    let validators_spec = create_validators_spec(num_producers, num_validators);
    let clients = validators_spec_clients(&validators_spec);

    let genesis = TestLoopBuilder::new_genesis_builder().validators_spec(validators_spec).build();

    let producer_account = clients[0].clone();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build();

    let execution_delay = 4;
    delay_endorsements_propagation(&mut env, execution_delay);

    let mut env = env.warmup();

    let node = TestLoopNode::for_account(&env.node_datas, &producer_account).into_owned();

    env.test_loop.run_until(
        |test_loop_data| {
            let head = node.head(test_loop_data);
            let execution_head = node.last_executed(test_loop_data);
            head.height > execution_head.height + 2
        },
        Duration::seconds(20),
    );

    (env, node)
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_rpc_get_block_by_finality() {
    init_test_logger();
    let (mut env, node) = setup_spice_env_with_execution_delay();

    let test_loop_data = env.test_loop_data();
    let execution_head = node.last_executed(test_loop_data);
    let final_head = node.client(test_loop_data).chain.final_head().unwrap();

    let view_client = env.test_loop.data.get_mut(&node.data().view_client_sender.actor_handle());
    let block_none =
        view_client.handle(GetBlock(BlockReference::Finality(Finality::None))).unwrap();
    assert_eq!(block_none.header.height, execution_head.height);
    let block_final =
        view_client.handle(GetBlock(BlockReference::Finality(Finality::Final))).unwrap();
    assert!(block_final.header.height <= execution_head.height);
    assert!(block_final.header.height <= final_head.height);
    let block_doomslug =
        view_client.handle(GetBlock(BlockReference::Finality(Finality::DoomSlug))).unwrap();
    assert!(block_doomslug.header.height <= execution_head.height);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_rpc_unknown_block_past_execution_head() {
    init_test_logger();
    let (mut env, node) = setup_spice_env_with_execution_delay();

    let test_loop_data = env.test_loop_data();
    let execution_head = node.last_executed(test_loop_data);
    let consensus_head = node.head(test_loop_data);
    assert!(consensus_head.height > execution_head.height + 1);

    let view_client = env.test_loop.data.get_mut(&node.data().view_client_sender.actor_handle());

    // Query by height within execution head: should succeed
    let result = view_client
        .handle(GetBlock(BlockReference::BlockId(BlockId::Height(execution_head.height))));
    assert!(result.is_ok(), "block at execution_head height should be found");

    // Query by height past execution head: should return UnknownBlock
    let result = view_client
        .handle(GetBlock(BlockReference::BlockId(BlockId::Height(consensus_head.height))));
    assert!(
        matches!(result, Err(GetBlockError::UnknownBlock { .. })),
        "block past execution_head should be unknown, got: {result:?}"
    );

    // Query by hash of the consensus head (not yet executed): should return UnknownBlock
    let result = view_client
        .handle(GetBlock(BlockReference::BlockId(BlockId::Hash(consensus_head.last_block_hash))));
    assert!(
        matches!(result, Err(GetBlockError::UnknownBlock { .. })),
        "block at consensus_head hash should be unknown, got: {result:?}"
    );

    // Query by finality None: should succeed and return execution head
    let block_none =
        view_client.handle(GetBlock(BlockReference::Finality(Finality::None))).unwrap();
    assert_eq!(block_none.header.height, execution_head.height);

    // Query handle_query by height past execution head: should return UnknownBlock
    let query_result = view_client.handle(Query::new(
        BlockReference::BlockId(BlockId::Height(consensus_head.height)),
        QueryRequest::ViewAccount { account_id: "account0".parse().unwrap() },
    ));
    assert!(
        matches!(query_result, Err(QueryError::UnknownBlock { .. })),
        "query past execution_head should be unknown block, got: {query_result:?}"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_garbage_collection() {
    init_test_logger();

    let num_producers = 2;
    let num_validators = 0;
    let validators_spec = create_validators_spec(num_producers, num_validators);
    let clients = validators_spec_clients_with_rpc(&validators_spec);

    let epoch_length = 5;
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .epoch_length(epoch_length)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .gc_num_epochs_to_keep(1)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let node = TestLoopNode::rpc(&env.node_datas);
    env.test_loop.run_until(
        // We want to make sure that gc runs at least once and it doesn't trigger any asserts.
        |test_loop_data| node.tail(test_loop_data) >= epoch_length,
        Duration::seconds(20),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

// TODO(spice-resharding): Add a test for witness GC during resharding.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_garbage_collection_witnesses() {
    init_test_logger();

    let num_producers = 2;
    let num_validators = 0;
    let validators_spec = create_validators_spec(num_producers, num_validators);
    let clients = validators_spec_clients_with_rpc(&validators_spec);

    let epoch_length = 5;
    let shard_layout = ShardLayout::multi_shard(2, 0);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .shard_layout(shard_layout.clone())
        .epoch_length(epoch_length)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .gc_num_epochs_to_keep(1)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build();

    // We delay endorsements to simulate slow execution validation causing execution to lag behind.
    let execution_delay = 4;
    delay_endorsements_propagation(&mut env, execution_delay);
    env = env.warmup();

    // Use a chunk producer node (not RPC) since only chunk producers store witnesses.
    let producer_node = TestLoopNode::from(env.node_datas[0].clone());
    env.test_loop.run_until(
        |test_loop_data| {
            let chain_store = &producer_node.client(test_loop_data).chain.chain_store;
            let final_head = chain_store.final_head().unwrap();
            get_last_certified_block_header(chain_store, &final_head.last_block_hash)
                .map_or(0, |header| header.height())
                >= 10
        },
        Duration::seconds(20),
    );
    let shard_tracker = &producer_node.client(env.test_loop_data()).shard_tracker;
    let tracked_shards: Vec<_> = shard_layout
        .shard_ids()
        // This gets tracked shards for genesis, but it should not change during the test.
        .filter(|shard_id| shard_tracker.cares_about_shard(&CryptoHash::default(), *shard_id))
        .collect();
    let chain_store = &producer_node.client(env.test_loop_data()).chain.chain_store;
    assert_witness_gc_invariant(chain_store, &tracked_shards);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Verifies witness GC invariant: witness exists iff block is uncertified.
///
/// From the perspective of final_head:
/// - Certified blocks (height <= last_certified_height): witness should NOT exist
/// - Uncertified blocks (height > last_certified_height): witness SHOULD exist
fn assert_witness_gc_invariant(chain_store: &ChainStoreAdapter, tracked_shards: &[ShardId]) {
    let final_head = chain_store.final_head().unwrap();
    let last_certified_height =
        get_last_certified_block_header(chain_store, &final_head.last_block_hash).unwrap().height();
    let execution_head = chain_store.spice_execution_head().unwrap();
    let store = chain_store.store().store();

    // Verify that old witnesses are cleared
    for (key, _) in store.iter(DBCol::witnesses()) {
        let (block_hash, shard_id) = get_block_shard_id_rev(&key).unwrap();
        let block_height = chain_store.get_block_height(&block_hash).unwrap();
        assert!(
            // Note we allow 1 block difference here since GC is async.
            block_height > last_certified_height - 1,
            "witness at height {block_height} shard {shard_id} should have been GC'd (last_certified_height = {last_certified_height})"
        );
    }

    // Verify that recent witnesses for uncertified blocks exist
    for height in (last_certified_height + 1)..=execution_head.height {
        let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
        for &shard_id in tracked_shards {
            let key = near_primitives::utils::get_block_shard_id(&block_hash, shard_id);
            assert!(
                store.get(DBCol::witnesses(), &key).is_some(),
                "witness at height {height} shard {shard_id} should exist"
            );
        }
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_restart_rpc_node() {
    init_test_logger();

    let num_producers = 1;
    let num_validators = 0;
    let validators_spec = create_validators_spec(num_producers, num_validators);
    let clients = validators_spec_clients_with_rpc(&validators_spec);

    let sender = create_account_id("sender");
    let receiver = create_account_id("receiver");

    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .add_user_account_simple(sender.clone(), Balance::from_near(10))
        .add_user_account_simple(receiver.clone(), Balance::from_near(0))
        .build();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let rpc = TestLoopNode::rpc(&env.node_datas).into_owned();
    let node = TestLoopNode::from(env.node_datas[0].clone());
    assert_ne!(rpc.data().account_id, node.data().account_id);

    rpc.run_until_head_height(&mut env.test_loop, 5);
    let killed_rpc_state = env.kill_node(&rpc.data().identifier);

    let tx = SignedTransaction::send_money(
        1,
        sender.clone(),
        receiver.clone(),
        &create_user_test_signer(&sender),
        Balance::from_near(1),
        node.head(env.test_loop_data()).last_block_hash,
    );
    node.run_tx(&mut env.test_loop, tx, Duration::seconds(20));

    let new_rpc_identifier = format!("{}-restart", rpc.data().identifier);
    env.restart_node(&new_rpc_identifier, killed_rpc_state);
    // After restart new node_datas are created.
    let rpc = TestLoopNode::rpc(&env.node_datas);

    assert_ne!(rpc.head(env.test_loop_data()).height, node.head(env.test_loop_data()).height);

    rpc.run_for_number_of_blocks(&mut env.test_loop, 5);

    let view_account_result = rpc.view_account_query(&env.test_loop.data, &receiver).unwrap();
    assert_eq!(view_account_result.amount, Balance::from_near(1));

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_restart_producer_node() {
    init_test_logger();

    let num_producers = 4;
    let num_validators = 0;
    let validators_spec = create_validators_spec(num_producers, num_validators);
    let clients = validators_spec_clients(&validators_spec);

    let sender = create_account_id("sender");
    let receiver = create_account_id("receiver");
    let shard_layout = ShardLayout::multi_shard_custom(vec![create_account_id("zzzz")], 3);
    // Keeping both sender and receiver in the same shards makes it easier to send and track
    // transaction on the correct node.
    assert_eq!(
        shard_layout.account_id_to_shard_id(&sender),
        shard_layout.account_id_to_shard_id(&receiver)
    );

    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .shard_layout(shard_layout.clone())
        .add_user_account_simple(sender.clone(), Balance::from_near(10))
        .add_user_account_simple(receiver.clone(), Balance::from_near(0))
        .build();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build();

    let execution_delay = 2;
    // Delay is required to make sure that new blocks processing doesn't trigger requests for
    // missing data.
    delay_endorsements_propagation(&mut env, execution_delay);

    let mut env = env.warmup();

    let node = TestLoopNode::from(env.node_datas[0].clone());
    let client = node.client(env.test_loop_data());
    let epoch_id = client.chain.head().unwrap().epoch_id;
    let shard_id = shard_layout.account_id_to_shard_id(&receiver);
    let epoch_manager = client.epoch_manager.clone();
    let producers = epoch_manager.get_epoch_chunk_producers_for_shard(&epoch_id, shard_id).unwrap();

    let restart_node = TestLoopNode::for_account(&env.node_datas, &producers[0]).into_owned();
    let stable_node = TestLoopNode::for_account(&env.node_datas, &producers[1]).into_owned();

    restart_node.run_until_head_height(&mut env.test_loop, 5);
    let killed_node_state = env.kill_node(&restart_node.data().identifier);

    let tx = SignedTransaction::send_money(
        1,
        sender.clone(),
        receiver.clone(),
        &create_user_test_signer(&sender),
        Balance::from_near(1),
        stable_node.head(env.test_loop_data()).last_block_hash,
    );
    stable_node.run_tx(&mut env.test_loop, tx, Duration::seconds(20));

    let new_node_identifier = format!("{}-restart", restart_node.data().identifier);
    env.restart_node(&new_node_identifier, killed_node_state);
    // After restart new node_datas are created.
    let restart_node = TestLoopNode::for_account(&env.node_datas, &restart_node.data().account_id);

    assert_ne!(
        restart_node.head(env.test_loop_data()).height,
        stable_node.head(env.test_loop_data()).height
    );

    restart_node.run_for_number_of_blocks(&mut env.test_loop, 10);

    let view_account_result =
        restart_node.view_account_query(&env.test_loop.data, &receiver).unwrap();
    assert_eq!(view_account_result.amount, Balance::from_near(1));

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_restart_validator_node() {
    init_test_logger();

    let num_producers = 1;
    let num_validators = 1;
    let validators_spec = create_validators_spec(num_producers, num_validators);
    let clients = validators_spec_clients(&validators_spec);

    let sender = create_account_id("sender");
    let receiver = create_account_id("receiver");

    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .add_user_account_simple(sender.clone(), Balance::from_near(10))
        .add_user_account_simple(receiver.clone(), Balance::from_near(0))
        .build();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build();

    let execution_delay = 2;
    // Delay is required to make sure that new blocks processing doesn't trigger requests of
    // missing data.
    delay_endorsements_propagation(&mut env, execution_delay);

    let mut env = env.warmup();

    let node = TestLoopNode::from(env.node_datas[0].clone());
    let client = node.client(env.test_loop_data());
    let epoch_id = client.chain.head().unwrap().epoch_id;
    let epoch_manager = client.epoch_manager.clone();
    let producer = epoch_manager
        .get_epoch_chunk_producers(&epoch_id)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .take_account_id();

    let producer_node = TestLoopNode::for_account(&env.node_datas, &producer).into_owned();
    let validator_node = {
        let node_datas = env
            .node_datas
            .iter()
            .filter(|data| data.account_id != producer_node.data().account_id)
            .next()
            .unwrap();
        TestLoopNode::from(node_datas.clone())
    };

    producer_node.run_until_head_height(&mut env.test_loop, 5);
    let killed_node_state = env.kill_node(&validator_node.data().identifier);

    let tx = SignedTransaction::send_money(
        1,
        sender.clone(),
        receiver.clone(),
        &create_user_test_signer(&sender),
        Balance::from_near(1),
        validator_node.head(env.test_loop_data()).last_block_hash,
    );
    let tx_processor_sender = &producer_node.data().rpc_handler_sender;
    let retry_when_congested = false;
    let mut tx_runner = TransactionRunner::new(tx, retry_when_congested);
    let future_spawner = env.test_loop.future_spawner("TransactionRunner");

    let start_height = producer_node.head(env.test_loop_data()).height;
    env.test_loop.run_until(
        |test_loop_data| {
            let client = producer_node.client(test_loop_data);
            if let Poll::Ready(tx_res) =
                tx_runner.poll(&tx_processor_sender, client, &future_spawner)
            {
                panic!("Transaction executed without validator in place, result: {tx_res:?}");
            }
            producer_node.head(test_loop_data).height > start_height + 5
        },
        Duration::seconds(5),
    );

    let new_node_identifier = format!("{}-restart", producer_node.data().identifier);
    env.restart_node(&new_node_identifier, killed_node_state);
    // After restart new node_datas are created.
    let validator_node =
        TestLoopNode::for_account(&env.node_datas, &validator_node.data().account_id);

    assert_ne!(
        producer_node.head(env.test_loop_data()).height,
        validator_node.head(env.test_loop_data()).height
    );

    let view_account_result =
        producer_node.view_account_query(&env.test_loop.data, &receiver).unwrap();
    assert_eq!(view_account_result.amount, Balance::from_near(0));

    producer_node.run_for_number_of_blocks(&mut env.test_loop, 10);

    let view_account_result =
        producer_node.view_account_query(&env.test_loop.data, &receiver).unwrap();
    assert_eq!(view_account_result.amount, Balance::from_near(1));

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[cfg(feature = "test_features")]
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_chain_with_missing_chunks() {
    use crate::utils::account::validators_spec_clients_with_rpc;

    init_test_logger();
    let accounts: Vec<AccountId> =
        (0..100).map(|i| create_account_id(&format!("account{}", i))).collect_vec();

    // With 2 shards and 4 producers we should still be able to include all transactions
    // when one of the producers starts missing chunks.
    let num_producers = 4;
    let num_validators = 0;
    let boundary_account = accounts[accounts.len() / 2].clone();
    let shard_layout = ShardLayout::multi_shard_custom(vec![boundary_account], 1);

    let validators_spec = create_validators_spec(num_producers, num_validators);
    let clients = validators_spec_clients_with_rpc(&validators_spec);

    const INITIAL_BALANCE: Balance = Balance::from_near(1_000_000);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_BALANCE)
        .build();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let rpc_node = TestLoopNode::rpc(&env.node_datas);
    let client = rpc_node.client(env.test_loop_data());
    let epoch_manager = client.epoch_manager.clone();
    let node_with_missing_chunks = epoch_manager
        .get_epoch_chunk_producers(client.chain.get_head_block().unwrap().header().epoch_id())
        .unwrap()
        .swap_remove(0)
        .take_account_id();
    let node_with_missing_chunks =
        TestLoopNode::for_account(&env.node_datas, &node_with_missing_chunks);
    node_with_missing_chunks.send_adversarial_message(
        &mut env.test_loop,
        near_client::NetworkAdversarialMessage::AdvProduceChunks(
            near_client::client_actor::AdvProduceChunksMode::StopProduce,
        ),
    );

    for account in &accounts {
        let got_balance = rpc_node.view_account_query(&env.test_loop.data, account).unwrap().amount;
        assert_eq!(got_balance, INITIAL_BALANCE);
    }

    // We cannot use rpc_node to schedule transactions since it may direct transactions to
    // producer with missing chunks.
    let node_data = env
        .node_datas
        .iter()
        .find(|node_data| node_data.account_id != node_with_missing_chunks.data().account_id)
        .unwrap()
        .clone();
    let (sent_txs, balance_changes) =
        schedule_send_money_txs(&[node_data], &accounts, &env.test_loop);

    let mut observed_txs = HashSet::new();
    env.test_loop.run_until(
        |test_loop_data| {
            let head = rpc_node.head(test_loop_data);
            let client = rpc_node.client(test_loop_data);
            let block = client.chain.get_block(&head.last_block_hash).unwrap();
            for chunk in block.chunks().iter() {
                let Ok(chunk) = client.chain.get_chunk(&chunk.chunk_hash()) else {
                    continue;
                };
                for tx in chunk.to_transactions() {
                    observed_txs.insert(tx.get_hash());
                }
            }
            observed_txs.len() == sent_txs.lock().len()
        },
        Duration::seconds(200),
    );

    // A few more blocks to make sure everything is executed.
    let head_height = rpc_node.head(env.test_loop_data()).height;
    rpc_node.run_until_head_height(&mut env.test_loop, head_height + 3);

    let client = rpc_node.client(env.test_loop_data());
    let mut block =
        client.chain.get_block(&client.chain.final_head().unwrap().last_block_hash).unwrap();
    let mut missed_execution = false;
    let mut have_missing_chunks = false;
    while !block.header().is_genesis() {
        for chunk in block.chunks().iter_raw() {
            if !chunk.is_new_chunk(block.header().height()) {
                have_missing_chunks = true;
            }
        }

        if !client.chain.spice_core_reader.all_execution_results_exist(block.header()).unwrap() {
            let execution_results = client
                .chain
                .spice_core_reader
                .get_execution_results_by_shard_id(block.header())
                .unwrap();
            missed_execution = true;
            println!(
                "not all execution result for block at height: {}; execution_results: {:?}",
                block.header().height(),
                execution_results
            );
        }
        block = client.chain.get_block(block.header().prev_hash()).unwrap();
    }

    assert!(!missed_execution, "some of the blocks are missing execution results");
    assert!(have_missing_chunks);

    assert!(!balance_changes.is_empty());
    for (account, balance_change) in &balance_changes {
        let got_balance = rpc_node.view_account_query(&env.test_loop.data, account).unwrap().amount;
        let want_balance = Balance::from_yoctonear(
            (INITIAL_BALANCE.as_yoctonear() as i128 + balance_change).try_into().unwrap(),
        );
        assert_eq!(got_balance, want_balance);
        assert_ne!(*balance_change, 0);
    }
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn schedule_send_money_txs(
    node_datas: &[crate::setup::state::NodeExecutionData],
    accounts: &[AccountId],
    test_loop: &near_async::test_loop::TestLoopV2,
) -> (Arc<Mutex<HashSet<near_primitives::hash::CryptoHash>>>, HashMap<AccountId, i128>) {
    let sent_txs = Arc::new(Mutex::new(HashSet::new()));
    let mut balance_changes = HashMap::new();
    let node_data = Arc::new(node_datas.to_vec());
    for (i, sender) in accounts.iter().cloned().enumerate() {
        let amount = Balance::from_near(1).checked_mul((i + 1).try_into().unwrap()).unwrap();
        let receiver = accounts[(i + 1) % accounts.len()].clone();
        *balance_changes.entry(sender.clone()).or_default() -= amount.as_yoctonear() as i128;
        *balance_changes.entry(receiver.clone()).or_default() += amount.as_yoctonear() as i128;

        let node_data = node_data.clone();
        let sent_txs = sent_txs.clone();
        test_loop.send_adhoc_event_with_delay(
            format!("transaction {}", i),
            Duration::milliseconds(100 * i as i64),
            move |data| {
                let clients = node_data
                    .iter()
                    .map(|test_data| &data.get(&test_data.client_sender.actor_handle()).client)
                    .collect_vec();

                let anchor_hash = get_anchor_hash(&clients);

                let tx = SignedTransaction::send_money(
                    1,
                    sender.clone(),
                    receiver.clone(),
                    &create_user_test_signer(&sender),
                    amount,
                    anchor_hash,
                );
                sent_txs.lock().insert(tx.get_hash());
                let process_tx_request =
                    ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };
                node_data[i % clients.len()].rpc_handler_sender.send(process_tx_request);
            },
        );
    }
    (sent_txs, balance_changes)
}
