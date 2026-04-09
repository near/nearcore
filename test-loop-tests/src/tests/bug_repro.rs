use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{
    create_account_ids, create_validators_spec, validators_spec_clients_with_rpc,
};
use itertools::Itertools as _;
use near_async::messaging::CanSend as _;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::ProcessTxRequest;
use near_network::types::PeerMessage;
use near_network::{T2MessageBody, TieredMessageBody};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance};
use parking_lot::RwLock;
use std::sync::Arc;

#[test]
fn slow_test_repro_1183() {
    init_test_logger();

    let block_producers = ["test1", "test2", "test3", "test4"];
    let validators_spec = ValidatorsSpec::desired_roles(&block_producers, &[]);
    let num_shards = 4;
    let shard_layout = ShardLayout::multi_shard(num_shards, 3);
    let epoch_length = 5;

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .build();

    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .minimum_validators_per_shard(2)
        .build_store_for_genesis_protocol_version();

    let clients = block_producers.into_iter().map(|a| a.parse().unwrap()).collect_vec();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build();

    // TODO: convert override handler to transport filter

    let client_actor_handle = &env.node_datas[1].client_sender.actor_handle();
    env.test_loop.run_until(
        |test_loop_data| {
            let client = &test_loop_data.get(client_actor_handle).client;
            let head = client.chain.head().unwrap();
            head.height >= 25
        },
        Duration::seconds(60),
    );
}

#[test]
#[ignore = "entire test logic in override handler — needs rewrite"]
// Was also: #[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_sync_from_archival_node() {
    init_test_logger();

    let block_producers = ["test1", "test2", "test3", "test4"];
    let validators_spec = ValidatorsSpec::desired_roles(&block_producers, &[]);
    let num_shards = 4;
    let shard_layout = ShardLayout::multi_shard(num_shards, 3);
    let epoch_length = 4;
    let block_prod_time = Duration::milliseconds(100);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);

    let clients = block_producers.into_iter().map(|a| a.parse().unwrap()).collect_vec();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .cold_storage_archival_clients(vec![clients[0].clone()])
        .config_modifier(move |config, idx| {
            config.min_block_production_delay = block_prod_time;
            config.max_block_production_delay = 3 * block_prod_time;
            config.max_block_wait_delay = 3 * block_prod_time;
            // Archival node
            if idx == 0 {
                config.tracked_shards_config = TrackedShardsConfig::AllShards;
            }
        })
        .build();

    let largest_height = Arc::new(RwLock::new(0u64));

    // TODO: convert override handler to transport filter
    env.test_loop.run_until(|_| *largest_height.read() >= 50, Duration::seconds(20));
}

#[test]
fn slow_test_long_gap_between_blocks() {
    init_test_logger();

    let block_producers = ["test1", "test2"];
    let validators_spec = ValidatorsSpec::desired_roles(&block_producers, &[]);
    let num_shards = 2;
    let shard_layout = ShardLayout::multi_shard(num_shards, 3);
    let epoch_length = 1000;
    let target_height = 600;
    let block_prod_time = Duration::milliseconds(100);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);

    let clients = block_producers.into_iter().map(|a| a.parse().unwrap()).collect_vec();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .config_modifier(move |config, _| {
            config.min_block_production_delay = block_prod_time;
            config.max_block_production_delay = 3 * block_prod_time;
            config.max_block_wait_delay = 3 * block_prod_time;
        })
        .build();

    // TODO: convert override handler to transport filter

    let client_actor_handle = &env.node_datas[1].client_sender.actor_handle();
    env.test_loop.run_until(
        |test_loop_data| {
            let client = &test_loop_data.get(client_actor_handle).client;
            let head = client.chain.final_head().unwrap();
            head.height > target_height
        },
        Duration::seconds(3 * 70),
    );
}

/// 1 RPC node, 1 validator node, 1 shard
/// Submit the same transaction twice, it should get forwarded to the validator node twice.
/// Should work both when the RPC node has a `validator_signer` and when it doesn't.
/// Reproduces an issue where the RPC didn't forward retried transactions when
/// the `validator_signer` was set. (See https://github.com/near/nearcore/pull/14958)
#[test]
fn test_rpc_forwards_retried_transaction() {
    init_test_logger();

    let shard_layout = ShardLayout::single_shard();
    let user_accounts = create_account_ids(["account0"]);
    let initial_balance = Balance::from_near(1_000_000);
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&user_accounts, initial_balance)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build();
    let rpc_data_idx = env.rpc_data_idx();

    // First test the case where `validator_signer` is set.
    assert!(env.rpc_node().client().validator_signer.get().is_some());

    // Record ForwardTx messages sent by the RPC node
    let forward_tx_requests = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let rpc_peer_id = env.node_datas[rpc_data_idx].peer_id.clone();
    {
        let forward_tx_requests_clone = forward_tx_requests.clone();
        env.shared_state.network_shared_state.register_message_filter(move |from, _to, msg| {
            if from == &rpc_peer_id {
                if let PeerMessage::Routed(routed_msg) = msg {
                    if let TieredMessageBody::T2(body) = routed_msg.body() {
                        if let T2MessageBody::ForwardTx(tx) = body.as_ref() {
                            forward_tx_requests_clone
                                .lock()
                                .push(("validator0".parse::<AccountId>().unwrap(), tx.get_hash()));
                        }
                    }
                }
            }
            Some(msg.clone())
        });
    }

    // Submit tx1 twice
    let tx1 = SignedTransaction::send_money(
        1,
        user_accounts[0].clone(),
        user_accounts[0].clone(),
        &create_user_test_signer(&user_accounts[0]),
        Balance::from_near(1),
        env.rpc_node().head().last_block_hash,
    );
    env.node_datas[rpc_data_idx].rpc_handler_sender.send(ProcessTxRequest {
        transaction: tx1.clone(),
        is_forwarded: false,
        check_only: false,
    });
    env.node_datas[rpc_data_idx].rpc_handler_sender.send(ProcessTxRequest {
        transaction: tx1.clone(),
        is_forwarded: false,
        check_only: false,
    });

    // Run TestLoop to process the transaction requests
    env.rpc_runner().run_for_number_of_blocks(1);

    // There should be two ForwardTx(validator0, tx1) messages recorded.
    let validator_acc: AccountId = "validator0".parse().unwrap();
    assert_eq!(
        forward_tx_requests.lock().as_slice(),
        &[(validator_acc.clone(), tx1.get_hash()), (validator_acc.clone(), tx1.get_hash())]
    );
    forward_tx_requests.lock().clear();

    // Now set validator_signer to None.
    env.rpc_node().client().validator_signer.update(None);

    // Submit tx2 twice
    let tx2 = SignedTransaction::send_money(
        2,
        user_accounts[0].clone(),
        user_accounts[0].clone(),
        &create_user_test_signer(&user_accounts[0]),
        Balance::from_near(1),
        env.rpc_node().head().last_block_hash,
    );
    env.node_datas[rpc_data_idx].rpc_handler_sender.send(ProcessTxRequest {
        transaction: tx2.clone(),
        is_forwarded: false,
        check_only: false,
    });
    env.node_datas[rpc_data_idx].rpc_handler_sender.send(ProcessTxRequest {
        transaction: tx2.clone(),
        is_forwarded: false,
        check_only: false,
    });

    // Run TestLoop for a bit
    env.rpc_runner().run_for_number_of_blocks(1);

    // There should be two ForwardTx(validator0, tx2) messages recorded.
    assert_eq!(
        forward_tx_requests.lock().as_slice(),
        &[(validator_acc.clone(), tx2.get_hash()), (validator_acc, tx2.get_hash())]
    );
}
