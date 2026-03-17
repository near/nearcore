use crate::setup::builder::TestLoopBuilder;
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::{execute_money_transfers, get_anchor_hash, get_next_nonce};
use itertools::Itertools;
use near_async::messaging::CanSend;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_network::client::ProcessTxRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance};

const NUM_CLIENTS: usize = 4;

// Test that a new node that only has genesis can use whatever method available
// to sync up to the current state of the network.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_sync_from_genesis() {
    init_test_logger();
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(NUM_CLIENTS).cloned().collect_vec();

    let validators_spec =
        ValidatorsSpec::desired_roles(&clients.iter().map(|t| t.as_str()).collect_vec(), &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .genesis_height(10000)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build();

    let first_epoch_tracked_shards = env
        .node_datas
        .iter()
        .map(|node_data| TestLoopNode { data: &env.test_loop.data, node_data }.tracked_shards())
        .collect_vec();
    tracing::info!(?first_epoch_tracked_shards, "first epoch tracked shards");

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();

    // Make sure the chain progresses for several epochs.
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().height > 10050
        },
        Duration::seconds(50),
    );

    // Add new node
    let new_node_state = env.node_state_builder().account_id(&accounts[NUM_CLIENTS]).build();
    env.add_node(accounts[NUM_CLIENTS].as_str(), new_node_state);

    // Check that the new node will reach a high height as well.
    let new_node = env.node_datas.last().unwrap().client_sender.actor_handle();
    env.test_loop.run_until(
        |test_loop_data| test_loop_data.get(&new_node).client.chain.head().unwrap().height > 10050,
        Duration::seconds(20),
    );
}

/// Kill all validators simultaneously and restart them, repeating multiple
/// times. Verifies that validators with NoShards tracking can recover via
/// state sync under cross-shard transaction load with shard assignment
/// shuffling.
#[test]
fn slow_test_validator_restart_under_cross_shard_load() {
    init_test_logger();

    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(NUM_CLIENTS).cloned().collect_vec();

    let validators_spec =
        ValidatorsSpec::desired_roles(&clients.iter().map(|t| t.as_str()).collect_vec(), &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .shard_layout(ShardLayout::multi_shard(4, 3))
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .genesis_height(10000)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .gc_num_epochs_to_keep(20)
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(20);

    // Two cycles: first tests restart after normal operation, second tests
    // restart after a restart.
    for i in 0..2 {
        // Collect identifiers and accounts for all validators before killing.
        let node_infos: Vec<_> = (0..NUM_CLIENTS)
            .map(|idx| {
                let account: AccountId = format!("account{}", idx).parse().unwrap();
                let data_idx = env.account_data_idx(&account);
                let data = &env.node_datas[data_idx];
                (data.account_id.clone(), data.identifier.clone())
            })
            .collect();

        // Restart all validators.
        let killed_states: Vec<_> =
            node_infos.iter().map(|(_, identifier)| env.kill_node(identifier)).collect();
        for ((_, identifier), state) in node_infos.iter().zip(killed_states) {
            let new_id = format!("{}-restart-{}", identifier, i);
            env.restart_node(&new_id, state);
        }

        // Give the restarted validators time to recover consensus. After a
        // simultaneous restart, doomslug needs to go through its timeout
        // mechanism before validators can produce blocks again.
        let reference_account: AccountId = "account0".parse().unwrap();
        env.runner_for_account(&reference_account)
            .run_for_number_of_blocks_with_timeout(15, Duration::seconds(60));

        // Let a few more blocks propagate so all nodes fully converge.
        env.test_loop.run_for(Duration::seconds(5));

        // Verify all validators reached the same height.
        let reference_height = env.node_for_account(&reference_account).head().height;
        for idx in 1..NUM_CLIENTS {
            let account: AccountId = format!("account{}", idx).parse().unwrap();
            assert_eq!(
                env.node_for_account(&account).head().height,
                reference_height,
                "Node {} failed to catch up on restart cycle {i}",
                idx,
            );
        }

        // Send cross-shard transfers after each restart to generate traffic
        // across all shards.
        let live_nodes = &env.node_datas[env.node_datas.len() - NUM_CLIENTS..];
        let clients: Vec<_> = live_nodes
            .iter()
            .map(|nd| &env.test_loop.data.get(&nd.client_sender.actor_handle()).client)
            .collect();
        let anchor_hash = get_anchor_hash(&clients);
        for (j, sender) in accounts.iter().enumerate() {
            let receiver = &accounts[(j + 1) % accounts.len()];
            let nonce = get_next_nonce(&env.test_loop.data, live_nodes, sender);
            let tx = SignedTransaction::send_money(
                nonce,
                sender.clone(),
                receiver.clone(),
                &create_user_test_signer(sender),
                Balance::from_near(1),
                anchor_hash,
            );
            live_nodes[j % NUM_CLIENTS].rpc_handler_sender.send(ProcessTxRequest {
                transaction: tx,
                is_forwarded: false,
                check_only: false,
            });
        }
    }
}
