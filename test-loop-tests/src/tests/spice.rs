use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use itertools::Itertools;
use near_async::messaging::{CanSend as _, Handler as _};
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::{ProcessTxRequest, Query};
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::stateless_validation::spice_chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, BlockHeight, BlockReference};
use near_primitives::views::{QueryRequest, QueryResponseKind};
use parking_lot::{Mutex, RwLock};

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{
    create_account_id, create_validators_spec, validators_spec_clients,
    validators_spec_clients_with_rpc,
};
use crate::utils::get_node_data;
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::get_anchor_hash;

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
        .transaction_validity_period(1000)
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

    let get_balance = |test_loop_data: &mut TestLoopData, account, epoch_id| {
        let shard_id = shard_layout.account_id_to_shard_id(account);
        let cp =
            &epoch_manager.get_epoch_chunk_producers_for_shard(&epoch_id, shard_id).unwrap()[0];

        let view_client_handle = get_node_data(&node_datas, cp).view_client_sender.actor_handle();

        let view_client = test_loop_data.get_mut(&view_client_handle);

        let query_response = view_client.handle(Query::new(
            BlockReference::Finality(near_primitives::types::Finality::Final),
            QueryRequest::ViewAccount { account_id: account.clone() },
        ));

        let query_response = query_response.unwrap();

        let QueryResponseKind::ViewAccount(view_account_result) = query_response.kind else {
            panic!();
        };
        view_account_result.amount
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

    let view_client = env.test_loop.data.get_mut(&node.data().view_client_sender.actor_handle());
    let query_response = view_client
        .handle(Query::new(
            BlockReference::Finality(near_primitives::types::Finality::None),
            QueryRequest::ViewAccount { account_id: receiver },
        ))
        .unwrap();
    let QueryResponseKind::ViewAccount(view_account_result) = query_response.kind else {
        panic!();
    };
    assert_eq!(view_account_result.amount, Balance::from_near(1));

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn delay_endorsements_propagation(env: &mut TestLoopEnv, delay_height: u64) {
    let core_writer_senders: HashMap<_, _> = env
        .node_datas
        .iter()
        .map(|datas| (datas.account_id.clone(), datas.spice_core_writer_sender.clone()))
        .collect();

    for node in &env.node_datas {
        let senders = core_writer_senders.clone();
        let block_heights: Arc<RwLock<HashMap<CryptoHash, BlockHeight>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let delayed_endorsements: Arc<
            RwLock<VecDeque<(CryptoHash, AccountId, SpiceChunkEndorsement)>>,
        > = Arc::new(RwLock::new(VecDeque::new()));
        let peer_actor = env.test_loop.data.get_mut(&node.peer_manager_sender.actor_handle());
        peer_actor.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
            match request {
                NetworkRequests::Block { ref block } => {
                    block_heights.write().insert(*block.hash(), block.header().height());

                    let mut delayed_endorsements = delayed_endorsements.write();
                    loop {
                        let Some(front) = delayed_endorsements.front() else {
                            break;
                        };
                        let height = block_heights.read()[&front.0];
                        if height + delay_height >= block.header().height() {
                            break;
                        }
                        let (_, target, endorsement) = delayed_endorsements.pop_front().unwrap();
                        senders[&target].send(SpiceChunkEndorsementMessage(endorsement));
                    }
                    Some(request)
                }
                NetworkRequests::SpiceChunkEndorsement(target, endorsement) => {
                    delayed_endorsements.write().push_back((
                        *endorsement.block_hash(),
                        target,
                        endorsement,
                    ));
                    None
                }
                _ => Some(request),
            }
        }));
    }
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

    let view_client_handle = rpc_node.data().view_client_sender.actor_handle();
    // Note that TestLoopNode::view_account_query doesn't work with spice yet.
    let get_balance = |test_loop_data: &mut TestLoopData, account: &AccountId| {
        let view_client = test_loop_data.get_mut(&view_client_handle);
        let query_response = view_client.handle(Query::new(
            BlockReference::Finality(near_primitives::types::Finality::Final),
            QueryRequest::ViewAccount { account_id: account.clone() },
        ));
        let QueryResponseKind::ViewAccount(view_account_result) = query_response.unwrap().kind
        else {
            panic!();
        };
        view_account_result.amount
    };

    for account in &accounts {
        let got_balance = get_balance(&mut env.test_loop.data, account);
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

        if !client.chain.spice_core_reader.all_execution_results_exist(&block).unwrap() {
            let execution_results =
                client.chain.spice_core_reader.get_execution_results_by_shard_id(&block).unwrap();
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
        let got_balance = get_balance(&mut env.test_loop.data, account);
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
