use std::sync::Arc;

use itertools::Itertools as _;
use near_async::messaging::{CanSend as _, Handler as _};
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::{ProcessTxRequest, Query};
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockReference, NumSeats};
use near_primitives::views::{QueryRequest, QueryResponseKind};
use parking_lot::RwLock;
use rand::{Rng as _, thread_rng};

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;
use crate::utils::rotating_validators_runner::RotatingValidatorsRunner;
use crate::utils::transactions::get_anchor_hash;
use crate::utils::{ONE_NEAR, get_node_data};

struct Params {
    num_transfers: usize,
    rotate_validators: bool,
    /// Chunk messages will be dropped with 20% probability.
    drop_chunks: bool,
}

/// The basic flow of the test spins up validators, and starts sending to them
/// several (at most 64) cross-shard transactions. i-th transaction sends money from
/// validator i/8 to validator i%8. It submits txs one at a time, and waits for their completion
/// before sending the next.
fn test_cross_shard_tx_common(Params { num_transfers, rotate_validators, drop_chunks }: Params) {
    init_test_logger();

    let seed: u64 = thread_rng().r#gen();
    println!("RNG seed: {seed}. If test fails use it to find the issue.");
    let rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(seed);
    let rng = Arc::new(RwLock::new(rng));

    let stake = ONE_NEAR;

    let (mut runner, validator_spec, validator_accounts) = if rotate_validators {
        let validators: Vec<Vec<AccountId>> = [
            [
                "test1.1", "test1.2", "test1.3", "test1.4", "test1.5", "test1.6", "test1.7",
                "test1.8",
            ],
            [
                "test2.1", "test2.2", "test2.3", "test2.4", "test2.5", "test2.6", "test2.7",
                "test2.8",
            ],
            [
                "test3.1", "test3.2", "test3.3", "test3.4", "test3.5", "test3.6", "test3.7",
                "test3.8",
            ],
        ]
        .iter()
        .map(|vec| vec.iter().map(|account| account.parse().unwrap()).collect())
        .collect();

        let seats: NumSeats = validators[0].len().try_into().unwrap();
        let runner = RotatingValidatorsRunner::new(stake, validators);
        let validator_accounts = runner.all_validators_accounts();
        let validator_spec = runner.genesis_validators_spec(seats, seats, seats);
        (Runner::RotatingValidatorsRunner(runner), validator_spec, validator_accounts)
    } else {
        let validators = [
            "test1.1", "test1.2", "test1.3", "test1.4", "test1.5", "test1.6", "test1.7", "test1.8",
        ];
        let validator_accounts =
            validators.iter().map(|account| account.parse().unwrap()).collect();
        let validator_spec = ValidatorsSpec::desired_roles(&validators, &[]);
        (Runner::SimpleRunner, validator_spec, validator_accounts)
    };

    let test_accounts: Vec<AccountId> =
        (0..8).map(|i| format!("test_account{i}").parse().unwrap()).collect_vec();

    let epoch_length: u64 = 20;
    let num_shards = 8;
    let shard_layout =
        ShardLayout::multi_shard_custom(test_accounts.iter().skip(1).cloned().collect_vec(), 3);

    assert_eq!(shard_layout.num_shards(), num_shards);
    assert!(
        test_accounts
            .iter()
            .map(|account| shard_layout.account_id_to_shard_id(account))
            .all_unique()
    );

    let mut genesis_builder = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validator_spec)
        .add_user_accounts_simple(&validator_accounts, stake)
        .shard_layout(shard_layout);

    let mut balances = vec![];

    for (i, account) in test_accounts.iter().enumerate() {
        let amount = 1000 + 100 * i as u128;
        genesis_builder = genesis_builder.add_user_account_simple(account.clone(), amount);

        balances.push(amount);
    }

    let genesis = genesis_builder.build();

    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .minimum_validators_per_shard(2)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(validator_accounts)
        .epoch_config_store(epoch_config_store)
        .build()
        .warmup();

    for node_datas in &env.node_datas {
        let rng = rng.clone();
        let peer_actor_handle = node_datas.peer_manager_sender.actor_handle();
        let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
        peer_actor.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
            let mut rng = rng.write();
            match &request {
                NetworkRequests::PartialEncodedChunkRequest { .. }
                | NetworkRequests::PartialEncodedChunkResponse { .. }
                | NetworkRequests::PartialEncodedChunkMessage { .. }
                | NetworkRequests::PartialEncodedChunkForward { .. } => {
                    if drop_chunks && rng.gen_ratio(1, 5) {
                        return None;
                    }
                }
                _ => (),
            }
            Some(request)
        }));
    }

    let mut finished_transfers = 0;
    let mut nonce = 1;
    let mut unsuccessful_queries = 0;

    let observed_balances = get_balances(&mut env.test_loop.data, &env.node_datas, &test_accounts);
    assert_eq!(balances, observed_balances);

    let node_datas = env.node_datas.clone();

    let mut max_height = 0;
    runner.run_until(
        &mut env,
        |test_loop_data| {
            let observed_balances = get_balances(test_loop_data, &node_datas, &test_accounts);
            if balances != observed_balances {
                unsuccessful_queries += 1;
                if unsuccessful_queries % 100 == 0 {
                    println!("Waiting for balances; finished transfers: {finished_transfers}");
                    println!("Expected: {:?}", balances);
                    println!("Received: {:?}", observed_balances);
                }
                return false;
            }
            assert_eq!(balances, observed_balances);
            finished_transfers += 1;
            if finished_transfers > num_transfers {
                return true;
            }

            let clients = node_datas
                .iter()
                .map(|test_data| {
                    &test_loop_data.get(&test_data.client_sender.actor_handle()).client
                })
                .collect_vec();

            max_height = std::cmp::max(max_height, clients[0].chain.head().unwrap().height);

            let anchor_hash = get_anchor_hash(&clients);

            let from = finished_transfers % test_accounts.len();
            let to = (finished_transfers / test_accounts.len()) % test_accounts.len();
            let amount = (5 + finished_transfers) as u128;

            nonce += 1;
            let tx = SignedTransaction::send_money(
                nonce,
                test_accounts[from].clone(),
                test_accounts[to].clone(),
                &create_user_test_signer(&test_accounts[from]),
                amount,
                anchor_hash,
            );
            node_datas[0].rpc_handler_sender.send(ProcessTxRequest {
                transaction: tx,
                is_forwarded: false,
                check_only: false,
            });

            balances[from] -= amount;
            balances[to] += amount;

            false
        },
        Duration::seconds(num_transfers as i64 * 4),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

enum Runner {
    RotatingValidatorsRunner(RotatingValidatorsRunner),
    SimpleRunner,
}

impl Runner {
    fn run_until(
        &mut self,
        env: &mut TestLoopEnv,
        condition: impl FnMut(&mut TestLoopData) -> bool,
        maximum_duration: Duration,
    ) {
        match self {
            Runner::RotatingValidatorsRunner(runner) => {
                runner.run_until(env, condition, maximum_duration)
            }
            Runner::SimpleRunner { .. } => env.test_loop.run_until(condition, maximum_duration),
        }
    }
}

fn get_balances(
    test_loop_data: &mut TestLoopData,
    node_datas: &Vec<NodeExecutionData>,
    test_accounts: &Vec<AccountId>,
) -> Vec<u128> {
    let client_handle = node_datas[0].client_sender.actor_handle();
    let client = &test_loop_data.get(&client_handle).client;
    let epoch_manager = client.chain.epoch_manager.clone();
    let head = client.chain.head().unwrap();
    let epoch_id = head.epoch_id;
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

    test_accounts
        .iter()
        .map(|account| {
            let shard_id = shard_layout.account_id_to_shard_id(account);
            let cp =
                &epoch_manager.get_epoch_chunk_producers_for_shard(&epoch_id, shard_id).unwrap()[0];

            let view_client_handle =
                get_node_data(&node_datas, cp).view_client_sender.actor_handle();

            let view_client = test_loop_data.get_mut(&view_client_handle);

            let query_response = view_client.handle(Query::new(
                BlockReference::latest(),
                QueryRequest::ViewAccount { account_id: account.clone() },
            ));

            let query_response = query_response.unwrap();

            let QueryResponseKind::ViewAccount(view_account_result) = query_response.kind else {
                panic!();
            };
            view_account_result.amount
        })
        .collect_vec()
}

#[test]
fn ultra_slow_test_cross_shard_tx() {
    test_cross_shard_tx_common(Params {
        num_transfers: 64,
        rotate_validators: false,
        drop_chunks: false,
    });
}

#[test]
fn ultra_slow_test_cross_shard_tx_drop_chunks() {
    test_cross_shard_tx_common(Params {
        num_transfers: 64,
        rotate_validators: false,
        drop_chunks: true,
    });
}

#[test]
fn ultra_slow_test_cross_shard_tx_with_validator_rotation() {
    test_cross_shard_tx_common(Params {
        num_transfers: 24,
        rotate_validators: true,
        drop_chunks: false,
    });
}

#[test]
fn ultra_slow_test_cross_shard_tx_with_validator_rotation_drop_chunks() {
    test_cross_shard_tx_common(Params {
        num_transfers: 24,
        rotate_validators: true,
        drop_chunks: true,
    });
}
