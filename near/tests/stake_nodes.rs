use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use actix::{Actor, Addr, System};
use futures::future::Future;
use rand::Rng;
use tempdir::TempDir;

use lazy_static::lazy_static;
use near::config::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near::{load_test_config, start_with_config, GenesisConfig, NearConfig};
use near_client::{ClientActor, Query, Status, ViewClientActor};
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_network::NetworkClientMessages;
use near_primitives::crypto::signer::EDSigner;
use near_primitives::rpc::{QueryResponse, ValidatorInfo};
use near_primitives::test_utils::init_integration_logger;
use near_primitives::transaction::{Action, SignedTransaction, StakeAction};
use near_primitives::types::{AccountId, Balance, Nonce};

lazy_static! {
    static ref HEAVY_TESTS_LOCK: Mutex<()> = Mutex::new(());
}

fn heavy_test<F>(f: F)
where
    F: FnOnce() -> (),
{
    let _guard = HEAVY_TESTS_LOCK.lock();
    f();
}

#[derive(Clone)]
struct TestNode {
    account_id: AccountId,
    config: NearConfig,
    client: Addr<ClientActor>,
    view_client: Addr<ViewClientActor>,
}

fn stake_transaction(
    nonce: Nonce,
    signer_id: AccountId,
    stake: Balance,
    signer: Arc<dyn EDSigner>,
) -> SignedTransaction {
    SignedTransaction::from_actions(
        nonce,
        signer_id.clone(),
        signer_id,
        signer.clone(),
        vec![Action::Stake(StakeAction { stake, public_key: signer.public_key() })],
    )
}

fn init_test_staking(num_accounts: usize, num_nodes: usize, epoch_length: u64) -> Vec<TestNode> {
    init_integration_logger();

    let mut genesis_config = GenesisConfig::testing_spec(num_accounts, num_nodes);
    genesis_config.epoch_length = epoch_length;
    genesis_config.num_block_producers = num_accounts;
    genesis_config.validator_kickout_threshold = 0.2;
    let first_node = open_port();

    let configs = (0..num_accounts).map(|i| {
        let mut config = load_test_config(
            &format!("near.{}", i),
            if i == 0 { first_node } else { open_port() },
            &genesis_config,
        );
        if i != 0 {
            config.network_config.boot_nodes = convert_boot_nodes(vec![("near.0", first_node)]);
        }
        config.client_config.skip_sync_wait = false;
        config.client_config.min_num_peers = num_nodes - 1;
        config
    });
    configs
        .enumerate()
        .map(|(i, config)| {
            let dir = TempDir::new(&format!("stake_node_{}", i)).unwrap();
            let (client, view_client) = start_with_config(dir.path(), config.clone());
            TestNode { account_id: format!("near.{}", i), config, client, view_client }
        })
        .collect()
}

/// Runs one validator network, sends staking transaction for the second node and
/// waits until it becomes a validator.
#[test]
fn test_stake_nodes() {
    heavy_test(|| {
        let system = System::new("NEAR");
        let test_nodes = init_test_staking(2, 1, 10);

        let tx = stake_transaction(
            1,
            test_nodes[1].account_id.clone(),
            TESTING_INIT_STAKE,
            test_nodes[1].config.block_producer.as_ref().unwrap().signer.clone(),
        );
        actix::spawn(
            test_nodes[0]
                .client
                .send(NetworkClientMessages::Transaction(tx))
                .map(|_| ())
                .map_err(|_| ()),
        );

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                actix::spawn(test_nodes[0].client.send(Status {}).then(|res| {
                    if res.unwrap().unwrap().validators
                        == vec![
                            ValidatorInfo { account_id: "near.1".to_string(), is_slashed: false },
                            ValidatorInfo { account_id: "near.0".to_string(), is_slashed: false },
                        ]
                    {
                        System::current().stop();
                    }
                    futures::future::ok(())
                }));
            }),
            100,
            5000,
        )
        .start();

        system.run().unwrap();
    });
}

/// TODO(1094): Enable kickout test after figuring
#[test]
#[ignore]
fn test_validator_kickout() {
    heavy_test(|| {
        let system = System::new("NEAR");
        let test_nodes = init_test_staking(4, 4, 24);
        let num_nodes = test_nodes.len();
        let mut rng = rand::thread_rng();
        let stakes = (0..num_nodes / 2).map(|_| rng.gen_range(1, 100));
        let stake_transactions = stakes.enumerate().map(|(i, stake)| {
            let test_node = &test_nodes[i];
            stake_transaction(
                1,
                test_node.account_id.clone(),
                stake,
                test_node.config.block_producer.as_ref().unwrap().signer.clone(),
            )
        });

        for (i, stake_transaction) in stake_transactions.enumerate() {
            let test_node = &test_nodes[i];
            actix::spawn(
                test_node
                    .client
                    .send(NetworkClientMessages::Transaction(stake_transaction))
                    .map(|_| ())
                    .map_err(|_| ()),
            );
        }

        let finalized_mark: Arc<Vec<_>> =
            Arc::new((0..num_nodes).map(|_| Arc::new(AtomicBool::new(false))).collect());

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let test_nodes = test_nodes.clone();
                let test_node1 = test_nodes[0].clone();
                let finalized_mark1 = finalized_mark.clone();

                actix::spawn(test_node1.client.send(Status {}).then(move |res| {
                    let expected: Vec<_> = (num_nodes / 2..num_nodes)
                        .cycle()
                        .take(num_nodes)
                        .map(|i| ValidatorInfo {
                            account_id: format!("near.{}", i),
                            is_slashed: false,
                        })
                        .collect();
                    if res.unwrap().unwrap().validators == expected {
                        for i in 0..num_nodes / 2 {
                            let mark = finalized_mark1[i].clone();
                            actix::spawn(
                                test_node1
                                    .view_client
                                    .send(Query {
                                        path: format!(
                                            "account/{}",
                                            test_nodes[i].account_id.clone()
                                        ),
                                        data: vec![],
                                    })
                                    .then(move |res| match res.unwrap().unwrap() {
                                        QueryResponse::ViewAccount(result) => {
                                            if result.staked == 0
                                                || result.amount == TESTING_INIT_BALANCE
                                            {
                                                mark.store(true, Ordering::SeqCst);
                                            }
                                            futures::future::ok(())
                                        }
                                        _ => panic!("wrong return result"),
                                    }),
                            );
                        }
                        for i in num_nodes / 2..num_nodes {
                            let mark = finalized_mark1[i].clone();

                            actix::spawn(
                                test_node1
                                    .view_client
                                    .send(Query {
                                        path: format!(
                                            "account/{}",
                                            test_nodes[i].account_id.clone()
                                        ),
                                        data: vec![],
                                    })
                                    .then(move |res| match res.unwrap().unwrap() {
                                        QueryResponse::ViewAccount(result) => {
                                            assert_eq!(result.staked, TESTING_INIT_STAKE);
                                            assert_eq!(
                                                result.amount,
                                                TESTING_INIT_BALANCE - TESTING_INIT_STAKE
                                            );
                                            mark.store(true, Ordering::SeqCst);
                                            futures::future::ok(())
                                        }
                                        _ => panic!("wrong return result"),
                                    }),
                            );
                        }

                        if finalized_mark1.iter().all(|mark| mark.load(Ordering::SeqCst)) {
                            System::current().stop();
                        }
                    }
                    futures::future::ok(())
                }));
            }),
            1000,
            10000,
        )
        .start();

        system.run().unwrap();
    })
}

#[test]
fn test_validator_join() {
    heavy_test(|| {
        let system = System::new("NEAR");
        let test_nodes = init_test_staking(4, 2, 16);
        let unstake_transaction = stake_transaction(
            1,
            test_nodes[1].account_id.clone(),
            0,
            test_nodes[1].config.block_producer.as_ref().unwrap().signer.clone(),
        );

        let stake_transaction = stake_transaction(
            1,
            test_nodes[2].account_id.clone(),
            TESTING_INIT_STAKE,
            test_nodes[2].config.block_producer.as_ref().unwrap().signer.clone(),
        );
        actix::spawn(
            test_nodes[1]
                .client
                .send(NetworkClientMessages::Transaction(unstake_transaction))
                .map(|_| ())
                .map_err(|_| ()),
        );
        actix::spawn(
            test_nodes[0]
                .client
                .send(NetworkClientMessages::Transaction(stake_transaction))
                .map(|_| ())
                .map_err(|_| ()),
        );

        let (done1, done2) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
        let (done1_copy1, done2_copy1) = (done1.clone(), done2.clone());
        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let test_nodes = test_nodes.clone();
                let test_node1 = test_nodes[0].clone();
                let (done1_copy2, done2_copy2) = (done1_copy1.clone(), done2_copy1.clone());
                actix::spawn(test_node1.client.send(Status {}).then(move |res| {
                    let expected = vec![
                        ValidatorInfo { account_id: "near.0".to_string(), is_slashed: false },
                        ValidatorInfo { account_id: "near.2".to_string(), is_slashed: false },
                    ];
                    if res.unwrap().unwrap().validators == expected {
                        actix::spawn(
                            test_node1
                                .view_client
                                .send(Query {
                                    path: format!("account/{}", test_nodes[1].account_id.clone()),
                                    data: vec![],
                                })
                                .then(move |res| match res.unwrap().unwrap() {
                                    QueryResponse::ViewAccount(result) => {
                                        if result.staked == 0
                                            && result.amount == TESTING_INIT_BALANCE
                                        {
                                            done1_copy2.store(true, Ordering::SeqCst);
                                        }
                                        futures::future::ok(())
                                    }
                                    _ => panic!("wrong return result"),
                                }),
                        );
                        actix::spawn(
                            test_node1
                                .view_client
                                .send(Query {
                                    path: format!("account/{}", test_nodes[2].account_id.clone()),
                                    data: vec![],
                                })
                                .then(move |res| match res.unwrap().unwrap() {
                                    QueryResponse::ViewAccount(result) => {
                                        if result.staked == TESTING_INIT_STAKE
                                            && result.amount
                                                == TESTING_INIT_BALANCE - TESTING_INIT_STAKE
                                        {
                                            done2_copy2.store(true, Ordering::SeqCst);
                                        }

                                        futures::future::ok(())
                                    }
                                    _ => panic!("wrong return result"),
                                }),
                        );
                    }

                    futures::future::ok(())
                }));
                if done1_copy1.load(Ordering::SeqCst) && done2_copy1.load(Ordering::SeqCst) {
                    System::current().stop();
                }
            }),
            1000,
            10000,
        )
        .start();

        system.run().unwrap();
    });
}
