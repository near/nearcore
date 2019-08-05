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
use near_primitives::rpc::{QueryResponse, ValidatorInfo};
use near_primitives::serialize::BaseEncode;
use near_primitives::test_utils::init_integration_logger;
use near_primitives::transaction::{StakeTransaction, TransactionBody};
use near_primitives::types::AccountId;
use std::path::Path;

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

fn init_test_staking(
    paths: Vec<&Path>,
    num_nodes: usize,
    num_validators: usize,
    epoch_length: u64,
) -> Vec<TestNode> {
    init_integration_logger();

    let mut genesis_config = GenesisConfig::testing_spec(num_nodes, num_validators);
    genesis_config.epoch_length = epoch_length;
    genesis_config.num_block_producers = num_nodes;
    genesis_config.validator_kickout_threshold = 20;
    let first_node = open_port();

    let configs = (0..num_nodes).map(|i| {
        let mut config = load_test_config(
            &format!("near.{}", i),
            if i == 0 { first_node } else { open_port() },
            &genesis_config,
        );
        if i != 0 {
            config.network_config.boot_nodes = convert_boot_nodes(vec![("near.0", first_node)]);
            config.client_config.min_num_peers = 1;
        } else {
            config.client_config.min_num_peers = 0;
        }
        config
    });
    configs
        .enumerate()
        .map(|(i, config)| {
            let (client, view_client) = start_with_config(paths[i], config.clone());
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
        let num_nodes = 2;
        let dirs = (0..num_nodes)
            .map(|i| TempDir::new(&format!("stake_node_{}", i)).unwrap())
            .collect::<Vec<_>>();
        let test_nodes = init_test_staking(
            dirs.iter().map(|dir| dir.path()).collect::<Vec<_>>(),
            num_nodes,
            1,
            10,
        );

        let tx = TransactionBody::Stake(StakeTransaction {
            nonce: 1,
            originator: test_nodes[1].account_id.clone(),
            amount: TESTING_INIT_STAKE,
            public_key: test_nodes[1]
                .config
                .block_producer
                .clone()
                .unwrap()
                .signer
                .public_key()
                .to_base(),
        })
        .sign(&*test_nodes[1].config.block_producer.clone().unwrap().signer);
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
        let num_nodes = 4;
        let dirs = (0..num_nodes)
            .map(|i| TempDir::new(&format!("validator_kickout_{}", i)).unwrap())
            .collect::<Vec<_>>();
        let test_nodes = init_test_staking(
            dirs.iter().map(|dir| dir.path()).collect::<Vec<_>>(),
            num_nodes,
            4,
            24,
        );
        let mut rng = rand::thread_rng();
        let stakes = (0..num_nodes / 2).map(|_| rng.gen_range(1, 100));
        let stake_transactions = stakes.enumerate().map(|(i, stake)| {
            let test_node = &test_nodes[i];
            TransactionBody::Stake(StakeTransaction {
                nonce: 1,
                originator: test_node.account_id.clone(),
                amount: stake,
                public_key: test_node
                    .config
                    .block_producer
                    .as_ref()
                    .unwrap()
                    .signer
                    .public_key()
                    .to_base(),
            })
            .sign(&*test_node.config.block_producer.as_ref().unwrap().signer)
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
                                            if result.stake == 0
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
                                            assert_eq!(result.stake, TESTING_INIT_STAKE);
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
        let num_nodes = 4;
        let dirs = (0..num_nodes)
            .map(|i| TempDir::new(&format!("validator_join_{}", i)).unwrap())
            .collect::<Vec<_>>();
        let test_nodes = init_test_staking(
            dirs.iter().map(|dir| dir.path()).collect::<Vec<_>>(),
            num_nodes,
            2,
            16,
        );
        let unstake_transaction = TransactionBody::Stake(StakeTransaction {
            nonce: 1,
            originator: test_nodes[1].account_id.clone(),
            amount: 0,
            public_key: test_nodes[1]
                .config
                .block_producer
                .as_ref()
                .unwrap()
                .signer
                .public_key()
                .to_base(),
        })
        .sign(&*test_nodes[1].config.block_producer.as_ref().unwrap().signer);
        let stake_transaction = TransactionBody::Stake(StakeTransaction {
            nonce: 1,
            originator: test_nodes[2].account_id.clone(),
            amount: TESTING_INIT_STAKE,
            public_key: test_nodes[2]
                .config
                .block_producer
                .as_ref()
                .unwrap()
                .signer
                .public_key()
                .to_base(),
        })
        .sign(&*test_nodes[2].config.block_producer.as_ref().unwrap().signer);
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
                        ValidatorInfo { account_id: "near.0".to_string(), is_slashed: false },
                        ValidatorInfo { account_id: "near.2".to_string(), is_slashed: false },
                    ];
                    println!("\n\n\nRESULT: {:?}\n\n\n", res);
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
                                        if result.stake == 0
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
                                        if result.stake == TESTING_INIT_STAKE
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
            20000,
        )
        .start();

        system.run().unwrap();
    });
}
