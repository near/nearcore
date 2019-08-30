use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use actix::{Actor, Addr, System};
use futures::future::Future;
use rand::Rng;
use tempdir::TempDir;

use near::config::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near::{load_test_config, start_with_config, GenesisConfig, NearConfig};
use near_client::{ClientActor, Query, Status, ViewClientActor};
use near_crypto::Signer;
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_network::NetworkClientMessages;
use near_primitives::rpc::{QueryResponse, ValidatorInfo};
use near_primitives::serialize::BaseEncode;
use near_primitives::test_utils::{heavy_test, init_integration_logger};
use near_primitives::transaction::{StakeTransaction, TransactionBody};
use near_primitives::types::AccountId;
use std::path::Path;

#[derive(Clone)]
struct TestNode {
    account_id: AccountId,
    config: NearConfig,
    client: Addr<ClientActor>,
    view_client: Addr<ViewClientActor>,
    genesis_hash: CryptoHash,
}

fn stake_transaction(
    nonce: Nonce,
    signer_id: AccountId,
    stake: Balance,
    signer: Arc<dyn Signer>,
    block_hash: CryptoHash,
) -> SignedTransaction {
    SignedTransaction::from_actions(
        nonce,
        signer_id.clone(),
        signer_id,
        signer.clone(),
        vec![Action::Stake(StakeAction { stake, public_key: signer.public_key() })],
        block_hash,
    )
}

fn init_test_staking(
    paths: Vec<&Path>,
    num_nodes: usize,
    num_validators: usize,
    epoch_length: u64,
    enable_rewards: bool,
) -> Vec<TestNode> {
    init_integration_logger();
    //    init_test_logger();

    let mut genesis_config = GenesisConfig::testing_spec(num_nodes, num_validators);
    genesis_config.epoch_length = epoch_length;
    genesis_config.num_block_producers = num_nodes;
    genesis_config.validator_kickout_threshold = 20;
    if !enable_rewards {
        genesis_config.max_inflation_rate = 0;
        genesis_config.gas_price = 0;
    }
    let first_node = open_port();

    let configs = (0..num_nodes).map(|i| {
        let mut config = load_test_config(
            &format!("near.{}", i),
            if i == 0 { first_node } else { open_port() },
            &genesis_config,
        );
        if i != 0 {
            config.network_config.boot_nodes = convert_boot_nodes(vec![("near.0", first_node)]);
        }
        config.client_config.min_num_peers = num_nodes - 1;
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
            false,
        );

        let tx = stake_transaction(
            1,
            test_nodes[1].account_id.clone(),
            TESTING_INIT_STAKE,
            test_nodes[1].config.block_producer.as_ref().unwrap().signer.clone(),
            test_nodes[1].genesis_hash,
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
            8,
            false,
        );
        let mut rng = rand::thread_rng();
        let stakes = (0..num_nodes / 2).map(|_| rng.gen_range(1, 100));
        let stake_transactions = stakes.enumerate().map(|(i, stake)| {
            let test_node = &test_nodes[i];
            stake_transaction(
                1,
                test_node.account_id.clone(),
                stake,
                test_node.config.block_producer.as_ref().unwrap().signer.clone(),
                test_node.genesis_hash,
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
                let test_node1 = test_nodes[num_nodes / 2].clone();
                let finalized_mark1 = finalized_mark.clone();

                actix::spawn(test_node1.client.send(Status {}).then(move |res| {
                    let expected: Vec<_> = (num_nodes / 2..num_nodes)
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
            100,
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
            test_nodes[1].genesis_hash,
        );

        let stake_transaction = stake_transaction(
            1,
            test_nodes[2].account_id.clone(),
            TESTING_INIT_STAKE,
            test_nodes[2].config.block_producer.as_ref().unwrap().signer.clone(),
            test_nodes[2].genesis_hash,
        );
        let stake_cost = stake_cost();

        let num_nodes = 4;
        let dirs = (0..num_nodes)
            .map(|i| TempDir::new(&format!("validator_join_{}", i)).unwrap())
            .collect::<Vec<_>>();
        let test_nodes = init_test_staking(
            dirs.iter().map(|dir| dir.path()).collect::<Vec<_>>(),
            num_nodes,
            2,
            16,
            false,
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
                                            && result.amount == TESTING_INIT_BALANCE - stake_cost
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
                                                == TESTING_INIT_BALANCE
                                                    - TESTING_INIT_STAKE
                                                    - stake_cost
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
