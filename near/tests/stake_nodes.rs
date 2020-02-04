use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use actix::{Actor, Addr, System};
use futures::{future, FutureExt};
use rand::Rng;
use tempdir::TempDir;

use near::config::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near::{load_test_config, start_with_config, GenesisConfig, NearConfig};
use near_client::{ClientActor, GetBlock, Query, Status, ViewClientActor};
use near_crypto::{InMemorySigner, KeyType};
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_network::NetworkClientMessages;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::{heavy_test, init_integration_logger};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeightDelta, NumSeats};
use near_primitives::views::{QueryRequest, QueryResponseKind, ValidatorInfo};
use testlib::genesis_hash;

#[derive(Clone)]
struct TestNode {
    account_id: AccountId,
    signer: Arc<InMemorySigner>,
    config: NearConfig,
    client: Addr<ClientActor>,
    view_client: Addr<ViewClientActor>,
    genesis_hash: CryptoHash,
}

fn init_test_staking(
    paths: Vec<&Path>,
    num_node_seats: NumSeats,
    num_validator_seats: NumSeats,
    epoch_length: BlockHeightDelta,
    enable_rewards: bool,
) -> Vec<TestNode> {
    init_integration_logger();

    let seeds = (0..num_node_seats).map(|i| format!("near.{}", i)).collect::<Vec<_>>();
    let mut genesis_config =
        GenesisConfig::test(seeds.iter().map(|s| s.as_str()).collect(), num_validator_seats);
    genesis_config.epoch_length = epoch_length;
    genesis_config.num_block_producer_seats = num_node_seats;
    genesis_config.block_producer_kickout_threshold = 20;
    genesis_config.chunk_producer_kickout_threshold = 20;
    if !enable_rewards {
        genesis_config.max_inflation_rate = 0;
        genesis_config.min_gas_price = 0;
    }
    let first_node = open_port();

    let configs = (0..num_node_seats).map(|i| {
        let mut config = load_test_config(
            &format!("near.{}", i),
            if i == 0 { first_node } else { open_port() },
            &genesis_config,
        );
        if i != 0 {
            config.network_config.boot_nodes = convert_boot_nodes(vec![("near.0", first_node)]);
        }
        config.client_config.min_num_peers = num_node_seats as usize - 1;
        config
    });
    configs
        .enumerate()
        .map(|(i, config)| {
            let genesis_hash = genesis_hash(&config.genesis_config);
            let (client, view_client) = start_with_config(paths[i], config.clone());
            let account_id = format!("near.{}", i);
            let signer =
                Arc::new(InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id));
            TestNode { account_id, signer, config, client, view_client, genesis_hash }
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

        let tx = SignedTransaction::stake(
            1,
            test_nodes[1].account_id.clone(),
            // &*test_nodes[1].config.block_producer.as_ref().unwrap().signer,
            &*test_nodes[1].signer,
            TESTING_INIT_STAKE,
            test_nodes[1].config.block_producer.as_ref().unwrap().signer.public_key(),
            test_nodes[1].genesis_hash,
        );
        actix::spawn(test_nodes[0].client.send(NetworkClientMessages::Transaction(tx)).map(drop));

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                actix::spawn(test_nodes[0].client.send(Status { is_health_check: false }).then(
                    |res| {
                        let res = res.unwrap();
                        if res.is_err() {
                            return future::ready(());
                        }
                        if res.unwrap().validators
                            == vec![
                                ValidatorInfo {
                                    account_id: "near.1".to_string(),
                                    is_slashed: false,
                                },
                                ValidatorInfo {
                                    account_id: "near.0".to_string(),
                                    is_slashed: false,
                                },
                            ]
                        {
                            System::current().stop();
                        }
                        future::ready(())
                    },
                ));
            }),
            100,
            5000,
        )
        .start();

        system.run().unwrap();
    });
}

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
            let signer = Arc::new(InMemorySigner::from_seed(
                &test_node.account_id,
                KeyType::ED25519,
                &test_node.account_id,
            ));
            SignedTransaction::stake(
                1,
                test_node.account_id.clone(),
                &*signer,
                stake,
                test_node.config.block_producer.as_ref().unwrap().signer.public_key(),
                test_node.genesis_hash,
            )
        });

        for (i, stake_transaction) in stake_transactions.enumerate() {
            let test_node = &test_nodes[i];
            actix::spawn(
                test_node
                    .client
                    .send(NetworkClientMessages::Transaction(stake_transaction))
                    .map(drop),
            );
        }

        let finalized_mark: Arc<Vec<_>> =
            Arc::new((0..num_nodes).map(|_| Arc::new(AtomicBool::new(false))).collect());

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let test_nodes = test_nodes.clone();
                let test_node1 = test_nodes[(num_nodes / 2) as usize].clone();
                let finalized_mark1 = finalized_mark.clone();

                actix::spawn(test_node1.client.send(Status { is_health_check: false }).then(
                    move |res| {
                        let expected: Vec<_> = (num_nodes / 2..num_nodes)
                            .map(|i| ValidatorInfo {
                                account_id: format!("near.{}", i),
                                is_slashed: false,
                            })
                            .collect();
                        let res = res.unwrap();
                        if res.is_err() {
                            return future::ready(());
                        }
                        if res.unwrap().validators == expected {
                            for i in 0..num_nodes / 2 {
                                let mark = finalized_mark1[i as usize].clone();
                                actix::spawn(
                                    test_node1
                                        .view_client
                                        .send(Query::new(
                                            None,
                                            QueryRequest::ViewAccount {
                                                account_id: test_nodes[i as usize]
                                                    .account_id
                                                    .clone(),
                                            },
                                        ))
                                        .then(move |res| {
                                            match res.unwrap().unwrap().unwrap().kind {
                                                QueryResponseKind::ViewAccount(result) => {
                                                    if result.locked == 0
                                                        || result.amount == TESTING_INIT_BALANCE
                                                    {
                                                        mark.store(true, Ordering::SeqCst);
                                                    }
                                                    future::ready(())
                                                }
                                                _ => panic!("wrong return result"),
                                            }
                                        }),
                                );
                            }
                            for i in num_nodes / 2..num_nodes {
                                let mark = finalized_mark1[i as usize].clone();

                                actix::spawn(
                                    test_node1
                                        .view_client
                                        .send(Query::new(
                                            None,
                                            QueryRequest::ViewAccount {
                                                account_id: test_nodes[i as usize]
                                                    .account_id
                                                    .clone(),
                                            },
                                        ))
                                        .then(move |res| {
                                            match res.unwrap().unwrap().unwrap().kind {
                                                QueryResponseKind::ViewAccount(result) => {
                                                    assert_eq!(result.locked, TESTING_INIT_STAKE);
                                                    assert_eq!(
                                                        result.amount,
                                                        TESTING_INIT_BALANCE - TESTING_INIT_STAKE
                                                    );
                                                    mark.store(true, Ordering::SeqCst);
                                                    future::ready(())
                                                }
                                                _ => panic!("wrong return result"),
                                            }
                                        }),
                                );
                            }

                            if finalized_mark1.iter().all(|mark| mark.load(Ordering::SeqCst)) {
                                System::current().stop();
                            }
                        }
                        future::ready(())
                    },
                ));
            }),
            100,
            20000,
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
            false,
        );
        let signer = Arc::new(InMemorySigner::from_seed(
            &test_nodes[1].account_id,
            KeyType::ED25519,
            &test_nodes[1].account_id,
        ));
        let unstake_transaction = SignedTransaction::stake(
            1,
            test_nodes[1].account_id.clone(),
            &*signer,
            0,
            test_nodes[1].config.block_producer.as_ref().unwrap().signer.public_key(),
            test_nodes[1].genesis_hash,
        );

        let signer = Arc::new(InMemorySigner::from_seed(
            &test_nodes[2].account_id,
            KeyType::ED25519,
            &test_nodes[2].account_id,
        ));
        let stake_transaction = SignedTransaction::stake(
            1,
            test_nodes[2].account_id.clone(),
            &*signer,
            TESTING_INIT_STAKE,
            test_nodes[2].config.block_producer.as_ref().unwrap().signer.public_key(),
            test_nodes[2].genesis_hash,
        );

        actix::spawn(
            test_nodes[1]
                .client
                .send(NetworkClientMessages::Transaction(unstake_transaction))
                .map(drop),
        );
        actix::spawn(
            test_nodes[0]
                .client
                .send(NetworkClientMessages::Transaction(stake_transaction))
                .map(drop),
        );

        let (done1, done2) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
        let (done1_copy1, done2_copy1) = (done1.clone(), done2.clone());
        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let test_nodes = test_nodes.clone();
                let test_node1 = test_nodes[0].clone();
                let (done1_copy2, done2_copy2) = (done1_copy1.clone(), done2_copy1.clone());
                actix::spawn(test_node1.client.send(Status { is_health_check: false }).then(
                    move |res| {
                        let expected = vec![
                            ValidatorInfo { account_id: "near.0".to_string(), is_slashed: false },
                            ValidatorInfo { account_id: "near.2".to_string(), is_slashed: false },
                        ];
                        let res = res.unwrap();
                        if res.is_err() {
                            return future::ready(());
                        }
                        if res.unwrap().validators == expected {
                            actix::spawn(
                                test_node1
                                    .view_client
                                    .send(Query::new(
                                        None,
                                        QueryRequest::ViewAccount {
                                            account_id: test_nodes[1].account_id.clone(),
                                        },
                                    ))
                                    .then(move |res| match res.unwrap().unwrap().unwrap().kind {
                                        QueryResponseKind::ViewAccount(result) => {
                                            if result.locked == 0 {
                                                done1_copy2.store(true, Ordering::SeqCst);
                                            }
                                            future::ready(())
                                        }
                                        _ => panic!("wrong return result"),
                                    }),
                            );
                            actix::spawn(
                                test_node1
                                    .view_client
                                    .send(Query::new(
                                        None,
                                        QueryRequest::ViewAccount {
                                            account_id: test_nodes[2].account_id.clone(),
                                        },
                                    ))
                                    .then(move |res| match res.unwrap().unwrap().unwrap().kind {
                                        QueryResponseKind::ViewAccount(result) => {
                                            if result.locked == TESTING_INIT_STAKE {
                                                done2_copy2.store(true, Ordering::SeqCst);
                                            }

                                            future::ready(())
                                        }
                                        _ => panic!("wrong return result"),
                                    }),
                            );
                        }

                        future::ready(())
                    },
                ));
                if done1_copy1.load(Ordering::SeqCst) && done2_copy1.load(Ordering::SeqCst) {
                    System::current().stop();
                }
            }),
            1000,
            60000,
        )
        .start();

        system.run().unwrap();
    });
}

#[test]
fn test_inflation() {
    heavy_test(|| {
        let system = System::new("NEAR");
        let num_nodes = 1;
        let dirs = (0..num_nodes)
            .map(|i| TempDir::new(&format!("stake_node_{}", i)).unwrap())
            .collect::<Vec<_>>();
        let epoch_length = 10;
        let test_nodes = init_test_staking(
            dirs.iter().map(|dir| dir.path()).collect::<Vec<_>>(),
            num_nodes,
            1,
            epoch_length,
            true,
        );
        let initial_total_supply = test_nodes[0].config.genesis_config.total_supply;
        let max_inflation_rate = test_nodes[0].config.genesis_config.max_inflation_rate;
        let num_blocks_per_year = test_nodes[0].config.genesis_config.num_blocks_per_year;

        let (done1, done2) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
        let (done1_copy1, done2_copy1) = (done1.clone(), done2.clone());
        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let (done1_copy2, done2_copy2) = (done1_copy1.clone(), done2_copy1.clone());
                actix::spawn(test_nodes[0].view_client.send(GetBlock::Best).then(move |res| {
                    let header_view = res.unwrap().unwrap().header;
                    if header_view.height >= 2 && header_view.height <= epoch_length {
                        if header_view.total_supply == initial_total_supply {
                            done1_copy2.store(true, Ordering::SeqCst);
                        }
                    }
                    future::ready(())
                }));
                actix::spawn(test_nodes[0].view_client.send(GetBlock::Best).then(move |res| {
                    let header_view = res.unwrap().unwrap().header;
                    if header_view.height > epoch_length && header_view.height < epoch_length * 2 {
                        let inflation = initial_total_supply
                            * max_inflation_rate as u128
                            * epoch_length as u128
                            / (100 * num_blocks_per_year as u128);
                        if header_view.total_supply == initial_total_supply + inflation {
                            done2_copy2.store(true, Ordering::SeqCst);
                        }
                    }
                    future::ready(())
                }));
                if done1_copy1.load(Ordering::SeqCst) && done2_copy1.load(Ordering::SeqCst) {
                    System::current().stop();
                }
            }),
            100,
            10000,
        )
        .start();

        system.run().unwrap();
    });
}
