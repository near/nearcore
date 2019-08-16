use actix::{Addr, System};
use futures::future;
use futures::future::Future;
use near_client::test_utils::setup_mock_all_validators;
use near_client::{ClientActor, Query, ViewClientActor};
use near_network::{NetworkRequests, NetworkResponses, PeerInfo};
use near_primitives::rpc::QueryResponse::ViewAccount;
use near_primitives::test_utils::init_test_logger;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

/// Tests that the KeyValueRuntime properly sets balances in genesis and makes them queriable
#[test]
fn test_keyvalue_runtime_balances() {
    let validator_groups = 2;
    let successful_queries = Arc::new(AtomicUsize::new(0));
    init_test_logger();
    System::run(move || {
        let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
            Arc::new(RwLock::new(vec![]));

        let validators = vec![vec!["test1", "test2", "test3", "test4"]];
        let key_pairs =
            vec![PeerInfo::random(), PeerInfo::random(), PeerInfo::random(), PeerInfo::random()];

        *connectors.write().unwrap() = setup_mock_all_validators(
            validators.clone(),
            key_pairs.clone(),
            validator_groups,
            true,
            100,
            Arc::new(RwLock::new(move |_account_id: String, _msg: &NetworkRequests| {
                (NetworkResponses::NoResponse, true)
            })),
        );

        let connectors_ = connectors.write().unwrap();
        let flat_validators = validators.iter().flatten().collect::<Vec<_>>();
        for i in 0..4 {
            let expected = (1000 + i * 100) as u128;

            let successful_queries2 = successful_queries.clone();
            actix::spawn(
                connectors_[i]
                    .1
                    .send(Query { path: "account/".to_owned() + flat_validators[i], data: vec![] })
                    .then(move |res| {
                        let query_responce = res.unwrap().unwrap();
                        if let ViewAccount(view_account_result) = query_responce {
                            assert_eq!(view_account_result.amount, expected);
                            successful_queries2.fetch_add(1, Ordering::Relaxed);
                            if successful_queries2.load(Ordering::Relaxed) >= 4 {
                                System::current().stop();
                            }
                        }
                        future::result(Ok(()))
                    }),
            );
        }

        near_network::test_utils::wait_or_panic(5000);
    })
    .unwrap();
}

#[cfg(test)]
#[cfg(feature = "expensive_tests")]
mod tests {
    use actix::{Addr, MailboxError, System};
    use futures::future;
    use futures::future::Future;
    use near_chain::test_utils::account_id_to_shard_id;
    use near_client::test_utils::setup_mock_all_validators;
    use near_client::{ClientActor, Query, ViewClientActor};
    use near_network::{
        NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses, PeerInfo,
    };
    use near_primitives::rpc::QueryResponse;
    use near_primitives::rpc::QueryResponse::ViewAccount;
    use near_primitives::test_utils::init_test_logger;
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::types::AccountId;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};

    fn send_tx(
        num_validators: usize,
        connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>>,
        connector_ordinal: usize,
        from: AccountId,
        to: AccountId,
        amount: u128,
        nonce: u64,
    ) {
        let connectors1 = connectors.clone();
        actix::spawn(
            connectors.write().unwrap()[connector_ordinal]
                .0
                .send(NetworkClientMessages::Transaction(SignedTransaction::create_payment_tx(
                    from.clone(),
                    to.clone(),
                    amount,
                    nonce,
                )))
                .then(move |x| {
                    match x.unwrap() {
                        NetworkClientResponses::NoResponse => {
                            assert_eq!(num_validators, 24);
                            send_tx(
                                num_validators,
                                connectors1,
                                (connector_ordinal + 8) % num_validators,
                                from,
                                to,
                                amount,
                                nonce,
                            );
                        }
                        NetworkClientResponses::ValidTx => {
                            println!(
                                "Transaction was received by validator {:?}",
                                connector_ordinal
                            );
                        }
                        _ => assert!(false),
                    }
                    future::result(Ok(()))
                }),
        );
    }

    fn test_cross_shard_tx_callback(
        res: Result<Result<QueryResponse, String>, MailboxError>,
        account_id: AccountId,
        connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>>,
        iteration: Arc<AtomicUsize>,
        nonce: Arc<AtomicUsize>,
        validators: Vec<&'static str>,
        successful_queries: Arc<RwLock<HashSet<AccountId>>>,
        unsuccessful_queries: Arc<AtomicUsize>,
        balances: Arc<RwLock<Vec<u128>>>,
        observed_balances: Arc<RwLock<Vec<u128>>>,
        presumable_epoch: Arc<RwLock<usize>>,
        num_iters: usize,
    ) {
        let res = res.unwrap();

        let query_response = match res {
            Ok(query_response) => query_response,
            Err(e) => {
                println!("Query failed with {:?}", e);
                *presumable_epoch.write().unwrap() += 1;
                let connectors_ = connectors.write().unwrap();
                let connectors1 = connectors.clone();
                let iteration1 = iteration.clone();
                let nonce1 = nonce.clone();
                let validators1 = validators.clone();
                let successful_queries1 = successful_queries.clone();
                let unsuccessful_queries1 = unsuccessful_queries.clone();
                let balances1 = balances.clone();
                let observed_balances1 = observed_balances.clone();
                let presumable_epoch1 = presumable_epoch.clone();
                actix::spawn(
                    connectors_[account_id_to_shard_id(&account_id, 8) as usize
                        + (*presumable_epoch.read().unwrap() * 8) % 24]
                        .1
                        .send(Query { path: "account/".to_owned() + &account_id, data: vec![] })
                        .then(move |x| {
                            test_cross_shard_tx_callback(
                                x,
                                account_id,
                                connectors1,
                                iteration1,
                                nonce1,
                                validators1,
                                successful_queries1,
                                unsuccessful_queries1,
                                balances1,
                                observed_balances1,
                                presumable_epoch1,
                                num_iters,
                            );
                            future::result(Ok(()))
                        }),
                );
                return;
            }
        };

        if let ViewAccount(view_account_result) = query_response {
            let mut expected = 0;
            for i in 0..8 {
                if validators[i] == view_account_result.account_id {
                    expected = balances.read().unwrap()[i];
                    observed_balances.write().unwrap()[i] = view_account_result.amount;
                }
            }

            assert_eq!(account_id, view_account_result.account_id);

            if view_account_result.amount == expected {
                let mut successful_queries_local = successful_queries.write().unwrap();
                assert!(!successful_queries_local.contains(&account_id));
                successful_queries_local.insert(account_id.clone());
                if successful_queries_local.len() == 8 {
                    println!("Finished iteration {}", iteration.load(Ordering::Relaxed));

                    iteration.fetch_add(1, Ordering::Relaxed);
                    let iteration_local = iteration.load(Ordering::Relaxed);
                    if iteration_local > num_iters {
                        System::current().stop();
                    }

                    let from = iteration_local % 8;
                    let to = (iteration_local / 8) % 8;
                    let amount = (5 + iteration_local) as u128;
                    let next_nonce = nonce.fetch_add(1, Ordering::Relaxed);

                    send_tx(
                        validators.len(),
                        connectors.clone(),
                        account_id_to_shard_id(&validators[from].to_string(), 8) as usize,
                        validators[from].to_string(),
                        validators[to].to_string(),
                        amount,
                        next_nonce as u64,
                    );

                    let connectors_ = connectors.write().unwrap();

                    let mut balances_local = balances.write().unwrap();
                    balances_local[from] -= amount;
                    balances_local[to] += amount;

                    successful_queries_local.clear();
                    unsuccessful_queries.store(0, Ordering::Relaxed);

                    // Send the initial balance queries for the iteration
                    for i in 0..8 {
                        let connectors1 = connectors.clone();
                        let iteration1 = iteration.clone();
                        let nonce1 = nonce.clone();
                        let validators1 = validators.clone();
                        let successful_queries1 = successful_queries.clone();
                        let unsuccessful_queries1 = unsuccessful_queries.clone();
                        let balances1 = balances.clone();
                        let observed_balances1 = observed_balances.clone();
                        let presumable_epoch1 = presumable_epoch.clone();
                        let account_id1 = validators[i].to_string();
                        actix::spawn(
                            connectors_[account_id_to_shard_id(&validators[i].to_string(), 8)
                                as usize
                                + (*presumable_epoch.read().unwrap() * 8) % 24]
                                .1
                                .send(Query {
                                    path: "account/".to_owned() + validators[i].clone(),
                                    data: vec![],
                                })
                                .then(move |x| {
                                    test_cross_shard_tx_callback(
                                        x,
                                        account_id1,
                                        connectors1,
                                        iteration1,
                                        nonce1,
                                        validators1,
                                        successful_queries1,
                                        unsuccessful_queries1,
                                        balances1,
                                        observed_balances1,
                                        presumable_epoch1,
                                        num_iters,
                                    );
                                    future::result(Ok(()))
                                }),
                        );
                    }
                }
            } else {
                // The balance is not correct, optionally trace, and resend the query
                unsuccessful_queries.fetch_add(1, Ordering::Relaxed);
                if unsuccessful_queries.load(Ordering::Relaxed) % 100 == 0 {
                    println!("Waiting for balances");
                    print!("Expected: ");
                    for i in 0..8 {
                        print!("{} ", balances.read().unwrap()[i]);
                    }
                    println!();
                    print!("Received: ");
                    for i in 0..8 {
                        print!("{} ", observed_balances.read().unwrap()[i]);
                    }
                    println!();
                }

                let connectors_ = connectors.write().unwrap();
                let connectors1 = connectors.clone();
                let presumable_epoch1 = presumable_epoch.clone();
                actix::spawn(
                    connectors_[account_id_to_shard_id(&account_id, 8) as usize
                        + (*presumable_epoch.read().unwrap() * 8) % 24]
                        .1
                        .send(Query { path: "account/".to_owned() + &account_id, data: vec![] })
                        .then(move |x| {
                            test_cross_shard_tx_callback(
                                x,
                                account_id,
                                connectors1,
                                iteration,
                                nonce,
                                validators,
                                successful_queries,
                                unsuccessful_queries,
                                balances,
                                observed_balances,
                                presumable_epoch1,
                                num_iters,
                            );
                            future::result(Ok(()))
                        }),
                );
            }
        }
    }

    fn test_cross_shard_tx_common(rotate_validators: bool) {
        let validator_groups = 4;
        let num_iters = 64;
        init_test_logger();
        System::run(move || {
            let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
                Arc::new(RwLock::new(vec![]));

            let validators = if rotate_validators {
                vec![
                    vec![
                        "test1.1", "test1.2", "test1.3", "test1.4", "test1.5", "test1.6",
                        "test1.7", "test1.8",
                    ],
                    vec![
                        "test2.1", "test2.2", "test2.3", "test2.4", "test2.5", "test2.6",
                        "test2.7", "test2.8",
                    ],
                    vec![
                        "test3.1", "test3.2", "test3.3", "test3.4", "test3.5", "test3.6",
                        "test3.7", "test3.8",
                    ],
                ]
            } else {
                vec![vec![
                    "test1.1", "test1.2", "test1.3", "test1.4", "test1.5", "test1.6", "test1.7",
                    "test1.8",
                ]]
            };
            let key_pairs = (0..32).map(|_| PeerInfo::random()).collect::<Vec<_>>();
            let balances = Arc::new(RwLock::new(vec![]));
            let observed_balances = Arc::new(RwLock::new(vec![]));
            let presumable_epoch = Arc::new(RwLock::new(0usize));

            let mut balances_local = balances.write().unwrap();
            let mut observed_balances_local = observed_balances.write().unwrap();
            for i in 0..8 {
                balances_local.push(1000 + 100 * i);
                observed_balances_local.push(0);
            }

            *connectors.write().unwrap() = setup_mock_all_validators(
                validators.clone(),
                key_pairs.clone(),
                validator_groups,
                true,
                if rotate_validators { 150 } else { 50 },
                Arc::new(RwLock::new(move |_account_id: String, _msg: &NetworkRequests| {
                    (NetworkResponses::NoResponse, true)
                })),
            );

            let connectors_ = connectors.write().unwrap();
            let iteration = Arc::new(AtomicUsize::new(0));
            let nonce = Arc::new(AtomicUsize::new(1));
            let successful_queries = Arc::new(RwLock::new(HashSet::new()));
            let unsuccessful_queries = Arc::new(AtomicUsize::new(0));
            let flat_validators = validators.iter().flatten().map(|x| *x).collect::<Vec<_>>();

            for i in 0..8 {
                let connectors1 = connectors.clone();
                let iteration1 = iteration.clone();
                let nonce1 = nonce.clone();
                let flat_validators1 = flat_validators.clone();
                let successful_queries1 = successful_queries.clone();
                let unsuccessful_queries1 = unsuccessful_queries.clone();
                let balances1 = balances.clone();
                let observed_balances1 = observed_balances.clone();
                let presumable_epoch1 = presumable_epoch.clone();
                let account_id1 = flat_validators[i].clone();
                actix::spawn(
                    connectors_[i + *presumable_epoch.read().unwrap() * 8]
                        .1
                        .send(Query {
                            path: "account/".to_owned() + flat_validators[i].clone(),
                            data: vec![],
                        })
                        .then(move |x| {
                            test_cross_shard_tx_callback(
                                x,
                                account_id1.to_string(),
                                connectors1,
                                iteration1,
                                nonce1,
                                flat_validators1,
                                successful_queries1,
                                unsuccessful_queries1,
                                balances1,
                                observed_balances1,
                                presumable_epoch1,
                                num_iters,
                            );
                            future::result(Ok(()))
                        }),
                );
            }

            // On X1 it takes ~1m 15s
            near_network::test_utils::wait_or_panic(600000);
        })
        .unwrap();
    }

    #[test]
    fn test_cross_shard_tx() {
        test_cross_shard_tx_common(false);
    }

    #[test]
    fn test_cross_shard_tx_with_validator_rotation() {
        test_cross_shard_tx_common(true);
    }
}
