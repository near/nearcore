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
    let validators_per_shard = 2;
    let successful_queries = Arc::new(AtomicUsize::new(0));
    init_test_logger();
    System::run(move || {
        let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
            Arc::new(RwLock::new(vec![]));

        let validators = vec!["test1", "test2", "test3", "test4"];
        let key_pairs =
            vec![PeerInfo::random(), PeerInfo::random(), PeerInfo::random(), PeerInfo::random()];

        *connectors.write().unwrap() = setup_mock_all_validators(
            validators.clone(),
            key_pairs.clone(),
            validators_per_shard,
            true,
            100,
            Arc::new(RwLock::new(move |_account_id: String, _msg: &NetworkRequests| {
                (NetworkResponses::NoResponse, true)
            })),
        );

        let connectors_ = connectors.write().unwrap();
        for i in 0..4 {
            let expected = (1000 + i * 100) as u128;

            let successful_queries2 = successful_queries.clone();
            actix::spawn(
                connectors_[i]
                    .1
                    .send(Query { path: "account/".to_owned() + validators[i], data: vec![] })
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
    use near_network::{NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo};
    use near_primitives::rpc::QueryResponse;
    use near_primitives::rpc::QueryResponse::ViewAccount;
    use near_primitives::test_utils::init_test_logger;
    use near_primitives::transaction::SignedTransaction;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};

    fn test_cross_shard_tx_callback(
        res: Result<Result<QueryResponse, String>, MailboxError>,
        connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>>,
        iteration: Arc<AtomicUsize>,
        validators: Vec<&'static str>,
        successful_queries: Arc<AtomicUsize>,
        unsuccessful_queries: Arc<AtomicUsize>,
        balances: Arc<RwLock<Vec<u128>>>,
        observed_balances: Arc<RwLock<Vec<u128>>>,
        num_iters: usize,
    ) {
        let query_responce = res.unwrap().unwrap();
        if let ViewAccount(view_account_result) = query_responce {
            let mut expected = 0;
            let mut account_id = "".to_owned();
            for i in 0..8 {
                if validators[i] == view_account_result.account_id {
                    expected = balances.read().unwrap()[i];
                    account_id = view_account_result.account_id.clone();
                    observed_balances.write().unwrap()[i] = view_account_result.amount;
                }
            }
            if view_account_result.amount == expected {
                successful_queries.fetch_add(1, Ordering::Relaxed);
                if successful_queries.load(Ordering::Relaxed) >= 8 {
                    println!("Finished iteration {}", iteration.load(Ordering::Relaxed));

                    iteration.fetch_add(1, Ordering::Relaxed);
                    let iteration_local = iteration.load(Ordering::Relaxed);
                    if iteration_local > num_iters {
                        System::current().stop();
                    }

                    let connectors_ = connectors.write().unwrap();

                    let from = iteration_local % 8;
                    let to = (iteration_local / 8) % 8;
                    let amount = (5 + iteration_local) as u128;
                    connectors_[account_id_to_shard_id(&validators[from].to_string(), 8) as usize]
                        .0
                        .do_send(NetworkClientMessages::Transaction(
                            SignedTransaction::create_payment_tx(
                                validators[from].to_string(),
                                validators[to].to_string(),
                                amount,
                            ),
                        ));
                    let mut balances_local = balances.write().unwrap();
                    balances_local[from] -= amount;
                    balances_local[to] += amount;

                    successful_queries.store(0, Ordering::Relaxed);
                    unsuccessful_queries.store(0, Ordering::Relaxed);

                    // Send the initial balance queries for the iteration
                    for i in 0..8 {
                        let connectors1 = connectors.clone();
                        let iteration1 = iteration.clone();
                        let validators1 = validators.clone();
                        let successful_queries1 = successful_queries.clone();
                        let unsuccessful_queries1 = unsuccessful_queries.clone();
                        let balances1 = balances.clone();
                        let observed_balances1 = observed_balances.clone();
                        actix::spawn(
                            connectors_
                                [account_id_to_shard_id(&validators[i].to_string(), 8) as usize]
                                .1
                                .send(Query {
                                    path: "account/".to_owned() + validators[i].clone(),
                                    data: vec![],
                                })
                                .then(move |x| {
                                    test_cross_shard_tx_callback(
                                        x,
                                        connectors1,
                                        iteration1,
                                        validators1,
                                        successful_queries1,
                                        unsuccessful_queries1,
                                        balances1,
                                        observed_balances1,
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
                actix::spawn(
                    connectors_[account_id_to_shard_id(&account_id, 8) as usize]
                        .1
                        .send(Query { path: "account/".to_owned() + &account_id, data: vec![] })
                        .then(move |x| {
                            test_cross_shard_tx_callback(
                                x,
                                connectors1,
                                iteration,
                                validators,
                                successful_queries,
                                unsuccessful_queries,
                                balances,
                                observed_balances,
                                num_iters,
                            );
                            future::result(Ok(()))
                        }),
                );
            }
        }
    }

    #[test]
    fn test_cross_shard_tx() {
        let validators_per_shard = 2;
        let num_iters = 64;
        init_test_logger();
        System::run(move || {
            let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
                Arc::new(RwLock::new(vec![]));

            let validators =
                vec!["test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8"];
            let key_pairs = (0..8).map(|_| PeerInfo::random()).collect::<Vec<_>>();
            let balances = Arc::new(RwLock::new(vec![]));
            let observed_balances = Arc::new(RwLock::new(vec![]));

            let mut balances_local = balances.write().unwrap();
            let mut observed_balances_local = observed_balances.write().unwrap();
            for i in 0..8 {
                balances_local.push(1000 + 100 * i);
                observed_balances_local.push(0);
            }

            *connectors.write().unwrap() = setup_mock_all_validators(
                validators.clone(),
                key_pairs.clone(),
                validators_per_shard,
                true,
                20,
                Arc::new(RwLock::new(move |_account_id: String, _msg: &NetworkRequests| {
                    (NetworkResponses::NoResponse, true)
                })),
            );

            let connectors_ = connectors.write().unwrap();
            let iteration = Arc::new(AtomicUsize::new(0));
            let successful_queries = Arc::new(AtomicUsize::new(0));
            let unsuccessful_queries = Arc::new(AtomicUsize::new(0));

            for i in 0..8 {
                let connectors1 = connectors.clone();
                let iteration1 = iteration.clone();
                let validators1 = validators.clone();
                let successful_queries1 = successful_queries.clone();
                let unsuccessful_queries1 = unsuccessful_queries.clone();
                let balances1 = balances.clone();
                let observed_balances1 = observed_balances.clone();
                actix::spawn(
                    connectors_[i]
                        .1
                        .send(Query {
                            path: "account/".to_owned() + validators[i].clone(),
                            data: vec![],
                        })
                        .then(move |x| {
                            test_cross_shard_tx_callback(
                                x,
                                connectors1,
                                iteration1,
                                validators1,
                                successful_queries1,
                                unsuccessful_queries1,
                                balances1,
                                observed_balances1,
                                num_iters,
                            );
                            future::result(Ok(()))
                        }),
                );
            }

            // On X1 it takes ~1m 15s
            near_network::test_utils::wait_or_panic(15000);
        })
        .unwrap();
    }
}
