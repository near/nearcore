#![allow(unused_imports)]

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use actix::{Addr, MailboxError, System};
use futures::{future, FutureExt};

use near_actix_test_utils::run_actix;
use near_chain::test_utils::{account_id_to_shard_id, ValidatorSchedule};
use near_crypto::{InMemorySigner, KeyType};
use near_network::types::PeerInfo;
use near_network::types::{
    NetworkResponses, PeerManagerMessageRequest, PeerManagerMessageResponse,
};
use near_o11y::testonly::init_integration_logger;
use near_o11y::WithSpanContextExt;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockReference};
use near_primitives::views::QueryResponseKind::ViewAccount;
use near_primitives::views::{QueryRequest, QueryResponse};

use crate::adapter::{ProcessTxRequest, ProcessTxResponse};
use crate::test_utils::{setup_mock_all_validators, ActorHandlesForTesting, BlockStats};
use crate::{ClientActor, Query, ViewClientActor};

/// Tests that the KeyValueRuntime properly sets balances in genesis and makes them queriable
#[test]
fn test_keyvalue_runtime_balances() {
    let successful_queries = Arc::new(AtomicUsize::new(0));
    init_integration_logger();
    run_actix(async move {
        let connectors: Arc<RwLock<Vec<ActorHandlesForTesting>>> = Arc::new(RwLock::new(vec![]));

        let vs = ValidatorSchedule::new()
            .num_shards(4)
            .block_producers_per_epoch(vec![vec![
                "test1".parse().unwrap(),
                "test2".parse().unwrap(),
                "test3".parse().unwrap(),
                "test4".parse().unwrap(),
            ]])
            .validator_groups(2);
        let validators = vs.all_block_producers().cloned().collect::<Vec<_>>();
        let key_pairs =
            vec![PeerInfo::random(), PeerInfo::random(), PeerInfo::random(), PeerInfo::random()];
        let (_, conn, _) = setup_mock_all_validators(
            vs,
            key_pairs,
            true,
            100,
            false,
            false,
            5,
            false,
            vec![false; validators.len()],
            vec![true; validators.len()],
            false,
            Box::new(move |_, _account_id: _, _msg: &PeerManagerMessageRequest| {
                (NetworkResponses::NoResponse.into(), true)
            }),
        );
        *connectors.write().unwrap() = conn;

        let connectors_ = connectors.write().unwrap();
        for i in 0..4 {
            let expected = (1000 + i * 100) as u128;

            let successful_queries2 = successful_queries.clone();
            let actor = connectors_[i].view_client_actor.send(
                Query::new(
                    BlockReference::latest(),
                    QueryRequest::ViewAccount { account_id: validators[i].clone() },
                )
                .with_span_context(),
            );
            let actor = actor.then(move |res| {
                let query_response = res.unwrap().unwrap();
                if let ViewAccount(view_account_result) = query_response.kind {
                    assert_eq!(view_account_result.amount, expected);
                    successful_queries2.fetch_add(1, Ordering::Relaxed);
                    if successful_queries2.load(Ordering::Relaxed) >= 4 {
                        System::current().stop();
                    }
                }
                future::ready(())
            });
            actix::spawn(actor);
        }

        near_network::test_utils::wait_or_panic(5000);
    });
}

fn send_tx(
    num_validators: usize,
    connectors: Arc<RwLock<Vec<ActorHandlesForTesting>>>,
    connector_ordinal: usize,
    from: AccountId,
    to: AccountId,
    amount: u128,
    nonce: u64,
    block_hash: CryptoHash,
) {
    let connectors1 = connectors.clone();
    let signer = InMemorySigner::from_seed(from.clone(), KeyType::ED25519, from.as_ref());
    actix::spawn(
        connectors.write().unwrap()[connector_ordinal]
            .client_actor
            .send(
                ProcessTxRequest {
                    transaction: SignedTransaction::send_money(
                        nonce,
                        from.clone(),
                        to.clone(),
                        &signer,
                        amount,
                        block_hash,
                    ),
                    is_forwarded: false,
                    check_only: false,
                }
                .with_span_context(),
            )
            .then(move |x| {
                match x.unwrap() {
                    ProcessTxResponse::NoResponse | ProcessTxResponse::RequestRouted => {
                        assert_eq!(num_validators, 24);
                        send_tx(
                            num_validators,
                            connectors1,
                            (connector_ordinal + 8) % num_validators,
                            from,
                            to,
                            amount,
                            nonce,
                            block_hash,
                        );
                    }
                    ProcessTxResponse::ValidTx => {
                        println!("Transaction was received by validator {:?}", connector_ordinal);
                    }
                    other @ _ => {
                        println!(
                            "Transaction was rejected with an unexpected outcome: {:?}",
                            other
                        );
                        assert!(false)
                    }
                }
                future::ready(())
            }),
    );
}

fn test_cross_shard_tx_callback(
    res: Result<Result<QueryResponse, crate::QueryError>, MailboxError>,
    account_id: AccountId,
    connectors: Arc<RwLock<Vec<ActorHandlesForTesting>>>,
    iteration: Arc<AtomicUsize>,
    nonce: Arc<AtomicUsize>,
    validators: Vec<AccountId>,
    successful_queries: Arc<RwLock<HashSet<AccountId>>>,
    unsuccessful_queries: Arc<AtomicUsize>,
    balances: Arc<RwLock<Vec<u128>>>,
    observed_balances: Arc<RwLock<Vec<u128>>>,
    presumable_epoch: Arc<RwLock<usize>>,
    num_iters: usize,
    block_hash: CryptoHash,
    block_stats: Arc<RwLock<BlockStats>>,
    min_ratio: Option<f64>,
    max_ratio: Option<f64>,
) {
    let res = res.unwrap();

    let query_response = match res {
        Ok(query_response) => query_response,
        Err(e) => {
            println!("Query failed with {:?}", e);
            *presumable_epoch.write().unwrap() += 1;
            let connectors_ = connectors.write().unwrap();
            let connectors1 = connectors.clone();
            let iteration1 = iteration;
            let nonce1 = nonce;
            let validators1 = validators;
            let successful_queries1 = successful_queries;
            let unsuccessful_queries1 = unsuccessful_queries;
            let balances1 = balances;
            let observed_balances1 = observed_balances;
            let presumable_epoch1 = presumable_epoch.clone();
            let actor = &connectors_[account_id_to_shard_id(&account_id, 8) as usize
                + (*presumable_epoch.read().unwrap() * 8) % 24]
                .view_client_actor;
            let actor = actor.send(
                Query::new(
                    BlockReference::latest(),
                    QueryRequest::ViewAccount { account_id: account_id.clone() },
                )
                .with_span_context(),
            );
            let actor = actor.then(move |x| {
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
                    block_hash,
                    block_stats,
                    min_ratio,
                    max_ratio,
                );
                future::ready(())
            });
            actix::spawn(actor);
            return;
        }
    };

    if let ViewAccount(view_account_result) = query_response.kind {
        let mut expected = 0;
        for i in 0..8 {
            if validators[i] == account_id {
                expected = balances.read().unwrap()[i];
                observed_balances.write().unwrap()[i] = view_account_result.amount;
            }
        }

        if view_account_result.amount == expected {
            let mut successful_queries_local = successful_queries.write().unwrap();
            assert!(!successful_queries_local.contains(&account_id));
            successful_queries_local.insert(account_id);
            if successful_queries_local.len() == 8 {
                println!("Finished iteration {}", iteration.load(Ordering::Relaxed));

                iteration.fetch_add(1, Ordering::Relaxed);
                let iteration_local = iteration.load(Ordering::Relaxed);
                if iteration_local > num_iters {
                    (&mut *block_stats.write().unwrap()).check_stats(true);
                    (&mut *block_stats.write().unwrap()).check_block_ratio(min_ratio, max_ratio);
                    System::current().stop();
                }

                let from = iteration_local % 8;
                let to = (iteration_local / 8) % 8;
                let amount = (5 + iteration_local) as u128;
                let next_nonce = nonce.fetch_add(1, Ordering::Relaxed);

                send_tx(
                    validators.len(),
                    connectors.clone(),
                    account_id_to_shard_id(&validators[from], 8) as usize,
                    validators[from].clone(),
                    validators[to].clone(),
                    amount,
                    next_nonce as u64,
                    block_hash,
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
                    let account_id1 = validators[i].clone();
                    let block_stats1 = block_stats.clone();
                    let actor = &connectors_[account_id_to_shard_id(&validators[i], 8) as usize
                        + (*presumable_epoch.read().unwrap() * 8) % 24]
                        .view_client_actor;
                    let actor = actor.send(
                        Query::new(
                            BlockReference::latest(),
                            QueryRequest::ViewAccount { account_id: validators[i].clone() },
                        )
                        .with_span_context(),
                    );
                    let actor = actor.then(move |x| {
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
                            block_hash,
                            block_stats1,
                            min_ratio,
                            max_ratio,
                        );
                        future::ready(())
                    });
                    actix::spawn(actor);
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
            let actor = &connectors_[account_id_to_shard_id(&account_id, 8) as usize
                + (*presumable_epoch.read().unwrap() * 8) % 24]
                .view_client_actor;
            let actor = actor.send(
                Query::new(
                    BlockReference::latest(),
                    QueryRequest::ViewAccount { account_id: account_id.clone() },
                )
                .with_span_context(),
            );
            let actor = actor.then(move |x| {
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
                    block_hash,
                    block_stats,
                    min_ratio,
                    max_ratio,
                );
                future::ready(())
            });
            actix::spawn(actor);
        }
    }
}

/// The basic flow of the test spins up validators, and starts sending to them
/// several (at most 64) cross-shard transactions. i-th transaction sends money from
/// validator i/8 to validator i%8. What makes the test good is that due to very low
/// block production time, it creates lots of forks, delaying both the transaction
/// acceptance, and the receipt delivery.
///
/// It submits txs one at a time, and waits for their completion before sending the
/// next. Whenever the transaction completed, it traces "Finished iteration 1" (with
/// the ordinal increasing). Given the test takes a while, checking how far below
/// the last some message is a good way to early tell that the test has stalled.
/// E.g. if the last message is not in the last 15% of the output, it is likely that
/// the test will not make further progress, depending on the mode (with validator
/// rotation some iterations are way longer than other).
fn test_cross_shard_tx_common(
    num_iters: usize,
    rotate_validators: bool,
    drop_chunks: bool,
    test_doomslug: bool,
    block_production_time: u64,
    min_ratio: Option<f64>,
    max_ratio: Option<f64>,
) {
    init_integration_logger();
    run_actix(async move {
        let connectors: Arc<RwLock<Vec<ActorHandlesForTesting>>> = Arc::new(RwLock::new(vec![]));

        let vs = ValidatorSchedule::new()
            .num_shards(8)
            .block_producers_per_epoch(
                if rotate_validators {
                    [
                        [
                            "test1.1", "test1.2", "test1.3", "test1.4", "test1.5", "test1.6",
                            "test1.7", "test1.8",
                        ],
                        [
                            "test2.1", "test2.2", "test2.3", "test2.4", "test2.5", "test2.6",
                            "test2.7", "test2.8",
                        ],
                        [
                            "test3.1", "test3.2", "test3.3", "test3.4", "test3.5", "test3.6",
                            "test3.7", "test3.8",
                        ],
                    ]
                    .iter()
                } else {
                    [[
                        "test1.1", "test1.2", "test1.3", "test1.4", "test1.5", "test1.6",
                        "test1.7", "test1.8",
                    ]]
                    .iter()
                }
                .map(|l| l.iter().map(|account_id| account_id.parse().unwrap()).collect())
                .collect(),
            )
            .validator_groups(4);
        let validators = vs.all_block_producers().cloned().collect::<Vec<_>>();

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

        let (genesis_block, conn, block_stats) = setup_mock_all_validators(
            vs,
            key_pairs,
            true,
            block_production_time,
            drop_chunks,
            !test_doomslug,
            20,
            test_doomslug,
            vec![true; validators.len()],
            vec![false; validators.len()],
            true,
            Box::new(move |_, _account_id: _, _msg: &PeerManagerMessageRequest| {
                (PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse), true)
            }),
        );
        *connectors.write().unwrap() = conn;
        let block_hash = *genesis_block.hash();

        let connectors_ = connectors.write().unwrap();
        let iteration = Arc::new(AtomicUsize::new(0));
        let nonce = Arc::new(AtomicUsize::new(1));
        let successful_queries = Arc::new(RwLock::new(HashSet::new()));
        let unsuccessful_queries = Arc::new(AtomicUsize::new(0));

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
            let account_id1 = validators[i].clone();
            let block_stats1 = block_stats.clone();
            let actor = &connectors_[account_id_to_shard_id(&validators[i], 8) as usize
                + *presumable_epoch.read().unwrap() * 8]
                .view_client_actor;
            let actor = actor.send(
                Query::new(
                    BlockReference::latest(),
                    QueryRequest::ViewAccount { account_id: validators[i].clone() },
                )
                .with_span_context(),
            );
            let actor = actor.then(move |x| {
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
                    block_hash,
                    block_stats1,
                    min_ratio,
                    max_ratio,
                );
                future::ready(())
            });
            actix::spawn(actor);
        }

        near_network::test_utils::wait_or_panic(if rotate_validators {
            1000 * 60 * 80
        } else {
            1000 * 60 * 45
        });
    });
}

/// Doesn't drop chunks, disabled doomslug, no validator rotation (each epoch
/// has the same set of validators).
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_cross_shard_tx() {
    test_cross_shard_tx_common(64, false, false, false, 70, Some(2.3), None);
}

/// Same as above, but doomslug is enabled.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_cross_shard_tx_doomslug() {
    test_cross_shard_tx_common(64, false, false, true, 200, None, Some(1.5));
}

/// Same as the first one but the chunks are sometimes dropped.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_cross_shard_tx_drop_chunks() {
    test_cross_shard_tx_common(64, false, true, false, 250, None, None);
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_cross_shard_tx_8_iterations() {
    test_cross_shard_tx_common(8, false, false, false, 200, Some(2.4), None);
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_cross_shard_tx_8_iterations_drop_chunks() {
    test_cross_shard_tx_common(8, false, true, false, 200, Some(2.4), None);
}

/// The next two tests are the same as the first one, but with validator
/// rotation. The two versions of the test have slightly different block
/// production times, and different number of iterations we expect to finish in
/// the allocated time. (the one with lower block production time is expected to
/// finish fewer because it has higher forkfulness).
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_cross_shard_tx_with_validator_rotation_1() {
    test_cross_shard_tx_common(8, true, false, false, 220, Some(2.4), None);
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_cross_shard_tx_with_validator_rotation_2() {
    test_cross_shard_tx_common(24, true, false, false, 400, Some(2.4), None);
}
