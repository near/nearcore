//! Executes a single transaction or a list of transactions on a set of nodes.

use crate::{
    remote_node::{try_wait, wait, RemoteNode},
    stats::Stats,
    transactions_generator::{Generator, TransactionType},
};
use futures::{future, FutureExt, StreamExt, TryFutureExt};
use log::{debug, info, warn};
use std::{
    sync::{Arc, RwLock},
    thread,
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tokio::time::interval;

pub struct Executor {
    /// Nodes that can be used to generate nonces
    pub nodes: Vec<Arc<RwLock<RemoteNode>>>,
}

impl Executor {
    /// Deploys test contract to each account of each node and waits for it to be committed.
    #[allow(dead_code)]
    fn deploy_contract(nodes: &Vec<Arc<RwLock<RemoteNode>>>) {
        for n in nodes {
            // Create deploy contract transactions.
            let transactions = Generator::deploy_test_contract(n);
            let mut hashes = vec![];
            // Submit deploy contract transactions.
            for tx in transactions {
                let hash = wait(|| n.write().unwrap().add_transaction(tx.clone()));
                debug!("txn to deploy contract submitted: {}", &hash);
                hashes.push(hash);
            }
            // Wait for them to propagate.
            wait(|| {
                for h in &hashes {
                    try_wait(|| n.write().unwrap().transaction_committed(h))?;
                }
                Ok(())
            });
        }
    }

    pub fn spawn(
        nodes: Vec<Arc<RwLock<RemoteNode>>>,
        timeout: Option<Duration>,
        tps: u64,
        transaction_type: TransactionType,
    ) -> JoinHandle<()> {
        // Deploy the testing contract, if needed.
        if let TransactionType::Set | TransactionType::HeavyStorageBlock = transaction_type {
            info!("start deploying contracts");
            Executor::deploy_contract(&nodes);
            info!("finish deploying contracts");
        }
        let stats = Arc::new(RwLock::new(Stats::new()));
        thread::spawn(move || {
            tokio::spawn(future::lazy(move |_| {
                // Channels into which we can signal to send a transaction.
                let mut signal_tx = vec![];
                let all_account_ids: Vec<_> = nodes
                    .iter()
                    .map(|n| {
                        n.read()
                            .unwrap()
                            .signers
                            .iter()
                            .map(|s| s.account_id.clone())
                            .collect::<Vec<_>>()
                    })
                    .flatten()
                    .collect();

                for node in &nodes {
                    for (signer_ind, _) in node.read().unwrap().signers.iter().enumerate() {
                        let stats = stats.clone();
                        let node = node.clone();
                        let all_account_ids = all_account_ids.to_vec();
                        let (tx, rx) = tokio::sync::mpsc::channel(1024);
                        signal_tx.push(tx);
                        // Spawn a task that sends transactions only from the given account making
                        // sure the nonces are correct.
                        tokio::spawn(
                            rx.for_each(move |_| {
                                let stats = stats.clone();
                                let t = match transaction_type {
                                    TransactionType::SendMoney => {
                                        Generator::send_money(&node, signer_ind, &all_account_ids)
                                    }
                                    TransactionType::Set => Generator::call_set(&node, signer_ind),
                                    TransactionType::HeavyStorageBlock => {
                                        Generator::call_heavy_storage_blocks(&node, signer_ind)
                                    }
                                };
                                node.write().unwrap().add_transaction_committed(t.clone()).unwrap();
                                let f = { node.write().unwrap().add_transaction_async(t) };
                                let f = f
                                    .map_ok(|r| debug!("txn submitted: {}", r))
                                    .map_err(|e| warn!("error submitting txn: {}", e));
                                tokio::time::timeout(Duration::from_secs(1), f)
                                    .map_ok(move |_| {
                                        stats.read().unwrap().inc_out_tx();
                                    })
                                    .map(|_| ()) // Ignore errors.
                            })
                            .map(|_| ()),
                        );
                    }
                }

                // Spawn the task that sets the tps.
                let period = Duration::from_nanos((Duration::from_secs(1).as_nanos() as u64) / tps);
                let timeout = timeout.map(|t| Instant::now() + t);
                let task = interval(period)
                    .take_while(move |_| {
                        if let Some(t_limit) = timeout {
                            if t_limit <= Instant::now() {
                                // We hit timeout.
                                return future::ready(false);
                            }
                        }
                        future::ready(true)
                    })
                    .for_each(move |_| {
                        let ind = rand::random::<usize>() % signal_tx.len();
                        let mut tx = signal_tx[ind].clone();
                        async move { tx.send(()).await }.map(drop)
                    });

                let node = nodes[0].clone();
                stats.write().unwrap().measure_from(&*node.write().unwrap());
                tokio::spawn(
                    task.then(move |_| {
                        future::lazy(move |_| {
                            let mut stats = stats.write().unwrap();
                            stats.measure_to(&*node.write().unwrap());
                            stats.collect_transactions(&*node.write().unwrap());
                            println!("{}", stats);
                            Ok(())
                        })
                    })
                    .map_err(|_: ()| ()),
                );
            }));
        })
    }
}
