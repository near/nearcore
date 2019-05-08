//! Executes a single transaction or a list of transactions on a set of nodes.

use crate::remote_node::{try_wait, wait, RemoteNode};
use crate::stats::Stats;
use crate::transactions_generator::{Generator, TransactionType};
use futures::future::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use std::sync::{Arc, RwLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::timer::Interval;
use tokio::util::FutureExt;

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
                hashes.push(wait(|| n.write().unwrap().add_transaction(tx.clone())));
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
            //            Executor::deploy_contract(&nodes);
        }
        let stats = Arc::new(RwLock::new(Stats::new()));
        thread::spawn(move || {
            tokio::run(futures::lazy(move || {
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
                            rx.map_err(|_| ())
                                .for_each(move |_| {
                                    let stats = stats.clone();
                                    let t = match transaction_type {
                                        TransactionType::SendMoney => Generator::send_money(
                                            &node,
                                            signer_ind,
                                            &all_account_ids,
                                        ),
                                        TransactionType::Set => {
                                            Generator::call_set(&node, signer_ind)
                                        }
                                        TransactionType::HeavyStorageBlock => {
                                            Generator::call_heavy_storage_blocks(&node, signer_ind)
                                        }
                                    };
                                    let f = { node.write().unwrap().add_transaction_async(t) };
                                    f.map_err(|_| ())
                                        .timeout(Duration::from_secs(1))
                                        .map(move |_| {
                                            stats.read().unwrap().inc_out_tx();
                                        })
                                        .or_else(|_| Ok(())) // Ignore errors.
                                        .map_err(|_: ()| ())
                                })
                                .map(|_| ())
                                .map_err(|_| ()),
                        );
                    }
                }

                // Spawn the task that sets the tps.
                let interval =
                    Duration::from_nanos((Duration::from_secs(1).as_nanos() as u64) / tps);
                let timeout = timeout.map(|t| Instant::now() + t);
                let task = Interval::new_interval(interval)
                    .take_while(move |_| {
                        if let Some(t_limit) = timeout {
                            if t_limit <= Instant::now() {
                                // We hit timeout.
                                return Ok(false);
                            }
                        }
                        Ok(true)
                    })
                    .map_err(|_| ())
                    .for_each(move |_| {
                        let ind = rand::random::<usize>() % signal_tx.len();
                        let tx = signal_tx[ind].clone();
                        tx.send(()).map(|_| ()).map_err(|_| ())
                    })
                    .map(|_| ())
                    .map_err(|_| ());

                let node = nodes[0].clone();
                stats.write().unwrap().measure_from(&*node.write().unwrap());
                tokio::spawn(
                    task.then(move |_| {
                        futures::future::lazy(move || {
                            let mut stats = stats.write().unwrap();
                            stats.measure_to(&*node.write().unwrap());
                            stats.collect_transactions(&*node.write().unwrap());
                            println!("{}", stats);
                            Ok(())
                        })
                    })
                    .map_err(|_: ()| ()),
                );
                Ok(())
            }));
        })
    }
}
