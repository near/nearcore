//! Executes a single transaction or a list of transactions on a set of nodes.

use crate::remote_node::RemoteNode;
use crate::stats::Stats;
use crate::transactions_generator::Generator;
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
    pub fn spawn(
        nodes: Vec<Arc<RwLock<RemoteNode>>>,
        timeout: Option<Duration>,
        tps: u64,
    ) -> JoinHandle<()> {
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
                                    let t =
                                        Generator::send_money(&node, signer_ind, &all_account_ids);
                                    let f = { node.write().unwrap().add_transaction_async(t) };
                                    f.map_err(|_| ())
                                        .timeout(Duration::from_secs(1))
                                        .map(move |_| {
                                            stats.read().unwrap().inc_out_tx();
                                        })
                                        .map_err(|_| ())
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
