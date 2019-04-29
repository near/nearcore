//! Executes a single transaction or a list of transactions on a set of nodes.

use crate::remote_node::RemoteNode;
use crate::sampler::sample_two;
use crate::transactions_generator::Generator;
use futures::future::Future;
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
    /// Spawn executor in a separate thread.
    /// Args:
    /// * `nodes`: nodes to run on;
    /// * `timeout`: if specified will terminate after the given time;
    /// transactions;
    /// * `tps`: transactions-per-second;
    pub fn spawn(
        nodes: Vec<Arc<RwLock<RemoteNode>>>,
        timeout: Option<Duration>,
        tps: u64,
    ) -> JoinHandle<()> {
        // Schedule submission of transactions with random delays and given tps.
        // We use tokio because it allows to spawn a large number of tasks that will be resolved
        // some time in the future.
        thread::spawn(move || {
            let interval = Duration::from_nanos((Duration::from_secs(1).as_nanos() as u64) / tps);
            let timeout = timeout.map(|t| Instant::now() + t);
            tokio::run(
                Interval::new_interval(interval)
                    .take_while(move |_| {
                        if let Some(t_limit) = timeout {
                            if t_limit > Instant::now() {
                                // We hit timeout.
                                return Ok(false);
                            }
                        }
                        Ok(true)
                    })
                    .map_err(|_| ()) // Timer errors are irrelevant.
                    .for_each(move |_| {
                        let (node_from, node_to) = sample_two(&nodes);
                        let node_from = node_from.clone();
                        let node_to = node_to.clone();
                        tokio::spawn(
                            futures::future::ok(())
                                .map_err(|_: ()| unimplemented!())
                                .and_then(move |_| {
                                    let t = Generator::send_money(&node_from, &node_to);
                                    let f = { node_from.write().unwrap().add_transaction(t) };
                                    f.timeout(Duration::from_secs(1))
                                        .map(|_| ())
                                        .map_err(|err| format!("Error sending transaction {}", err))
                                })
                                .map_err(|_| ()),
                        );
                        Ok(())
                    })
                    .map(|_| ())
                    .map_err(|_| ()),
            );
        })
    }
}
