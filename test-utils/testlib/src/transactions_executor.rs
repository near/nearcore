//! Executes a single transaction or a list of transactions on a set of nodes.

use crate::node::Node;
use crate::sampler::sample_one;
use crate::transactions_generator::{Generator, TransactionType};
use futures::future::Future;
use futures::stream::Stream;
use futures::sync::mpsc::{channel, Sender};
use primitives::transaction::SignedTransaction;
use primitives::types::AccountId;
use rand::distributions::{Distribution, Exp};
use std::collections::vec_deque::VecDeque;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::timer::{Delay, Interval};
use tokio::util::FutureExt;

/// How the messages should be sent to the nodes.
pub enum TrafficType {
    /// Messages are sent one after another.
    Regular,
    /// Submit transactions with random delay, following exponential distribution, which is commonly
    /// used to imitate network lag.
    ImitateLag {
        /// Mean latency.
        mean_delay: Duration,
    },
}

pub struct Executor {
    /// Nodes that can be used to generate nonces
    pub nodes: Vec<Arc<RwLock<dyn Node>>>,
}

impl Executor {
    /// Spawn executor in a separate thread.
    /// Args:
    /// * `nodes`: nodes to run on;
    /// * `transaction_type`: type of transaction to send;
    /// * `timeout`: if specified will terminate after the given time;
    /// * `transactions_limit`: if specified will terminate after submitting the given number of
    /// transactions;
    /// * `tps`: transactions-per-second;
    /// * `traffic_type`: how messages should be sent.
    pub fn spawn(
        nodes: Vec<Arc<RwLock<dyn Node>>>,
        transaction_type: TransactionType,
        timeout: Option<Duration>,
        transactions_limit: Option<usize>,
        tps: u64,
        traffic_type: TrafficType,
    ) -> JoinHandle<()> {
        let (tx_sender, tx_receiver) = channel(1000);
        Self::spawn_producer(nodes.to_vec(), tx_sender, transaction_type);

        // Schedule submission of transactions with random delays and given tps.
        // We use tokio because it allows to spawn a large number of tasks that will be resolved
        // some time in the future.
        thread::spawn(move || {
            let interval = Duration::from_nanos((Duration::from_secs(1).as_nanos() as u64) / tps);
            let timeout = timeout.map(|t| Instant::now() + t);
            #[allow(clippy::mutex_atomic)]
            let messages_sent = Arc::new(Mutex::new(0usize));
            tokio::run(
                Interval::new_interval(interval)
                    .take_while(move |_| {
                        let mut guard = messages_sent.lock().unwrap();
                        *guard += 1;
                        if let Some(t_limit) = transactions_limit {
                            if t_limit <= *guard {
                                // We hit transaction limit.
                                return Ok(false);
                            }
                        }

                        if let Some(t_limit) = timeout {
                            if t_limit > Instant::now() {
                                // We hit timeout.
                                return Ok(false);
                            }
                        }
                        Ok(true)
                    })
                    .map_err(|_| ()) // Timer errors are irrelevant.
                    .zip(tx_receiver)
                    .map(|(_, t)| t)
                    .for_each(move |t| {
                        let instant = match traffic_type {
                            TrafficType::Regular => Instant::now(),
                            TrafficType::ImitateLag { mean_delay } => {
                                Instant::now() + Self::sample_exp(mean_delay)
                            }
                        };
                        let node = sample_one(&nodes).clone();
                        tokio::spawn(
                            Delay::new(instant)
                                .map_err(|_| format!("Timer error"))
                                .and_then(move |_| {
                                    node.write().unwrap()
                                        .async_user().add_transaction(t)
                                        .timeout(Duration::from_secs(1))
                                        .map(|_| ())
                                        .map_err(|err| format!("Error sending transaction {}", err))
                                })
                                .map_err(|_| ())
                        );
                        Ok(())
                    })
                    .map(|_| ())
                    .map_err(|_| ()),
            );
        })
    }

    /// Get random duration according to exponential distribution.
    fn sample_exp(mean: Duration) -> Duration {
        let lambda = 1.0f64 / (mean.as_micros() as f64);
        let exp = Exp::new(lambda);
        Duration::from_micros(exp.sample(&mut rand::thread_rng()) as u64)
    }

    /// Spawn task that produces transactions.
    /// Args:
    /// * `nodes`: nodes with accounts that generate transactions;
    /// * `sender`: where to send the produced transactions;
    /// * `transaction_type`: what kind of transactions to send.
    fn spawn_producer(
        nodes: Vec<Arc<RwLock<dyn Node>>>,
        mut sender: Sender<SignedTransaction>,
        transaction_type: TransactionType,
    ) {
        thread::spawn(move || {
            let nonces = Self::get_nonces(&nodes);
            let mut generator = Generator::new(nodes, nonces).iter(transaction_type);
            let mut backlog = VecDeque::new();
            loop {
                if backlog.is_empty() {
                    backlog.push_back(generator.next().unwrap());
                }

                match sender.try_send(backlog.front().cloned().unwrap()) {
                    // The transaction was successfully processed. Pop it from the backlog.
                    Ok(()) => {
                        backlog.pop_front();
                    }
                    Err(err) => {
                        if err.is_disconnected() {
                            // The channel disconnected, we stop producing transactions.
                            break;
                        }
                        if err.is_full() {
                            // The channel is full, we wait a bit.
                            thread::sleep(Duration::from_millis(50));
                        }
                    }
                }
            }
        });
    }

    fn get_nonces(nodes: &Vec<Arc<RwLock<dyn Node>>>) -> RwLock<HashMap<AccountId, u64>> {
        let mut res = HashMap::new();
        for node in nodes {
            let guard = node.read().unwrap();
            let account_id = guard.account_id().unwrap().clone();
            let nonce = guard.user().view_account(&account_id).unwrap().nonce;
            // Since the first set of transactions is used for initialization (e.g. contract
            // deployment) we want them to get through, so we shift the nonces by 100 in case
            // there are other transactions that are send over to the testnet while we are computing
            // their nonces.
            res.insert(account_id, nonce + 100);
        }
        RwLock::new(res)
    }

    /// Submits transaction to a random node.
    pub fn submit_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        let node = sample_one(&self.nodes);
        node.write().unwrap().add_transaction(transaction)
    }
}
