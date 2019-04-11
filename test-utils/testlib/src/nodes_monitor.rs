//! A struct that monitors the state of a group of nodes and answers:
//! * Whether nodes are online;
//! * Whether nodes are in sync;
//! * Whether any node is stuck
//! * Transaction throughput of the nodes.

use crate::node::Node;
use node_http::types::GetBlocksByIndexRequest;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub enum NodeState {
    NotConnected,
    BestBlock(u64),
}

/// Stats about the block.
#[derive(Clone)]
pub struct BlockStats {
    /// When this block was observed. Note, ideally we want this to be a timestamp of when the block
    /// was produced, but we do not store such timestamp.
    timestamp: Instant,
    /// Number of transactions that this block contains.
    num_transactions: usize,
}

impl NodeState {
    pub fn is_running(&self) -> bool {
        match self {
            NodeState::BestBlock(_) => true,
            _ => false,
        }
    }
}

pub struct NodesMonitor {
    /// Nodes to monitor.
    nodes: Vec<Arc<RwLock<dyn Node>>>,
    /// States of the nodes.
    states: Arc<RwLock<Vec<NodeState>>>,
    /// Stats of the blocks.
    block_stats: Arc<RwLock<HashMap<u64, BlockStats>>>,
    /// How frequently should be check the state of the nodes.
    state_check_delay: Duration,
    /// How frequently we check for new blocks. We query only one node per update, also the update
    /// is pretty heavy, so `block_check_delay` can be less frequent than `state_check_delay`.
    block_check_delay: Duration,
    /// A variable indicating whether the background thread should shutdown.
    shutdown: Arc<RwLock<bool>>,
}

impl NodesMonitor {
    pub fn new(
        nodes: Vec<Arc<RwLock<dyn Node>>>,
        state_check_delay: Duration,
        block_check_delay: Duration,
    ) -> Self {
        let num_nodes = nodes.len();
        Self {
            nodes,
            states: Arc::new(RwLock::new(vec![NodeState::NotConnected; num_nodes])),
            block_stats: Default::default(),
            state_check_delay,
            block_check_delay,
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    /// Starts a background thread that updates the internal metrics.
    pub fn start(&self) {
        self.start_state_observer();
        self.start_block_observer();
    }

    fn start_state_observer(&self) {
        let shutdown = self.shutdown.clone();
        let state_check_delay = self.state_check_delay.clone();
        let nodes = self.nodes.to_vec();
        let states = self.states.clone();
        thread::spawn(move || {
            while !*shutdown.read().unwrap() {
                for (node_ind, n) in nodes.iter().enumerate() {
                    let node_state = match n.read().unwrap().user().get_best_block_index() {
                        None => NodeState::NotConnected,
                        Some(block_ind) => NodeState::BestBlock(block_ind),
                    };
                    states.write().unwrap()[node_ind] = node_state;
                }
                thread::sleep(state_check_delay);
            }
        });
    }

    fn start_block_observer(&self) {
        let shutdown = self.shutdown.clone();
        let block_check_delay = self.block_check_delay.clone();
        let nodes = self.nodes.to_vec();
        let states = self.states.clone();
        let block_stats = self.block_stats.clone();
        thread::spawn(move || {
            while !*shutdown.read().unwrap() {
                let mut leading_nodes = Self::leading_nodes(&nodes, &states);
                if !leading_nodes.is_empty() {
                    // Will use after #850 is submitted.
                    // let leader = sample_one(&leading_nodes);
                    let leader = leading_nodes.pop().unwrap();
                    let mut block_stats = block_stats.write().unwrap();
                    let prev_block_index = block_stats.keys().max().unwrap_or(&0);
                    let leader_guard = leader.read().unwrap();

                    if let Some(ref new_index) = leader_guard.user().get_best_block_index() {
                        if new_index > prev_block_index {
                            let blocks = leader_guard.user().get_shard_blocks_by_index(
                                GetBlocksByIndexRequest {
                                    start: Some(prev_block_index - 1),
                                    limit: None,
                                },
                            );
                            if let Ok(blocks) = blocks {
                                for b in &blocks.blocks {
                                    block_stats.insert(
                                        b.body.header.index,
                                        BlockStats {
                                            num_transactions: b.body.transactions.len(),
                                            timestamp: Instant::now(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                }
                thread::sleep(block_check_delay);
            }
        });
    }

    /// Computes average tps based on the current block stats.
    pub fn average_tps(&self) -> Option<u64> {
        let block_stats = self.block_stats.read().unwrap();
        let mut sorted_blocks: Vec<_> =
            block_stats.iter().map(|e| (e.0.clone(), e.1.clone())).collect();
        let num_blocks = sorted_blocks.len();
        if num_blocks <= 1 {
            return None;
        }
        sorted_blocks.sort_by_key(|(i, _)| *i);
        let mut acc = vec![];
        let mut prev_block = None;
        for (_, block) in sorted_blocks {
            prev_block = match prev_block {
                None => Some(block),
                Some(pb) => {
                    let tps = (block.num_transactions as f64)
                        / ((block.timestamp - pb.timestamp).as_micros() as f64)
                        * (Duration::from_secs(1).as_micros() as f64);
                    acc.push(tps);
                    Some(block)
                }
            };
        }
        Some((acc.iter().sum::<f64>() / (acc.len() as f64)) as u64)
    }

    /// Get nodes that have the most up-to-date block index.
    fn leading_nodes(
        nodes: &Vec<Arc<RwLock<dyn Node>>>,
        states: &Arc<RwLock<Vec<NodeState>>>,
    ) -> Vec<Arc<RwLock<dyn Node>>> {
        let guard = states.read().unwrap();
        let blocks: Vec<_> = guard
            .iter()
            .zip(nodes.iter())
            .filter_map(|(s, n)| match s {
                NodeState::NotConnected => None,
                NodeState::BestBlock(i) => Some((i, n.clone())),
            })
            .collect();
        if let Some(max) = blocks.iter().map(|(i, _)| *i).max() {
            blocks.into_iter().filter_map(|(i, n)| if i == max { Some(n) } else { None }).collect()
        } else {
            vec![]
        }
    }

    pub fn all_nodes_running(&self) -> bool {
        self.states.read().unwrap().iter().all(|n| n.is_running())
    }

    /// Returns if all nodes are in sync.
    /// Args:
    /// * `block_ind_tolerance`: maximum number of blocks a single node is allowed to be behind the
    /// leader, typically should be equal to 1.
    pub fn all_nodes_in_sync(&self, block_ind_tolerance: u64) -> bool {
        let guard = self.states.read().unwrap();
        let mut indices = vec![];
        for n in &*guard {
            indices.push(match n {
                NodeState::NotConnected => return false,
                NodeState::BestBlock(i) => i,
            });
        }
        let max = *indices.iter().cloned().max().unwrap();
        let min = *indices.iter().cloned().min().unwrap();
        min + block_ind_tolerance >= max
    }
}

impl Drop for NodesMonitor {
    fn drop(&mut self) {
        *self.shutdown.write().unwrap() = true;
    }
}
