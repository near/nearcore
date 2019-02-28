use std::sync::Arc;
use std::time::Duration;

use futures::sync::mpsc::{Receiver, Sender};
use futures::{future, Future, Sink, Stream};
use tokio::{self, timer::Interval};

use client::Client;
use nightshade::nightshade::{BlockProposal, ConsensusBlockProposal};
use primitives::hash::CryptoHash;
use mempool::pool_task::MemPoolControl;

pub fn spawn_consensus(
    client: Arc<Client>,
    consensus_tx: Sender<ConsensusBlockProposal>,
    mempool_control_rx: Receiver<MemPoolControl>,
    block_period: Duration,
) {
    let initial_beacon_block_index = client.beacon_chain.chain.best_index();
    let task = Interval::new_interval(block_period)
        .fold(
            (mempool_control_rx, initial_beacon_block_index),
            move |(mempool_control_rx, mut beacon_block_index), _| {
                let hash = client.shard_client.pool.snapshot_payload();
                if hash != CryptoHash::default() {
                    beacon_block_index += 1;
                    let c = ConsensusBlockProposal {
                        proposal: BlockProposal { author: 0, hash },
                        index: beacon_block_index,
                    };
                    tokio::spawn(consensus_tx.clone().send(c).map(|_| ()).map_err(|e| {
                        error!("Failure sending pass-through consensus {}", e);
                    }));
                    future::ok((mempool_control_rx, beacon_block_index))
                } else {
                    future::ok((mempool_control_rx, beacon_block_index))
                }
            },
        )
        .map(|_| ())
        .map_err(|e| error!("timer error: {}", e));

    tokio::spawn(task);
}
