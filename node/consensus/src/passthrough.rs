use std::sync::Arc;
use std::time::Duration;

use futures::sync::mpsc::{Receiver, Sender};
use futures::{future, Future, Sink, Stream};
use tokio::{self, timer::Interval};

use client::Client;
use nightshade::nightshade::{BlockProposal, ConsensusBlockProposal};
use primitives::hash::CryptoHash;
use primitives::block_traits::SignedHeader;
use nightshade::nightshade_task::Control;

pub fn spawn_consensus(
    client: Arc<Client>,
    consensus_tx: Sender<ConsensusBlockProposal>,
    control_rx: Receiver<Control>,
    block_period: Duration,
) {
    let initial_beacon_block_index = client.beacon_client.chain.best_index();
    let task = Interval::new_interval(block_period)
        .fold(
            (control_rx, initial_beacon_block_index),
            move |(control_rx, mut beacon_block_index), _| {
                // First check previous block was produced.
                if client.beacon_client.chain.best_index() >= beacon_block_index {
                    let hash = client.shard_client.pool.snapshot_payload();
                    let last_shard_block_header = client.shard_client.chain.best_header();
                    let receipt_block = client.shard_client.get_receipt_block(last_shard_block_header.index(), last_shard_block_header.shard_id());
                    if hash != CryptoHash::default() || receipt_block.is_some() {
                        beacon_block_index += 1;
                        let c = ConsensusBlockProposal {
                            proposal: BlockProposal { author: 0, hash },
                            index: beacon_block_index,
                        };
                        tokio::spawn(consensus_tx.clone().send(c).map(|_| ()).map_err(|e| {
                            error!("Failure sending pass-through consensus {}", e);
                        }));
                    }
                }
                future::ok((control_rx, beacon_block_index))
            },
        )
        .map(|_| ())
        .map_err(|e| error!("timer error: {}", e));

    tokio::spawn(task);
}
