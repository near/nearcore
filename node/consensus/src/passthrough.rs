use std::time::Duration;

use futures::{future, Future, Sink, Stream};
use futures::sync::mpsc::{Receiver, Sender};
use tokio::{self, timer::Interval};

use client::ChainConsensusBlockBody;
use primitives::consensus::Payload;
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;
use txflow::txflow_task::Control;
use client::Client;
use std::sync::Arc;

pub fn spawn_consensus(
    client: Arc<Client>,
    control_rx: Receiver<Control<BeaconWitnessSelector>>,
    consensus_tx: Sender<ChainConsensusBlockBody>,
    block_period: Duration,
) {
    let initial_beacon_block_index = client.beacon_chain.chain.best_index();
    let task = Interval::new_interval(block_period)
        .fold((control_rx, initial_beacon_block_index), move |(control_rx, mut beacon_block_index), _| {
            let payload = client.shard_client.pool.produce_payload();
            if !payload.is_empty() {
                beacon_block_index += 1;
                let c = ChainConsensusBlockBody {
                    payload,
                    beacon_block_index,
                };
                tokio::spawn(consensus_tx.clone().send(c).map(|_| ()).map_err(|e| {
                    error!("Failure sending pass-through consensus {}", e);
                }));
                future::ok((control_rx, beacon_block_index))
            } else {
                future::ok((control_rx, beacon_block_index))
            }
        })
        .map(|_| ())
        .map_err(|e| error!("timer error: {}", e));

    tokio::spawn(task);
}
