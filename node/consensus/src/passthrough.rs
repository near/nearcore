use std::time::Duration;

use futures::{future, Future, Sink, Stream};
use futures::future::Either;
use futures::sync::mpsc::{Receiver, Sender};
use tokio::{self, timer::Interval};

use client::ChainConsensusBlockBody;
use primitives::chain::ChainPayload;
use primitives::consensus::Payload;
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;
use txflow::txflow_task::Control;

pub fn spawn_consensus(
    payload_rx: Receiver<ChainPayload>,
    control_rx: Receiver<Control<BeaconWitnessSelector>>,
    consensus_tx: Sender<ChainConsensusBlockBody>,
    initial_beacon_block_index: u64,
    block_period: Duration,
) {
    let interval_stream =
        Interval::new_interval(block_period).map(|_| None as Option<ChainPayload>).map_err(|_| ());
    let payload_stream = payload_rx.map(Some);
    let task = payload_stream
        .select(interval_stream)
        .fold((control_rx, ChainPayload::default(), initial_beacon_block_index), move |(control_rx, mut payload, mut beacon_block_index), p| {
            if let Some(new_payload) = p {
                payload.union_update(new_payload);
                Either::A(future::ok((control_rx, payload, beacon_block_index)))
            } else {
                if !payload.is_empty() {
                    beacon_block_index += 1;
                    let c = ChainConsensusBlockBody {
                        payload,
                        beacon_block_index
                    };
                    Either::B(consensus_tx.clone().send(c).then(move |res| {
                        if let Err(err) = res {
                            error!("Failure sending pass-through consensus {}", err);
                        }
                        future::ok((control_rx, ChainPayload::default(), beacon_block_index))
                    }))
                } else {
                    Either::A(future::ok((control_rx, ChainPayload::default(), beacon_block_index)))
                }
            }
        })
        .map(|_| ());

    tokio::spawn(task);
}
