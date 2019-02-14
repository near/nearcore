use client::ChainConsensusBlockBody;
use futures::future::Either;
use futures::sync::mpsc::{Receiver, Sender};
use futures::{future, Future, Sink, Stream};
use primitives::signature::DEFAULT_SIGNATURE;
use primitives::types::{MessageDataBody, SignedMessageData};
use std::collections::HashSet;
use std::time::Duration;
use tokio::{self, timer::Interval};
use primitives::chain::ChainPayload;
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
        .fold((control_rx, vec![], initial_beacon_block_index), move |(control_rx, mut acc, mut beacon_block_index), p| {
            if let Some(payload) = p {
                let message: SignedMessageData<ChainPayload> = SignedMessageData {
                    owner_sig: DEFAULT_SIGNATURE, // TODO: Sign it.
                    hash: 0,                      // Compute real hash
                    body: MessageDataBody {
                        owner_uid: 0,
                        parents: HashSet::new(),
                        epoch: 0,
                        payload,
                        endorsements: vec![],
                    },
                    beacon_block_index: 0,  // Not used by the DevNet.
                };
                acc.push(message);
                Either::A(future::ok((control_rx, acc, beacon_block_index)))
            } else {
                if !acc.is_empty() {
                    beacon_block_index += 1;
                    let c = ChainConsensusBlockBody {
                        messages: acc,
                        beacon_block_index
                    };
                    Either::B(consensus_tx.clone().send(c).then(move |res| {
                        if let Err(err) = res {
                            error!("Failure sending pass-through consensus {}", err);
                        }
                        future::ok((control_rx, vec![], beacon_block_index))
                    }))
                } else {
                    Either::A(future::ok((control_rx, vec![], beacon_block_index)))
                }
            }
        })
        .map(|_| ());

    tokio::spawn(task);
}
