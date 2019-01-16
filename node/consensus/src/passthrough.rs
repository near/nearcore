use client::ChainConsensusBlockBody;
use futures::sync::mpsc::{Receiver, Sender};
use futures::{Future, Sink, Stream, future};
use primitives::signature::DEFAULT_SIGNATURE;
use primitives::types::{MessageDataBody, SignedMessageData, ChainPayload};
use std::collections::HashSet;
use tokio::{self, timer::Interval};
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;
use txflow::txflow_task::Control;
use std::time::Duration;

pub fn spawn_consensus(
    payload_rx: Receiver<ChainPayload>,
    control_rx: Receiver<Control<BeaconWitnessSelector>>,
    consensus_tx: Sender<ChainConsensusBlockBody>,
    block_period: Duration
) {
    let interval_stream = Interval::new_interval(block_period)
        .map(|_| None as Option<ChainPayload>)
        .map_err(|_| ());
    let payload_stream = payload_rx.map(Some);
    let task = payload_stream
        .select(interval_stream)
        .fold((control_rx, vec![]), move |(control_rx, mut acc), p| {
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
                };
                acc.push(message);
                future::ok((control_rx, acc))
            } else {
                if !acc.is_empty() {
                    let c = ChainConsensusBlockBody { messages: acc };
                    tokio::spawn(consensus_tx.clone().send(c).map(|_| ()).map_err(|e| {
                        error!("Failure sending pass-through consensus {:?}", e);
                    }));
                }
                future::ok((control_rx, vec![]))
            }
        }).map(|_| ());

    tokio::spawn(task);
}
