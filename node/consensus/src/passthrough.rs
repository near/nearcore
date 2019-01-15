use client::ChainConsensusBlockBody;
use futures::sync::mpsc::{Receiver, Sender};
use futures::{future, Future, Sink, Stream};
use primitives::signature::DEFAULT_SIGNATURE;
use primitives::types::{MessageDataBody, SignedMessageData, ChainPayload, Gossip};
use std::collections::HashSet;
use tokio;
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;
use txflow::txflow_task::Control;

#[allow(clippy::needless_pass_by_value)]
pub fn spawn_consensus(
    _inc_gossip_rx: Receiver<Gossip<ChainPayload>>,
    payload_rx: Receiver<ChainPayload>,
    _out_gossip_tx: Sender<Gossip<ChainPayload>>,
    control_rx: Receiver<Control<BeaconWitnessSelector>>,
    consensus_tx: Sender<ChainConsensusBlockBody>,
) {
    // Even though we are not using control_rx we have to keep it alive for the duration of the
    // passthrough consensus.
    let task = payload_rx
        .fold((consensus_tx, control_rx), |(consensus_tx, control_rx), p| {
            let message: SignedMessageData<ChainPayload> = SignedMessageData {
                owner_sig: DEFAULT_SIGNATURE, // TODO: Sign it.
                hash: 0,                      // Compute real hash
                body: MessageDataBody {
                    owner_uid: 0,
                    parents: HashSet::new(),
                    epoch: 0,
                    payload: p,
                    endorsements: vec![],
                },
            };
            let c = ChainConsensusBlockBody { messages: vec![message] };
            tokio::spawn(consensus_tx.clone().send(c).map(|_| ()).map_err(|e| {
                error!("Failure sending pass-through consensus {:?}", e);
            }));
            future::ok((consensus_tx, control_rx))
        })
        .map(|_| ())
        .map_err(|_| ());
    tokio::spawn(task);
}
