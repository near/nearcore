use beacon_chain_handler::producer::ChainConsensusBlockBody;
use futures::sync::mpsc::{Receiver, Sender};
use futures::{Future, Sink, Stream, future};
use primitives::signature::DEFAULT_SIGNATURE;
use primitives::types::{MessageDataBody, SignedMessageData, ChainPayload, Gossip};
use std::collections::HashSet;
use tokio::{self, timer::Interval};
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;
use txflow::txflow_task::Control;
use std::time::Duration;
use std::sync::Arc;
use parking_lot::Mutex;

const BLOCK_PERIOD: Duration = Duration::from_secs(1);

#[allow(clippy::needless_pass_by_value)]
pub fn spawn_consensus(
    _inc_gossip_rx: Receiver<Gossip<ChainPayload>>,
    payload_rx: Receiver<ChainPayload>,
    _out_gossip_tx: Sender<Gossip<ChainPayload>>,
    control_rx: Receiver<Control<BeaconWitnessSelector>>,
    consensus_tx: Sender<ChainConsensusBlockBody>,
    batch_transactions: bool,
) { 
    // whether produce blocks immediately when receives a transaction
    if !batch_transactions {
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
    } else {
        let message_queue = Arc::new(Mutex::new(vec![]));
        let read_message_task = payload_rx.for_each({
            let messages = message_queue.clone();
            move |p| {
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
            messages.lock().push(message);
            Ok(())
        }}).map_err(|_| ());
        let send_consensus_task = Interval::new_interval(BLOCK_PERIOD).for_each({
            let messages = message_queue.clone();
            move |_| {
            let mut messages = messages.lock();
            if !messages.is_empty() {
                let new_messages = messages.drain(..).collect();
                let c = ChainConsensusBlockBody { messages: new_messages };
                tokio::spawn(consensus_tx.clone().send(c).map(|_| ()).map_err(|e| {
                    error!("Failure sending pass-through consensus {:?}", e);
                }));
            }
            Ok(())
        }}).map_err(|_| ());
        tokio::spawn(read_message_task);
        tokio::spawn(send_consensus_task);
    }
}
