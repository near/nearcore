use client::ChainConsensusBlockBody;
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
use client::Client;
use std::sync::Arc;

pub fn spawn_consensus(
    client: Arc<Client>,
    control_rx: Receiver<Control<BeaconWitnessSelector>>,
    consensus_tx: Sender<ChainConsensusBlockBody>,
    block_period: Duration,
) {
    let task = Interval::new_interval(block_period)
        .fold(control_rx, move |control_rx, _| {
            let payload = client.shard_client.pool.produce_payload();
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
            let c = ChainConsensusBlockBody {
                messages: vec![message],
                beacon_block_index: 0
            };
            tokio::spawn(consensus_tx.clone().send(c).map(|_| ()).map_err(|e| {
                error!("Failure sending pass-through consensus {}", e);
            }));
            future::ok(control_rx)
        })
        .map(|_| ())
        .map_err(|e| error!("timer error: {}", e));

    tokio::spawn(task);
}
