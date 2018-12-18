use beacon_chain_handler::producer::ChainConsensusBlockBody;
use futures::sync::mpsc::{Receiver, Sender};
use futures::{future, Future, Sink, Stream};
use primitives::signature::DEFAULT_SIGNATURE;
use primitives::types::{MessageDataBody, SignedMessageData, ChainPayload, Gossip};
use std::collections::HashSet;
use tokio;

#[allow(clippy::needless_pass_by_value)]
pub fn spawn_consensus(
    payload_rx: Receiver<ChainPayload>,
    consensus_tx: Sender<ChainConsensusBlockBody>,
    _inc_gossip_rx: Receiver<Gossip<ChainPayload>>,
    _out_gossip_tx: Sender<Gossip<ChainPayload>>,
) {
    let task = payload_rx
        .fold(consensus_tx, |consensus_tx, p| {
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
            future::ok(consensus_tx)
        })
        .map(|_| ())
        .map_err(|_| ());
    tokio::spawn(task);
}
