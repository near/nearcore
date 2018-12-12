use beacon_chain_handler::producer::{ChainConsensusBlockBody, ShardChainPayload};
use futures::sync::mpsc::{Receiver, Sender};
use futures::{future, Future, Sink, Stream};
use primitives::signature::DEFAULT_SIGNATURE;
use primitives::types::{MessageDataBody, SignedMessageData, SignedTransaction};
use std::collections::HashSet;
use tokio;

pub fn spawn_pasthrough_consensus(
    transactions_rx: Receiver<SignedTransaction>,
    consensus_tx: Sender<ChainConsensusBlockBody>,
) {
    let task =
        transactions_rx
            .fold(consensus_tx, |consensus_tx, t| {
                let message: SignedMessageData<ShardChainPayload> = SignedMessageData {
                    owner_sig: DEFAULT_SIGNATURE, // TODO: Sign it.
                    hash: 0,                      // Compute real hash
                    body: MessageDataBody {
                        owner_uid: 0,
                        parents: HashSet::new(),
                        epoch: 0,
                        payload: (vec![t], vec![]),
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
