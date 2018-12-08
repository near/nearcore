use beacon_chain_handler::producer::{BeaconChainPayload, BeaconChainConsensusBlockBody};
use primitives::types::{SignedTransaction, SignedMessageData, MessageDataBody};
use primitives::signature::DEFAULT_SIGNATURE;
use futures::sync::mpsc::{Sender, Receiver};
use futures::{Future, future, Stream, Sink};
use std::collections::HashSet;
use tokio;


pub fn create_passthrough_beacon_block_consensus_task(
    transactions_rx: Receiver<SignedTransaction>,
    consensus_tx: &Sender<BeaconChainConsensusBlockBody>,
) -> Box<Future<Item=(), Error=()> + Send> {
    Box::new(transactions_rx.fold(consensus_tx.clone(), |consensus_tx, t| {
        let message: SignedMessageData<BeaconChainPayload> = SignedMessageData {
            owner_sig: DEFAULT_SIGNATURE,  // TODO: Sign it.
            hash: 0,  // Compute real hash
            body: MessageDataBody {
                owner_uid: 0,
                parents: HashSet::new(),
                epoch: 0,
                payload: (vec![t], vec![]),
                endorsements: vec![],
            },
        };
        let c = BeaconChainConsensusBlockBody {
            messages: vec![message],
        };
        tokio::spawn(consensus_tx.clone().send(c).map(|_| ()).map_err(|e| {
            error!("Failure sending pass-through consensus {:?}", e);
        }));
        future::ok(consensus_tx)
    }).map(|_| ()).map_err(|_| ()))
}
