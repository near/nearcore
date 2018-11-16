use futures::sync::mpsc;
use futures::{Stream, Async, Poll};

use primitives::types::{UID, Gossip};
use primitives::traits::{Payload, WitnessSelector};
use dag::DAG;

pub struct TxFlowTask<'a, P: Payload, W: WitnessSelector> {
    owner_uid: UID,
    starting_epoch: u64,
    messages_receiver: mpsc::Receiver<Gossip<P>>,
    payload_receiver: mpsc::Receiver<P>,
    messages_sender: mpsc::Sender<Gossip<P>>,
    dag: Option<DAG<'a, P, W>>,
}

impl<'a, P: Payload, W: WitnessSelector> TxFlowTask<'a, P, W>{
    pub fn new(owner_uid: UID,
               starting_epoch: u64,
               messages_receiver: mpsc::Receiver<Gossip<P>>,
               payload_receiver: mpsc::Receiver<P>,
               messages_sender: mpsc::Sender<Gossip<P>>) -> Self {
        let result = Self {
            owner_uid,
            starting_epoch,
            messages_receiver,
            payload_receiver,
            messages_sender,
            dag: None};
        result
    }
}

impl<'a, P: Payload, W: WitnessSelector> Stream for TxFlowTask<'a, P, W> {
    type Item = Gossip<P>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}
