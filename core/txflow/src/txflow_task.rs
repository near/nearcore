use futures::sync::mpsc;
use futures::{Stream, Poll};

use primitives::types::{UID, Gossip};
use primitives::traits::{Payload, WitnessSelector};
use dag::DAG;


pub struct TxFlowTask<'a, P: Payload, W: WitnessSelector> {
    owner_uid: UID,
    starting_epoch: u64,
    messages_receiver: mpsc::Receiver<Gossip<P>>,
    payload_receiver: mpsc::Receiver<P>,
    messages_sender: mpsc::Sender<Gossip<P>>,
    witness_selector: Box<W>,
    dag: Option<Box<DAG<'a, P, W>>>,
}

impl<'a, P: Payload, W: WitnessSelector> TxFlowTask<'a, P, W> {
    fn new(owner_uid: UID,
           starting_epoch: u64,
           messages_receiver: mpsc::Receiver<Gossip<P>>,
           payload_receiver: mpsc::Receiver<P>,
           messages_sender: mpsc::Sender<Gossip<P>>,
           witness_selector: Box<W>) -> Self {
        Self {
            owner_uid,
            starting_epoch,
            messages_receiver,
            payload_receiver,
            messages_sender,
            witness_selector,
            dag: None,
        }
    }
}

impl<'a, P: Payload, W: WitnessSelector> Stream for TxFlowTask<'a, P, W> {
    type Item = Gossip<P>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Check if DAG needs to be created.
        if self.dag.is_none() {
            let witness_ptr = self.witness_selector.as_ref() as *const W;
            self.dag = Some(Box::new(
                DAG::new(self.owner_uid, self.starting_epoch, unsafe {&*witness_ptr})));
        }
        unimplemented!()
    }
}
