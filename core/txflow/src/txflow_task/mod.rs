use std::borrow::{Borrow, BorrowMut};
use std::collections::{HashMap, HashSet};
use std::mem;
use std::time::{Duration, Instant};

use futures::sync::mpsc;
use futures::{stream, Async, Future, Poll, Sink, Stream};
use tokio::timer::Delay;

use crate::dag::DAG;
use primitives::signature::DEFAULT_SIGNATURE;
use primitives::traits::{Payload, WitnessSelector};
use primitives::types::{
    ConsensusBlockBody, Gossip, GossipBody, SignedMessageData, TxFlowHash, UID,
};

pub mod beacon_witness_selector;

static UNINITIALIZED_DAG_ERR: &'static str = "The DAG structure was not initialized yet.";
static UNINITIALIZED_STATE_ERR: &'static str = "The state was not initialized yet.";
static CANDIDATES_OUT_OF_SYNC_ERR: &'static str =
    "The structures that are used for candidates tracking are ouf ot sync.";
const COOLDOWN_MS: u64 = 1;
const FORCED_GOSSIP_MS: u64 = 1000;

pub struct State<W: WitnessSelector> {
    pub owner_uid: UID,
    pub starting_epoch: u64,
    /// The size of the random sample of witnesses that we draw every time we gossip.
    pub gossip_size: usize,
    pub witness_selector: Box<W>,
}

/// An enum that we use to start and stop the TxFlow task.
pub enum Control<W: WitnessSelector> {
    Reset(State<W>),
    Stop,
}

/// Spawns `TxFlowTask` as a tokio task.
pub fn spawn_task<
    'a,
    P: 'a + Payload + Send + Sync + 'static,
    W: WitnessSelector + Send + Sync + 'static,
>(
    messages_receiver: mpsc::Receiver<Gossip<P>>,
    payload_receiver: mpsc::Receiver<P>,
    messages_sender: mpsc::Sender<Gossip<P>>,
    control_receiver: mpsc::Receiver<Control<W>>,
    consensus_sender: mpsc::Sender<ConsensusBlockBody<P>>,
) {
    let task = TxFlowTask::new(
        messages_receiver,
        payload_receiver,
        messages_sender,
        control_receiver,
        consensus_sender,
    );
    tokio::spawn(task.for_each(|_| Ok(())));
}

/// A future that owns TxFlow DAG and encapsulates gossiping logic. Should be run as a separate
/// task by a reactor. Consumes a stream of gossips and payloads, and produces a stream of gossips
/// and consensuses. Currently produces only stream of gossips, TODO stream of consensuses.
pub struct TxFlowTask<'a, P: 'a + Payload, W: WitnessSelector> {
    state: Option<State<W>>,
    dag: Option<Box<DAG<'a, P, W>>>,
    messages_receiver: mpsc::Receiver<Gossip<P>>,
    payload_receiver: mpsc::Receiver<P>,
    messages_sender: mpsc::Sender<Gossip<P>>,
    control_receiver: mpsc::Receiver<Control<W>>,
    consensus_sender: mpsc::Sender<ConsensusBlockBody<P>>,

    /// Received messages that we cannot yet add to DAG, because we are missing parents.
    /// message -> hashes that we are missing.
    blocked_messages: HashMap<SignedMessageData<P>, HashSet<TxFlowHash>>,
    /// The transpose of `blocked_messages`.
    /// Missing hash -> hashes of the messages that are pending because of that hash.
    blocking_hashes: HashMap<TxFlowHash, HashSet<TxFlowHash>>,
    /// Hash of the message -> UID of the witness whom we should send a reply.
    blocked_replies: HashMap<TxFlowHash, UID>,

    /// A set of UID to which we should reply with a gossip ASAP.
    pending_replies: HashSet<UID>,
    /// A set of hashes that we need to fetch ASAP.
    /// Whom we should fetch it from -> hashes that should be fetched.
    pending_fetches: HashMap<UID, HashSet<TxFlowHash>>,
    /// The payload that we have accumulated so far. We should put this payload into a gossip ASAP.
    pending_payload: P,

    /// Timer that determines the minimum time that we should not gossip after the given message
    /// for the sake of not spamming the network with small packages.
    cooldown_delay: Option<Delay>,
    /// Timer that determines that maximum time allowed without gossip. Even if by the end of the
    /// timer we do not have any new payload or new messages we gossip the old root message anyway.
    forced_gossip_delay: Option<Delay>,
}

impl<'a, P: Payload, W: WitnessSelector> TxFlowTask<'a, P, W> {
    pub fn new(
        messages_receiver: mpsc::Receiver<Gossip<P>>,
        payload_receiver: mpsc::Receiver<P>,
        messages_sender: mpsc::Sender<Gossip<P>>,
        control_receiver: mpsc::Receiver<Control<W>>,
        consensus_sender: mpsc::Sender<ConsensusBlockBody<P>>,
    ) -> Self {
        Self {
            state: None,
            dag: None,
            messages_receiver,
            payload_receiver,
            messages_sender,
            control_receiver,
            consensus_sender,
            blocked_messages: HashMap::new(),
            blocking_hashes: HashMap::new(),
            blocked_replies: HashMap::new(),
            pending_replies: HashSet::new(),
            pending_fetches: HashMap::new(),
            pending_payload: P::new(),
            cooldown_delay: None,
            forced_gossip_delay: None,
        }
    }

    /// Mutable reference to the DAG.
    #[inline]
    fn dag_as_mut(&mut self) -> &mut DAG<'a, P, W> {
        self.dag.as_mut().expect(UNINITIALIZED_DAG_ERR).borrow_mut()
    }

    /// Immutable reference to the DAG.
    #[inline]
    fn dag_as_ref(&self) -> &DAG<'a, P, W> {
        self.dag.as_ref().expect(UNINITIALIZED_DAG_ERR).borrow()
    }

    #[inline]
    fn owner_uid(&self) -> UID {
        self.state.as_ref().expect(UNINITIALIZED_STATE_ERR).owner_uid
    }

    #[inline]
    fn starting_epoch(&self) -> u64 {
        self.state.as_ref().expect(UNINITIALIZED_STATE_ERR).starting_epoch
    }

    #[inline]
    fn gossip_size(&self) -> usize {
        self.state.as_ref().expect(UNINITIALIZED_STATE_ERR).gossip_size
    }

    #[inline]
    fn witness_selector(&self) -> &W {
        self.state.as_ref().expect(UNINITIALIZED_STATE_ERR).witness_selector.as_ref()
    }

    /// Sends a gossip by spawning a separate task.
    fn send_gossip(&self, gossip: Gossip<P>) {
        let copied_tx = self.messages_sender.clone();
        tokio::spawn(copied_tx.send(gossip).map(|_| ()).map_err(|e| {
            error!("Failure in the sub-task {:?}", e);
        }));
    }

    fn send_consensuses(&self, consensuses: Vec<ConsensusBlockBody<P>>) {
        let copied_tx = self.consensus_sender.clone();
        tokio::spawn(copied_tx.send_all(stream::iter_ok(consensuses)).map(|_| ()).map_err(|e| {
            error!("Failure in the sub-task {:?}", e);
        }));
    }

    /// Adds a message to the DAG, and if it unblocks other pending messages then recursively add
    /// them, too. Assumes that the provided message is not in the tracking containers, i.e. it is
    /// either a new message with all parents or it became unblocked just now.
    fn add_message(&mut self, message: SignedMessageData<P>) {
        let hash = message.hash;
        match self.dag_as_mut().add_existing_message(message) {
            Ok(consensuses) => self.send_consensuses(consensuses),
            Err(e) => panic!("Attempted to add invalid message to the DAG {}", e),
        };
        // Get messages that were blocked by this one.
        // Also start removing it from the collections `pending_messages` and `unknown_hashes` that
        // keep track of the blockers.
        let blocked_messages = match self.blocking_hashes.remove(&hash) {
            // There were no messages blocked by this one.
            None => return,
            Some(blocked_messages) => blocked_messages,
        };
        // Iterate over each of them and check whether now it is completely unblocked.
        for blocked_message_hash in blocked_messages {
            let is_unblocked = {
                let unknown_parents = self
                    .blocked_messages
                    .get_mut(&blocked_message_hash)
                    .expect(CANDIDATES_OUT_OF_SYNC_ERR);
                unknown_parents.remove(&hash); // Now this message is not blocked `message`.
                unknown_parents.is_empty()
            };

            if is_unblocked {
                // If it is unblocked then add it to the DAG.
                let (blocked_message, _) = self
                    .blocked_messages
                    .remove_entry(&blocked_message_hash)
                    .expect(CANDIDATES_OUT_OF_SYNC_ERR);
                // If this message also blocked some reply make then it pending.
                if let Some(reply_to_uid) = self.blocked_replies.remove(&blocked_message_hash) {
                    self.pending_replies.insert(reply_to_uid);
                }
                self.add_message(blocked_message);
            }
        }
    }

    /// Processes the incoming message.
    fn process_incoming_message(
        &mut self,
        message: SignedMessageData<P>,
        message_sender: UID,
        requires_reply: bool,
    ) {
        // Check one of the optimistic scenarios when we already know this message.
        if self.dag_as_ref().contains_message(message.hash)
            || self.blocked_messages.contains_key(&message.hash)
        {
            return;
        }

        let unknown_hashes: HashSet<TxFlowHash> = (&message.body.parents)
            .iter()
            .filter_map(|h| if self.dag_as_ref().contains_message(*h) { None } else { Some(*h) })
            .collect();

        if unknown_hashes.is_empty() {
            // We can go ahead and add this message to DAG.
            self.add_message(message);
            // We should also reply to the sender ASAP.
            if requires_reply {
                self.pending_replies.insert(message_sender);
            }
        } else {
            // This candidates is blocked. Update the tracking containers.
            if requires_reply {
                // We should reply later.
                self.blocked_replies.insert(message.hash, message_sender);
            }
            for h in &unknown_hashes {
                self.blocking_hashes.entry(*h).or_insert_with(HashSet::new).insert(message.hash);
            }

            // Record that we need to fetch these hashes.
            self.pending_fetches
                .entry(message_sender)
                .or_insert_with(HashSet::new)
                .extend(unknown_hashes.iter().cloned());

            // Finally, take ownership of this message and record that it is blocked.
            self.blocked_messages.insert(message, unknown_hashes);
        }
    }

    /// Take care of the gossip received from the network. Partially destroys the gossip.
    fn process_gossip(&mut self, mut gossip: Gossip<P>) {
        let sender_uid = gossip.sender_uid;
        match gossip.body {
            GossipBody::Unsolicited(message) => {
                self.pending_replies.insert(sender_uid);
                self.process_incoming_message(message, sender_uid, true);
            }
            GossipBody::UnsolicitedReply(message) => {
                self.process_incoming_message(message, sender_uid, false)
            }
            GossipBody::Fetch(ref mut hashes) => {
                let reply_messages: Vec<_> = hashes
                    .drain(..)
                    .filter_map(|h| self.dag_as_ref().copy_message_data_by_hash(h))
                    .collect();
                let reply = Gossip {
                    sender_uid: self.owner_uid(),
                    receiver_uid: gossip.sender_uid,
                    sender_sig: DEFAULT_SIGNATURE, // TODO: Sign it.
                    body: GossipBody::FetchReply(reply_messages),
                };
                self.send_gossip(reply);
            }
            GossipBody::FetchReply(ref mut messages) => {
                for m in messages.drain(..) {
                    self.process_incoming_message(m, sender_uid, false);
                }
            }
        }
    }

    fn init_dag(&mut self) {
        let witness_ptr = self.witness_selector() as *const W;
        // Since we are controlling the creation of the DAG by encapsulating it here
        // this code is safe.
        self.dag = Some(Box::new(DAG::new(self.owner_uid(), self.starting_epoch(), unsafe {
            &*witness_ptr
        })));
    }
}

// TxFlowTask can be used as a stream, where each element produced by the stream corresponds to
// an individual step of the algorithm.
impl<'a, P: Payload, W: WitnessSelector> Stream for TxFlowTask<'a, P, W> {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.control_receiver.poll() {
            // Independently on whether we were stopped or not, if we receive a reset signal we
            // reset the state and the dag.
            Ok(Async::Ready(Some(Control::Reset(state)))) => {
                self.state = Some(state);
                self.init_dag();
            }
            // Stop command received.
            Ok(Async::Ready(Some(Control::Stop))) => {
                if self.state.is_some() {
                    self.state = None;
                    self.dag = None;
                    // On the next call of poll if we still don't have the state this task will be
                    // parked because we will return NotReady.
                    return Ok(Async::Ready(Some(())));
                } else {
                    panic!("TxFlow task received stop command, but it is already stopped");
                }
            }
            // The input to control channel was dropped. We terminate TxFlow entirely.
            Ok(Async::Ready(None)) => {
                println!("Control channel was dropped");
                return Ok(Async::Ready(None));
            }
            Ok(Async::NotReady) => {
                // If there is not state then we cannot proceed, we return NotReady which will park
                // the task and wait until we get the state over the control channel.
                if self.state.is_none() {
                    return Ok(Async::NotReady);
                }
            }
            Err(err) => error!("Failed to read from the control channel {:?}", err),
        };

        // Process new gossips.
        let mut end_of_gossips = false;
        loop {
            match self.messages_receiver.poll() {
                Ok(Async::Ready(Some(gossip))) => self.process_gossip(gossip),
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(None)) => {
                    // End of the stream that feeds the gossips.
                    end_of_gossips = true;
                    break;
                }
                Err(err) => error!("Failed to receive a gossip {:?}", err),
            }
        }

        // Collect new payloads
        let mut end_of_payloads = false;
        loop {
            match self.payload_receiver.poll() {
                Ok(Async::Ready(Some(payload))) => self.pending_payload.union_update(payload),
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(None)) => {
                    // End of the stream that feeds the payloads.
                    end_of_payloads = true;
                    break;
                }
                Err(err) => error!("Failed to receive a payload {:?}", err),
            }
        }

        // Sending pending fetches.
        let owner_uid = self.owner_uid();
        let mut gossips_to_send = vec![];
        for (receiver_uid, mut hashes) in self.pending_fetches.drain() {
            assert!(!hashes.is_empty());
            let reply = Gossip {
                sender_uid: owner_uid,
                receiver_uid,
                sender_sig: DEFAULT_SIGNATURE, // TODO: Sign it.
                body: GossipBody::Fetch(hashes.drain().collect()),
            };
            gossips_to_send.push(reply);
        }
        for reply in gossips_to_send.drain(..) {
            self.send_gossip(reply);
        }

        // The following code should be executed only if the cooldown has passed.
        if let Some(ref mut d) = self.cooldown_delay {
            try_ready!(d.poll().map_err(|e| error!("Cooldown timer error {}", e)));
        }

        // The following section maybe creates a new root and sends it to some witnesses.
        let mut new_gossip_body = None;
        if !self.pending_payload.is_empty() || self.dag_as_ref().is_root_not_updated() {
            // Drain the current payload.
            let payload = mem::replace(&mut self.pending_payload, P::new());
            let (new_message, consensuses) = self.dag_as_mut().create_root_message(payload, vec![]);
            self.send_consensuses(consensuses);
            new_gossip_body = Some(&new_message.data);
        } else if let Some(ref mut d) = self.forced_gossip_delay {
            // There are no payloads or dangling roots.
            try_ready!(d.poll().map_err(|e| error!("Forced gossip timer error {}", e)));
        } else {
            // This situation happens when we just started TxFlow and haven't received any payloads
            // or gossip. In this case the `forced_gossip_delay` is None.
            return if end_of_gossips && end_of_payloads {
                // In rare cases, like in unit tests that run TxFlowTask in async mode
                // the input streams are dropped even before the task that runs TxFlowTask
                // is spawned. This if-clause covers it.
                Ok(Async::Ready(None))
            } else {
                // It is important that this line executes only when at least one of the sub-futures
                // returns NotReady, because it would break the executor notification system, and
                // this future would hung indefinitely.
                // Since we reached this line of code we broke out from the two loops that collect
                // the payload and the incoming gossip. Each of those two loops breaks when either
                // the stream has ended or the stream is NotReady. Since we are in the else-arm
                // it is not the case when both streams have ended, and so at least one stream has
                // returned NotReady.
                Ok(Async::NotReady)
            };
        }

        {
            let gossip_body = new_gossip_body.unwrap_or_else(||
                // There are no new payloads or dangling roots, but we are forced to gossip.
                // So we are gossiping the current root.
                self.dag_as_ref().current_root_data().expect("Expected only one root"));

            // First send gossip to random witnesses.
            let random_witnesses = self
                .witness_selector()
                .random_witnesses(gossip_body.body.epoch, self.gossip_size());
            for w in &random_witnesses {
                let gossip = Gossip {
                    sender_uid: owner_uid,
                    receiver_uid: *w,
                    sender_sig: DEFAULT_SIGNATURE, // TODO: Sign it.
                    body: GossipBody::Unsolicited(gossip_body.clone()),
                };
                self.send_gossip(gossip)
            }

            // Then, send it to whoever has requested it.
            for w in &self.pending_replies - &random_witnesses {
                let gossip = Gossip {
                    sender_uid: owner_uid,
                    receiver_uid: w,
                    sender_sig: DEFAULT_SIGNATURE, // TODO: Sign it.
                    body: GossipBody::UnsolicitedReply(gossip_body.clone()),
                };
                self.send_gossip(gossip)
            }
        }
        self.pending_replies.clear();

        // Reset the timers.
        let now = Instant::now();
        self.cooldown_delay = Some(Delay::new(now + Duration::from_millis(COOLDOWN_MS)));
        self.forced_gossip_delay = Some(Delay::new(now + Duration::from_millis(FORCED_GOSSIP_MS)));

        // If the gossip stream and the payload stream are closed then we are done.
        // This is very useful for unit tests that start TxFlowTask through spawn.
        if end_of_gossips && end_of_payloads {
            Ok(Async::Ready(None))
        } else {
            Ok(Async::Ready(Some(())))
        }
    }
}

#[cfg(test)]
mod fake_network;

#[cfg(test)]
mod tests {
    use super::TxFlowTask;
    use futures::future::*;
    use futures::sync::mpsc;
    use futures::Stream;
    use std::collections::HashSet;

    use primitives::traits::WitnessSelector;
    use primitives::types::UID;
    use crate::testing_utils::FakePayload;

    struct FakeWitnessSelector {}

    impl WitnessSelector for FakeWitnessSelector {
        fn epoch_witnesses(&self, _epoch: u64) -> &HashSet<UID> {
            unimplemented!()
        }
        fn epoch_leader(&self, _epoch: u64) -> UID {
            unimplemented!()
        }
        fn random_witnesses(&self, _epoch: u64, _sample_size: usize) -> HashSet<UID> {
            unimplemented!()
        }
    }

    #[test]
    fn empty_start_stop_async() {
        // Start TxFlowTask, but do not feed anything.
        // The input channels drop and end immediately and so should TxFlowTask
        // without producing anything.
        tokio::run(lazy(|| {
            let (_inc_gossip_tx, inc_gossip_rx) = mpsc::channel(1024);
            let (_inc_payload_tx, inc_payload_rx) = mpsc::channel(1024);
            let (out_gossip_tx, _out_gossip_rx) = mpsc::channel(1024);
            let (_control_tx, control_rx) = mpsc::channel(1024);
            let (consensus_tx, _consensus_rx) = mpsc::channel(1024);
            let task = TxFlowTask::<FakePayload, FakeWitnessSelector>::new(
                inc_gossip_rx,
                inc_payload_rx,
                out_gossip_tx,
                control_rx,
                consensus_tx,
            );
            tokio::spawn(task.for_each(|_| Ok(())));
            Ok(())
        }));
    }
}
