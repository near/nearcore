use std::borrow::{BorrowMut, Borrow};
use std::collections::{HashSet, HashMap};
use std::mem;
use std::time::{Instant, Duration};

use futures::{Future, Poll, Async, Stream, Sink};
use futures::sync::mpsc;
use tokio::timer::Delay;

use primitives::types::{UID, Gossip, GossipBody, SignedMessageData, TxFlowHash};
use primitives::traits::{Payload, WitnessSelector};
use dag::DAG;

static UNINITIALIZED_DAG_ERR: &'static str = "The DAG structure was not initialized yet.";
static CANDIDATES_OUT_OF_SYNC_ERR: &'static str = "The structures that are used for candidates tracking are ouf ot sync.";
const COOLDOWN_MS: u64 = 1;
const FORCED_GOSSIP_MS: u64 = 1000;

/// A future that owns TxFlow DAG and encapsulates gossiping logic. Should be run as a separate
/// task by a reactor. Consumes a stream of gossips and payloads, and produces a stream of gossips
/// and consensuses. Currently produces only stream of gossips, TODO stream of consensuses.
pub struct TxFlowTask<'a, P: 'a + Payload, W: 'a + WitnessSelector> {
    owner_uid: UID,
    starting_epoch: u64,
    /// The size of the random sample of witnesses that we draw every time we gossip.
    gossip_size: usize,
    messages_receiver: mpsc::Receiver<Gossip<P>>,
    payload_receiver: mpsc::Receiver<P>,
    messages_sender: mpsc::Sender<Gossip<P>>,
    witness_selector: Box<W>,
    dag: Option<Box<DAG<'a, P, W>>>,

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
    pub fn new(owner_uid: UID,
               starting_epoch: u64,
               gossip_size: usize,
               messages_receiver: mpsc::Receiver<Gossip<P>>,
               payload_receiver: mpsc::Receiver<P>,
               messages_sender: mpsc::Sender<Gossip<P>>,
               witness_selector: W) -> Self {
        Self {
            owner_uid,
            starting_epoch,
            gossip_size,
            messages_receiver,
            payload_receiver,
            messages_sender,
            witness_selector: Box::new(witness_selector),
            dag: None,
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

    /// Drop the current TxFlow DAG.
    pub fn drop_dag(&mut self) {
        self.dag.take();
    }

    /// Mutable reference to the DAG.
    fn dag_as_mut(&mut self) -> &mut DAG<'a, P, W>{
        self.dag.as_mut().expect(UNINITIALIZED_DAG_ERR).borrow_mut()
    }

    /// Immutable reference to the DAG.
    fn dag_as_ref(&self) -> &DAG<'a, P, W>{
        self.dag.as_ref().expect(UNINITIALIZED_DAG_ERR).borrow()
    }

    /// Sends a gossip by spawning a separate task.
    fn send_gossip(&self, gossip: Gossip<P>) {
        let copied_tx = self.messages_sender.clone();
        tokio::spawn(copied_tx.send(gossip).map(|_| ()).map_err(|e| {
           error!("Failure in the sub-task {:?}", e);
        }));
    }

    /// Adds a message to the DAG, and if it unblocks other pending messages then recursively add
    /// them, too. Assumes that the provided message is not in the tracking containers, i.e. it is
    /// either a new message with all parents or it became unblocked just now.
    fn add_message(&mut self, message: SignedMessageData<P>) {
        let hash =  message.hash;
        if let Err(e) = self.dag_as_mut().add_existing_message(message) {
            panic!("Attempted to add invalid message to the DAG {}", e)
        }
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
                let unknown_parents = self.blocked_messages
                    .get_mut(&blocked_message_hash)
                    .expect(CANDIDATES_OUT_OF_SYNC_ERR);
                unknown_parents.remove(&hash);  // Now this message is not blocked `message`.
                unknown_parents.is_empty()
            };

            if is_unblocked {
                // If it is unblocked then add it to the DAG.
                let (blocked_message, _) = self.blocked_messages.remove_entry(&blocked_message_hash)
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
    fn process_incoming_message(&mut self, message: SignedMessageData<P>, message_sender: UID, requires_reply: bool) {
        // Check one of the optimistic scenarios when we already know this message.
        if self.dag_as_ref().contains_message(message.hash)
            || self.blocked_messages.contains_key(&message.hash) {
            return;
        }

        let unknown_hashes: HashSet<TxFlowHash> = (&message.body.parents).iter().filter_map(
            |h| if self.dag_as_ref().contains_message(*h) {
                None } else {
                Some(*h)
            } ).collect();

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
                self.blocking_hashes.entry(*h).or_insert_with(HashSet::new)
                    .insert(message.hash);
            }

            // Record that we need to fetch these hashes.
            self.pending_fetches.entry(message_sender).or_insert_with(HashSet::new)
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
            },
            GossipBody::UnsolicitedReply(message) => self.process_incoming_message(message, sender_uid, false),
            GossipBody::Fetch(ref mut hashes) => {
                let reply_messages: Vec<_> =
                hashes.drain(..).filter_map(
                    |h| self.dag_as_ref()
                        .copy_message_data_by_hash(h)).collect();
                let reply = Gossip {
                    sender_uid: self.owner_uid,
                    receiver_uid: gossip.sender_uid,
                    sender_sig: 0,  // TODO: Sign it.
                    body: GossipBody::FetchReply(reply_messages)
                };
                self.send_gossip(reply);
            },
            GossipBody::FetchReply(ref mut messages) => {
                for m in messages.drain(..) {
                    self.process_incoming_message(m, sender_uid, false);
                }
            },
        }
    }

    fn init_dag(&mut self) {
        let witness_ptr = self.witness_selector.as_ref() as *const W;
        // Since we are controlling the creation of the DAG by encapsulating it here
        // this code is safe.
        self.dag = Some(Box::new(
            DAG::new(self.owner_uid, self.starting_epoch, unsafe {&*witness_ptr})));
    }
}

// TxFlowTask can be used as a stream, where each element produced by the stream corresponds to
// an individual step of the algorithm.
impl<'a, P: Payload, W: WitnessSelector> Stream for TxFlowTask<'a, P, W> {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Check if DAG needs to be created.
        if self.dag.is_none() {
            self.init_dag();
        }

        // Process new gossips.
        let mut end_of_gossips = false;
        loop {
            match self.messages_receiver.poll() {
                Ok(Async::Ready(Some(gossip))) => self.process_gossip(gossip),
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(None)) => {
                    // End of the stream that feeds the gossips.
                    end_of_gossips = true;
                    break
                },
                Err(err) => error!("Failed to receive a gossip {:?}", err),
            }
        };

        // Collect new payloads
        let mut end_of_payloads = false;
        loop {
            match self.payload_receiver.poll() {
                Ok(Async::Ready(Some(payload))) => self.pending_payload.union_update(payload),
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(None)) => {
                    // End of the stream that feeds the payloads.
                    end_of_payloads = true;
                    break
                },
                Err(err) => error!("Failed to receive a payload {:?}", err),
            }
        }

        // Sending pending fetches.
        let mut gossips_to_send = vec![];
        for (receiver_uid, mut hashes) in self.pending_fetches.drain() {
            assert!(!hashes.is_empty());
            let reply = Gossip {
                sender_uid: self.owner_uid,
                receiver_uid,
                sender_sig: 0,  // TODO: Sign it.
                body: GossipBody::Fetch(hashes.drain().collect())
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
            let new_message = self.dag_as_mut().create_root_message(payload, vec![]);
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
                Ok(Async::Ready(None)) } else {
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
                self.dag_as_ref().current_root_data().expect("Expected only one root")
            );

            // First send gossip to random witnesses.
            let random_witnesses = self.witness_selector.random_witnesses(
                gossip_body.body.epoch, self.gossip_size);
            for w in &random_witnesses {
                let gossip = Gossip {
                    sender_uid: self.owner_uid,
                    receiver_uid: *w,
                    sender_sig: 0,  // TODO: Sign it.
                    body: GossipBody::Unsolicited(gossip_body.clone())
                };
                self.send_gossip(gossip)
            }

            // Then, send it to whoever has requested it.
            for w in &self.pending_replies - &random_witnesses {
                let gossip = Gossip {
                    sender_uid: self.owner_uid,
                    receiver_uid: w,
                    sender_sig: 0,  // TODO: Sign it.
                    body: GossipBody::UnsolicitedReply(gossip_body.clone())
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
            Ok(Async::Ready(None)) } else {
            Ok(Async::Ready(Some(()))) }
    }
}

#[cfg(test)]
mod fake_network;

#[cfg(test)]
mod tests {
    use super::TxFlowTask;
    use std::collections::HashSet;
    use futures::future::*;
    use futures::Stream;
    use futures::sync::mpsc;

    use primitives::types::UID;
    use primitives::traits::WitnessSelector;
    use testing_utils::FakePayload;

    struct FakeWitnessSelector {
        schedule: HashSet<UID>,
        next_random_witnesses: HashSet<UID>,
    }

    impl FakeWitnessSelector {
        fn new() -> Self {
            Self {
                schedule: set!{0,1,2},
                next_random_witnesses: set!{0},
            }
        }

        // Will be used in the future unit tests.
        #[allow(dead_code)]
        fn set_next_random_witness(&mut self, uids: HashSet<UID>) {
            self.next_random_witnesses = uids;
        }
    }

    impl WitnessSelector for FakeWitnessSelector {
        fn epoch_witnesses(&self, _epoch: u64) -> &HashSet<UID> {
            &self.schedule
        }
        fn epoch_leader(&self, epoch: u64) -> UID {
            epoch % self.schedule.len() as u64
        }
        fn random_witnesses(&self, _epoch: u64, _sample_size: usize) -> HashSet<UID> {
            self.next_random_witnesses.clone()
        }
    }

    #[test]
    fn empty_start_stop_async() {
        // Start TxFlowTask, but do not feed anything.
        // The input channels drop and end immediately and so should TxFlowTask
        // without producing anything.
        tokio::run(lazy(|| {
            let owner_uid = 0;
            let starting_epoch = 0;
            let sample_size = 2;
            let (_inc_gossip_tx, inc_gossip_rx) = mpsc::channel(1_024);
            let (_inc_payload_tx, inc_payload_rx) = mpsc::channel(1_024);
            let (out_gossip_tx, _out_gossip_rx) = mpsc::channel(1_024);
            let selector = FakeWitnessSelector::new();
            let task = TxFlowTask::<FakePayload, _>::new(
                owner_uid, starting_epoch, sample_size, inc_gossip_rx,
                inc_payload_rx, out_gossip_tx, selector);
            tokio::spawn(task.for_each(|_| Ok(())));
            Ok(())
        }));
    }
}
