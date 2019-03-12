use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use elapsed::measure_time;
use futures::future::Future;
use futures::sink::Sink;
use futures::sync::mpsc;
use futures::try_ready;
use futures::Async;
use futures::Poll;
use futures::Stream;
use log::*;
use tokio::timer::Delay;

use primitives::aggregate_signature::BlsPublicKey;
use primitives::hash::{hash_struct, CryptoHash};
use primitives::signature::{verify, PublicKey, Signature};
use primitives::signer::BlockSigner;
use primitives::types::{AuthorityId, BlockIndex};

use crate::nightshade::{BlockProposal, ConsensusBlockProposal, Nightshade, State};

const COOLDOWN_MS: u64 = 200;

#[derive(Clone, Debug)]
pub enum Control {
    Reset {
        owner_uid: AuthorityId,
        block_index: BlockIndex,
        hash: CryptoHash,
        public_keys: Vec<PublicKey>,
        bls_public_keys: Vec<BlsPublicKey>,
    },
    Stop,
    PayloadConfirmation(AuthorityId, CryptoHash),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Message {
    pub sender_id: AuthorityId,
    pub receiver_id: AuthorityId,
    pub state: State,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum GossipBody {
    /// Use box because large size difference between variants
    NightshadeStateUpdate(Box<Message>),
    PayloadRequest(Vec<AuthorityId>),
    PayloadReply(Vec<SignedBlockProposal>),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Gossip {
    pub sender_id: AuthorityId,
    pub receiver_id: AuthorityId,
    pub body: GossipBody,
    block_index: u64,
    signature: Signature,
}

impl Gossip {
    fn new(
        sender_id: AuthorityId,
        receiver_id: AuthorityId,
        body: GossipBody,
        signer: Arc<BlockSigner>,
        block_index: u64,
    ) -> Self {
        let hash = hash_struct(&(sender_id, receiver_id, &body, block_index));

        Self { sender_id, receiver_id, body, signature: signer.sign(hash.as_ref()), block_index }
    }

    fn get_hash(&self) -> CryptoHash {
        hash_struct(&(self.sender_id, self.receiver_id, &self.body, self.block_index))
    }

    fn verify(&self, pk: &PublicKey) -> bool {
        verify(self.get_hash().as_ref(), &self.signature, &pk)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedBlockProposal {
    block_proposal: BlockProposal,
    signature: Signature,
}

impl SignedBlockProposal {
    fn new(author: AuthorityId, hash: CryptoHash, signer: Arc<BlockSigner>) -> Self {
        let block_proposal = BlockProposal { author, hash };
        let signature = signer.sign(block_proposal.hash.as_ref());

        Self { block_proposal, signature }
    }

    fn verify(&self, public_key: &PublicKey) -> bool {
        verify(self.block_proposal.hash.as_ref(), &self.signature, &public_key)
    }
}

pub struct NightshadeTask {
    /// Signer.
    signer: Arc<BlockSigner>,
    /// Blocks from other authorities containing payloads. At the beginning of the consensus
    /// authorities only have their own block. It is required for an authority to endorse a block
    /// from other authority to have its block.
    proposals: Vec<Option<SignedBlockProposal>>,
    /// Flags indicating if the proposing blocks can be fetched from the mempool.
    /// Mempool task should send the signal to confirm a proposal
    confirmed_proposals: Vec<bool>,
    /// Nightshade main data structure.
    nightshade: Option<Nightshade>,
    /// Block index that we are currently working on.
    block_index: Option<u64>,
    /// Standard public/secret keys are used to sign payloads and gossips
    public_keys: Vec<PublicKey>,
    /// Channel to receive gossips from other authorities.
    inc_gossips: mpsc::Receiver<Gossip>,
    /// Channel to send gossips to other authorities
    out_gossips: mpsc::Sender<Gossip>,
    /// Channel to start/reset consensus
    /// Important: Reset only works the first time it is sent.
    control_receiver: mpsc::Receiver<Control>,
    /// Consensus header and block index on which the consensus was reached.
    consensus_sender: mpsc::Sender<ConsensusBlockProposal>,
    /// Send block proposal info to retrieve payload.
    retrieve_payload_tx: mpsc::Sender<(AuthorityId, CryptoHash)>,
    /// Flag to determine if consensus was already reported in the consensus channel.
    consensus_reported: bool,
    /// Timer that determines the minimum time that we should not gossip after the given message
    /// for the sake of not spamming the network with small packages.
    cooldown_delay: Option<Delay>,
}

impl NightshadeTask {
    pub fn new(
        signer: Arc<BlockSigner>,
        inc_gossips: mpsc::Receiver<Gossip>,
        out_gossips: mpsc::Sender<Gossip>,
        control_receiver: mpsc::Receiver<Control>,
        consensus_sender: mpsc::Sender<ConsensusBlockProposal>,
        retrieve_payload_tx: mpsc::Sender<(AuthorityId, CryptoHash)>,
    ) -> Self {
        Self {
            signer,
            proposals: vec![],
            confirmed_proposals: vec![],
            nightshade: None,
            block_index: None,
            public_keys: vec![],
            inc_gossips,
            out_gossips,
            control_receiver,
            consensus_sender,
            retrieve_payload_tx,
            consensus_reported: false,
            cooldown_delay: None,
        }
    }

    fn nightshade_as_ref(&self) -> &Nightshade {
        self.nightshade.as_ref().expect("Nightshade should be initialized")
    }

    fn nightshade_as_mut_ref(&mut self) -> &mut Nightshade {
        self.nightshade.as_mut().expect("Nightshade should be initialized")
    }

    fn init_nightshade(
        &mut self,
        owner_uid: AuthorityId,
        block_index: BlockIndex,
        hash: CryptoHash,
        public_keys: Vec<PublicKey>,
        bls_public_keys: Vec<BlsPublicKey>,
    ) {
        let num_authorities = public_keys.len();
        info!(target: "nightshade", "Init nightshade for authority {}/{}, block {}, proposal {}", owner_uid, num_authorities, block_index, hash);
        assert!(self.block_index.is_none() ||
                    self.block_index.unwrap() < block_index ||
                    self.proposals[owner_uid as usize].as_ref().unwrap().block_proposal.hash == hash,
                "Reset without increasing block index: adversarial behavior");

        self.proposals = vec![None; num_authorities];
        self.proposals[owner_uid] =
            Some(SignedBlockProposal::new(owner_uid, hash, self.signer.clone()));
        self.confirmed_proposals = vec![false; num_authorities];
        self.confirmed_proposals[owner_uid] = true;
        self.block_index = Some(block_index);
        self.public_keys = public_keys;
        self.nightshade = Some(Nightshade::new(
            owner_uid,
            num_authorities,
            self.proposals[owner_uid].clone().unwrap().block_proposal,
            bls_public_keys,
            self.signer.clone(),
        ));
        self.consensus_reported = false;

        // Announce proposal to every other node in the beginning of the consensus
        for a in 0..num_authorities {
            if a == owner_uid {
                continue;
            }
            self.send_payloads(a, vec![owner_uid]);
        }
    }

    fn send_state(&self, message: Message) {
        self.send_gossip(Gossip::new(
            self.nightshade.as_ref().unwrap().owner_id,
            message.receiver_id,
            GossipBody::NightshadeStateUpdate(Box::new(message)),
            self.signer.clone(),
            self.block_index.unwrap(),
        ));
    }

    fn send_gossip(&self, message: Gossip) {
        let copied_tx = self.out_gossips.clone();
        tokio::spawn(copied_tx.send(message).map(|_| ()).map_err(|e| {
            error!("Error sending state. {:?}", e);
        }));
    }

    #[allow(dead_code)]
    fn is_final(&self) -> bool {
        self.nightshade_as_ref().is_final()
    }

    fn process_message(&mut self, message: Message) {
        // Get author of the payload inside this message
        let author = message.state.bare_state.endorses.author;

        // Check if we already receive this proposal.
        if let Some(signed_proposal) = &self.proposals[author] {
            if signed_proposal.block_proposal.hash == message.state.block_hash() {
                // Check if this proposal was already confirmed by the mempool
                if self.confirmed_proposals[author] {
                    if let Err(e) =
                        self.nightshade_as_mut_ref().update_state(message.sender_id, message.state)
                    {
                        warn!(target: "nightshade", "{}", e);
                    }
                } else {
                    // Wait for confirmation from mempool,
                    // request was already sent when the proposal arrived.
                }
            } else {
                // There is at least one malicious actor between the sender of this message
                // and the original author of the payload. But we can't determine which is the bad actor.
            }
        } else {
            // TODO: This message is discarded if we haven't received the proposal yet.
            let gossip = Gossip::new(
                self.owner_id(),
                author,
                GossipBody::PayloadRequest(vec![author]),
                self.signer.clone(),
                self.block_index.unwrap(),
            );
            self.send_gossip(gossip);
        }
    }

    fn process_gossip(&mut self, gossip: Gossip) {
        debug!(target: "nightshade", "Node: {} Processing gossip on block index {}", self.owner_id(), gossip.block_index);

        if Some(gossip.block_index) != self.block_index {
            return;
        }
        if !gossip.verify(&self.public_keys[gossip.sender_id]) {
            return;
        }

        match gossip.body {
            GossipBody::NightshadeStateUpdate(message) => self.process_message(*message),
            GossipBody::PayloadRequest(authorities) => {
                self.send_payloads(gossip.sender_id, authorities)
            }
            GossipBody::PayloadReply(payloads) => self.receive_payloads(gossip.sender_id, payloads),
        }
    }

    fn send_payloads(&self, receiver_id: AuthorityId, authorities: Vec<AuthorityId>) {
        let mut payloads = Vec::new();
        for a in authorities {
            if let Some(ref p) = self.proposals[a] {
                payloads.push(p.clone());
            }
        }
        let gossip = Gossip::new(
            self.nightshade.as_ref().unwrap().owner_id,
            receiver_id,
            GossipBody::PayloadReply(payloads),
            self.signer.clone(),
            self.block_index.unwrap(),
        );
        self.send_gossip(gossip);
    }

    fn request_payload_confirmation(&self, signed_payload: &SignedBlockProposal) {
        debug!("owner_uid={:?}, block_index={:?}, Request payload confirmation: {:?}", self.nightshade.as_ref().unwrap().owner_id, self.block_index, signed_payload);
        let authority = signed_payload.block_proposal.author;
        let hash = signed_payload.block_proposal.hash;
        let task = self.retrieve_payload_tx.clone().send((authority, hash)).map(|_| ()).map_err(
            move |_| error!("Failed to request confirmation for ({},{:?})", authority, hash),
        );
        tokio::spawn(task);
    }

    fn receive_payloads(&mut self, sender_id: AuthorityId, payloads: Vec<SignedBlockProposal>) {
        for signed_payload in payloads {
            let authority_id = signed_payload.block_proposal.author;

            // If the signed block is not properly signed by its author, we mark the sender as adversary.
            if !signed_payload.verify(&self.public_keys[authority_id]) {
                self.nightshade_as_mut_ref().set_adversary(sender_id);
                continue;
            }

            if let Some(ref p) = self.proposals[authority_id] {
                // If received proposal differs from our current proposal flag him as an adversary.
                if p.block_proposal.hash != signed_payload.block_proposal.hash {
                    self.nightshade_as_mut_ref().set_adversary(authority_id);
                    self.proposals[authority_id] = None;
                    panic!("the case of adversaries creating forks is not properly handled yet");
                }
            } else {
                self.request_payload_confirmation(&signed_payload);
                self.proposals[authority_id] = Some(signed_payload);
            }
        }
    }

    /// Sends gossip to random authority peers.
    fn gossip_state(&self) {
        let my_state = self.state();

        for i in 0..self.nightshade.as_ref().unwrap().num_authorities {
            if i != self.nightshade.as_ref().unwrap().owner_id {
                let message = Message {
                    sender_id: self.nightshade.as_ref().unwrap().owner_id,
                    receiver_id: i,
                    state: my_state.clone(),
                };
                self.send_state(message);
            }
        }
    }

    /// Helper functions to access values in `nightshade`. Used mostly to debug.

    fn state(&self) -> &State {
        self.nightshade_as_ref().state()
    }

    fn owner_id(&self) -> AuthorityId {
        self.nightshade_as_ref().owner_id
    }

    fn state_as_triplet(&self) -> (i64, AuthorityId, i64) {
        let state = self.state();
        (
            state.bare_state.primary_confidence,
            state.bare_state.endorses.author,
            state.bare_state.secondary_confidence,
        )
    }
}

impl Stream for NightshadeTask {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Control loop
        loop {
            match self.control_receiver.poll() {
                Ok(Async::Ready(Some(Control::Reset {
                    owner_uid,
                    block_index,
                    hash,
                    public_keys,
                    bls_public_keys,
                }))) => {
                    debug!(target: "nightshade",
                          "Control channel received Reset for owner_uid={}, block_index={}, hash={:?}",
                          owner_uid, block_index, hash);
                    self.init_nightshade(
                        owner_uid,
                        block_index,
                        hash,
                        public_keys,
                        bls_public_keys,
                    );
                    break;
                }
                Ok(Async::Ready(Some(Control::Stop))) => {
                    debug!(target: "nightshade", "Control channel received Stop");
                    if self.nightshade.is_some() {
                        self.nightshade = None;
                        // On the next call of poll if we still don't have the state this task will be
                        // parked because we will return NotReady.
                        continue;
                    }
                    // Otherwise loop until we encounter Reset command in the stream. If the stream
                    // is NotReady this will automatically park the task.
                }
                Ok(Async::Ready(Some(Control::PayloadConfirmation(authority_id, hash)))) => {
                    debug!(target: "nightshade", "Received confirmation for ({}, {:?})", authority_id, hash);
                    // Check that we got confirmation for the current proposal
                    // and not for a proposal in a previous block index
                    if let Some(signed_block) = &self.proposals[authority_id] {
                        if signed_block.block_proposal.hash == hash {
                            self.confirmed_proposals[authority_id] = true;
                        }
                    }

                    if !self.confirmed_proposals[authority_id] {
                        warn!(target: "nightshade", "Outdated confirmation.");
                    }
                }
                Ok(Async::Ready(None)) => {
                    debug!(target: "nightshade", "Control channel was dropped");
                    return Ok(Async::Ready(None));
                }
                Ok(Async::NotReady) => {
                    if self.nightshade.is_none() {
                        // If there is no state then we cannot proceed, we return NotReady which
                        // will park the task and wait until we get the state over the control
                        // channel.
                        return Ok(Async::NotReady);
                    }
                    // If there is a state then we do not care about the control.
                    break;
                }
                Err(err) => {
                    error!(target: "nightshade", "Failed to read from the control channel {:?}", err)
                }
            }
        }

        // Process new messages
        let mut end_of_messages = false;
        loop {
            match self.inc_gossips.poll() {
                Ok(Async::Ready(Some(gossip))) => {
                    let (elapsed, _) = measure_time(|| {
                        self.process_gossip(gossip);
                    });
                    debug!(target: "nightshade", "Node: {} Current state: {:?}, elapsed = {}", self.owner_id(), self.state_as_triplet(), elapsed);

                    // Report as soon as possible when an authority reach consensus on some outcome
                    if !self.consensus_reported {
                        if let Some(outcome) = self.nightshade_as_ref().committed.clone() {
                            self.consensus_reported = true;

                            if self.confirmed_proposals[outcome.author] {
                                tokio::spawn(
                                    self.consensus_sender
                                        .clone()
                                        .send(ConsensusBlockProposal {
                                            proposal: outcome,
                                            index: self.block_index.unwrap(),
                                        })
                                        .map(|_| ())
                                        .map_err(|e| error!("Failed sending consensus: {:?}", e)),
                                );
                            } else {
                                warn!(target: "nightshade", "Consensus reached on block proposal {} from {} that wasn't fetched.", outcome.hash, outcome.author);
                            }
                        }
                    }
                }
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(None)) => {
                    // End of the stream that feeds the messages.
                    end_of_messages = true;
                    break;
                }
                Err(err) => error!(target: "nightshade", "Failed to receive a gossip {:?}", err),
            }
        }

        // Send your state if the cooldown has passed
        if let Some(ref mut d) = self.cooldown_delay {
            try_ready!(d.poll().map_err(|e| error!("Cooldown timer error {}", e)));
        }

        self.gossip_state();

        let now = Instant::now();
        self.cooldown_delay = Some(Delay::new(now + Duration::from_millis(COOLDOWN_MS)));

        if end_of_messages {
            Ok(Async::Ready(None))
        } else {
            Ok(Async::Ready(Some(())))
        }
    }
}

pub fn spawn_nightshade_task(
    signer: Arc<BlockSigner>,
    inc_gossip_rx: mpsc::Receiver<Gossip>,
    out_gossip_tx: mpsc::Sender<Gossip>,
    consensus_tx: mpsc::Sender<ConsensusBlockProposal>,
    control_rx: mpsc::Receiver<Control>,
    retrieve_payload_tx: mpsc::Sender<(AuthorityId, CryptoHash)>,
) {
    let task = NightshadeTask::new(
        signer,
        inc_gossip_rx,
        out_gossip_tx,
        control_rx,
        consensus_tx,
        retrieve_payload_tx,
    );

    tokio::spawn(task.for_each(|_| Ok(())));
}
