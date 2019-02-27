use std::fmt::Debug;
use std::time::Duration;
use std::time::Instant;

use futures::future::Future;
use futures::sink::Sink;
use futures::sync::mpsc;
use futures::try_ready;
use futures::Async;
use futures::Poll;
use futures::Stream;
use log::*;
use serde::Serialize;
use tokio::timer::Delay;

use primitives::aggregate_signature::BlsPublicKey;
use primitives::aggregate_signature::BlsSecretKey;
use primitives::hash::hash_struct;
use primitives::hash::CryptoHash;
use primitives::signature::{sign, verify, PublicKey, SecretKey, Signature};

use super::nightshade::{AuthorityId, Block, Nightshade, State, ConsensusBlockHeader};

const COOLDOWN_MS: u64 = 50;

pub enum Control<P> {
    Reset {
        owner_uid: u64,
        block_index: u64,
        payload: P,
        public_keys: Vec<PublicKey>,
        owner_secret_key: SecretKey,
        bls_public_keys: Vec<BlsPublicKey>,
        bls_owner_secret_key: BlsSecretKey,
    },
    Stop,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Message {
    pub sender_id: AuthorityId,
    pub receiver_id: AuthorityId,
    pub state: State,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum GossipBody<P> {
    /// Use box because large size difference between variants
    NightshadeStateUpdate(Box<Message>),
    PayloadRequest(Vec<AuthorityId>),
    PayloadReply(Vec<SignedBlock<P>>),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Gossip<P> {
    pub sender_id: AuthorityId,
    pub receiver_id: AuthorityId,
    pub body: GossipBody<P>,
    signature: Signature,
    block_index: u64,
}

impl<P: Serialize> Gossip<P> {
    fn new(
        sender_id: AuthorityId,
        receiver_id: AuthorityId,
        body: GossipBody<P>,
        sk: &SecretKey,
        block_index: u64,
    ) -> Self {
        let hash = hash_struct(&(sender_id, receiver_id, &body));

        Self { sender_id, receiver_id, body, signature: sign(hash.as_ref(), &sk), block_index }
    }

    fn get_hash(&self) -> CryptoHash {
        hash_struct(&(self.sender_id, self.receiver_id, &self.body))
    }

    fn verify(&self, pk: &PublicKey) -> bool {
        verify(self.get_hash().as_ref(), &self.signature, &pk)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedBlock<P> {
    block: Block<P>,
    signature: Signature,
}

impl<P: Serialize> SignedBlock<P> {
    fn new(author: AuthorityId, payload: P, secret_key: &SecretKey) -> Self {
        let block = Block::new(author, payload);
        let signature = sign(block.header.hash.as_ref(), &secret_key);

        Self { block, signature }
    }
}

impl<P: Serialize> SignedBlock<P> {
    fn verify(&self, public_key: &PublicKey) -> bool {
        verify(self.block.header.hash.as_ref(), &self.signature, &public_key)
    }
}

pub struct NightshadeTask<P> {
    /// Blocks from other authorities containing payloads. At the beginning of the consensus
    /// authorities only have their own block. It is required for an authority to endorse a block
    /// from other authority to have its block.
    authority_blocks: Vec<Option<SignedBlock<P>>>,
    nightshade: Option<Nightshade>,
    /// Block index that we are currently working on.
    block_index: Option<u64>,
    /// Standard public/secret keys are used to sign payloads and gossips
    public_keys: Vec<PublicKey>,
    owner_secret_key: Option<SecretKey>,
    /// Channel to receive gossips from other authorities.
    inc_gossips: mpsc::Receiver<Gossip<P>>,
    /// Channel to send gossips to other authorities
    out_gossips: mpsc::Sender<Gossip<P>>,
    /// Channel to start/reset consensus
    /// Important: Reset only works the first time it is sent.
    control_receiver: mpsc::Receiver<Control<P>>,
    /// Consensus header and block index on which the consensus was reached.
    consensus_sender: mpsc::Sender<ConsensusBlockHeader>,
    /// Flag to determine if consensus was already reported in the consensus channel.
    consensus_reported: bool,
    /// Number of payloads from other authorities that we still don't have.
    missing_payloads: usize,
    /// Timer that determines the minimum time that we should not gossip after the given message
    /// for the sake of not spamming the network with small packages.
    cooldown_delay: Option<Delay>,
}

impl<P: Send + Debug + Clone + Serialize + 'static> NightshadeTask<P> {
    pub fn new(
        inc_gossips: mpsc::Receiver<Gossip<P>>,
        out_gossips: mpsc::Sender<Gossip<P>>,
        control_receiver: mpsc::Receiver<Control<P>>,
        consensus_sender: mpsc::Sender<ConsensusBlockHeader>,
    ) -> Self {
        Self {
            authority_blocks: vec![],
            nightshade: None,
            block_index: None,
            public_keys: vec![],
            owner_secret_key: None,
            inc_gossips,
            out_gossips,
            control_receiver,
            consensus_sender,
            consensus_reported: false,
            missing_payloads: 0,
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
        owner_uid: u64,
        block_index: u64,
        payload: P,
        public_keys: Vec<PublicKey>,
        owner_secret_key: SecretKey,
        bls_public_keys: Vec<BlsPublicKey>,
        bls_owner_secret_key: BlsSecretKey,
    ) {
        let num_authorities = public_keys.len();
        self.public_keys = public_keys;
        self.owner_secret_key = Some(owner_secret_key.clone());
        self.block_index = Some(block_index);
        self.missing_payloads = num_authorities - 1;
        self.authority_blocks = vec![None; num_authorities];
        self.authority_blocks[owner_uid as usize] =
            Some(SignedBlock::new(owner_uid as usize, payload, &owner_secret_key));
        self.consensus_reported = false;
        self.nightshade = Some(Nightshade::new(
            owner_uid as AuthorityId,
            num_authorities,
            self.authority_blocks[owner_uid as usize].clone().unwrap().block.header,
            bls_public_keys,
            bls_owner_secret_key,
        ));
    }

    fn send_state(&self, message: Message) {
        self.send_gossip(Gossip::new(
            self.nightshade.as_ref().unwrap().owner_id,
            message.receiver_id,
            GossipBody::NightshadeStateUpdate(Box::new(message)),
            self.owner_secret_key.as_ref().unwrap(),
            self.block_index.unwrap(),
        ));
    }

    fn send_gossip(&self, message: Gossip<P>) {
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

        // Check we already have such payload and request it otherwise
        if let Some(signed_block) = &self.authority_blocks[author] {
            if signed_block.block.hash() == message.state.block_hash() {
                if let Err(e) =
                    self.nightshade_as_mut_ref().update_state(message.sender_id, message.state)
                {
                    warn!(target: "nightshade", "{}", e);
                }
            } else {
                self.nightshade_as_mut_ref().set_adversary(author);
            }
        } else {
            // TODO: This message is discarded if we haven't receive the payload yet. We can store it
            // in a queue instead, and process it after we have the payload.

            let gossip = Gossip::new(
                self.nightshade.as_ref().unwrap().owner_id,
                author,
                GossipBody::PayloadRequest(vec![author]),
                self.owner_secret_key.as_ref().unwrap(),
                self.block_index.unwrap(),
            );
            self.send_gossip(gossip);
        }
    }

    fn process_gossip(&mut self, gossip: Gossip<P>) {
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
            if let Some(ref p) = self.authority_blocks[a] {
                payloads.push(p.clone());
            }
        }
        let gossip = Gossip::new(
            self.nightshade.as_ref().unwrap().owner_id,
            receiver_id,
            GossipBody::PayloadReply(payloads),
            self.owner_secret_key.as_ref().unwrap(),
            self.block_index.unwrap(),
        );
        self.send_gossip(gossip);
    }

    fn receive_payloads(&mut self, sender_id: AuthorityId, payloads: Vec<SignedBlock<P>>) {
        for signed_payload in payloads {
            let authority_id = signed_payload.block.author();

            // If the signed block is not properly signed by its author, we mark the sender as adversary.
            if !signed_payload.verify(&self.public_keys[authority_id]) {
                self.nightshade_as_mut_ref().set_adversary(sender_id);
                continue;
            }

            if let Some(ref p) = self.authority_blocks[authority_id] {
                if p.block.hash() != signed_payload.block.hash() {
                    self.nightshade_as_mut_ref().set_adversary(authority_id);
                    self.authority_blocks[authority_id] = None;
                }
            } else {
                self.authority_blocks[authority_id] = Some(signed_payload);
                self.missing_payloads -= 1;
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

    /// We need to have the payload for anything we want to endorse
    /// TODO do it in a smarter way
    fn collect_missing_payloads(&self) {
        for authority in 0..self.nightshade.as_ref().unwrap().num_authorities {
            if self.authority_blocks[authority].is_none() {
                let gossip = Gossip::new(
                    self.nightshade.as_ref().unwrap().owner_id,
                    authority,
                    GossipBody::PayloadRequest(vec![authority]),
                    self.owner_secret_key.as_ref().unwrap(),
                    self.block_index.unwrap(),
                );
                self.send_gossip(gossip);
            }
        }
    }

    /// Helper functions to access values in `nightshade`. Used mostly to debug.

    fn state(&self) -> &State {
        self.nightshade_as_ref().state()
    }

    fn owner_id(&self) -> AuthorityId { self.nightshade_as_ref().owner_id }

    fn state_as_triplet(&self) -> (i64, AuthorityId, i64) {
        let state= self.state();
        (state.bare_state.primary_confidence, state.bare_state.endorses.author, state.bare_state.secondary_confidence)
    }
}

impl<P: Send + Debug + Clone + Serialize + 'static> Stream for NightshadeTask<P> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Control loop
        loop {
            match self.control_receiver.poll() {
                Ok(Async::Ready(Some(Control::Reset {
                    owner_uid,
                    block_index,
                    payload,
                    public_keys,
                    owner_secret_key,
                    bls_public_keys,
                    bls_owner_secret_key,
                }))) => {
                    info!(target: "nightshade", "Control channel received Reset");
                    self.init_nightshade(
                        owner_uid,
                        block_index,
                        payload,
                        public_keys,
                        owner_secret_key,
                        bls_public_keys,
                        bls_owner_secret_key,
                    );
                    break;
                }
                Ok(Async::Ready(Some(Control::Stop))) => {
                    info!(target: "nightshade", "Control channel received Stop");
                    if self.nightshade.is_some() {
                        self.nightshade = None;
                        // On the next call of poll if we still don't have the state this task will be
                        // parked because we will return NotReady.
                        continue;
                    }
                    // Otherwise loop until we encounter Reset command in the stream. If the stream
                    // is NotReady this will automatically park the task.
                }
                Ok(Async::Ready(None)) => {
                    info!(target: "nightshade", "Control channel was dropped");
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
                    self.process_gossip(gossip);
                    debug!(target: "nightshade", "Node: {} Current state: {:?}", self.owner_id(), self.state_as_triplet());

                    // Report as soon as possible when an authority reach consensus on some outcome
                    if !self.consensus_reported {
                        if let Some(outcome) = self.nightshade_as_ref().committed.clone() {
                            self.consensus_reported = true;

                            tokio::spawn(
                                self.consensus_sender
                                    .clone()
                                    .send(ConsensusBlockHeader { header: outcome, index: self.block_index.unwrap() })
                                    .map(|_| ())
                                    .map_err(|e| error!("Failed sending consensus: {:?}", e)),
                            );
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

        if self.missing_payloads > 0 {
            self.collect_missing_payloads();
        }

        let now = Instant::now();
        self.cooldown_delay = Some(Delay::new(now + Duration::from_millis(COOLDOWN_MS)));

        if end_of_messages {
            Ok(Async::Ready(None))
        } else {
            Ok(Async::Ready(Some(())))
        }
    }
}

pub fn spawn_nightshade_task<P>(
    inc_gossip_rx: mpsc::Receiver<Gossip<P>>,
    out_gossip_tx: mpsc::Sender<Gossip<P>>,
    consensus_tx: mpsc::Sender<ConsensusBlockHeader>,
    control_rx: mpsc::Receiver<Control<P>>,
) where
    P: Serialize + Send + Clone + Debug + 'static,
{
    let task = NightshadeTask::new(inc_gossip_rx, out_gossip_tx, control_rx, consensus_tx);

    tokio::spawn(task.for_each(|_| Ok(())));
}
