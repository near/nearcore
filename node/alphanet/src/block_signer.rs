use primitives::chain::SignedShardBlock;
use primitives::beacon::SignedBeaconBlock;
use tokio::timer::Delay;
use primitives::types::AuthorityId;
use primitives::types::PartialSignature;
use primitives::types::BlockIndex;
use futures::sync::mpsc::{Receiver, Sender};
use futures::Future;
use std::time::Instant;
use std::time::Duration;
use futures::stream::Stream;
use futures::Async;
use futures::Poll;
use nightshade::nightshade_task::GossipBody;
use nightshade::nightshade_task::Gossip;
use futures::sink::Sink;
use primitives::block_traits::SignedBlock;

const COOLDOWN_MS: u64 = 200;

struct Confirmation(SignedBeaconBlock, SignedShardBlock);

enum BlockSignerGossipBody {
    Request,
    Reply(PartialSignature),
}

enum BlockSignerControl {
    Reset(SignedBeaconBlock, SignedShardBlock, BlockIndex),
    Stop,
}

/// TODO: Plug this task in alphanet.

/// TODO: Gossips are not signed, because we are only exchanging BLS signature. One edge of attack
/// might be spam a node with invalid BLS signature (BLS is slow and checking for invalid signature
/// is expensive).
struct BlockSignerGossip {
    receiver_id: AuthorityId,
    sender_id: AuthorityId,
    body: BlockSignerGossipBody,
    block_index: BlockIndex,
}

struct BlockSignerTask {
    owner_id: AuthorityId,

    beacon_block: Option<SignedBeaconBlock>,
    shard_block: Option<SignedShardBlock>,

    num_authorities: Option<usize>,
    block_index: Option<BlockIndex>,

    control_rx: Receiver<BlockSignerControl>,
    inc_gossips_rx: Receiver<BlockSignerGossip>,
    out_gossips_tx: Sender<BlockSignerGossip>,
    confirmation_tx: Sender<Confirmation>,

    cooldown_delay: Option<Delay>,
    has_confirmed: bool,
}

impl BlockSignerTask {
    fn new(owner_id: AuthorityId,
           control_rx: Receiver<BlockSignerControl>,
           inc_gossips_rx: Receiver<BlockSignerGossip>,
           out_gossips_tx: Sender<BlockSignerGossip>,
           confirmation_tx: Sender<Confirmation>) -> Self {
        Self {
            owner_id,
            beacon_block: None,
            shard_block: None,
            num_authorities: None,
            block_index: None,
            control_rx,
            inc_gossips_rx,
            out_gossips_tx,
            confirmation_tx,
            cooldown_delay: None,
            has_confirmed: false,
        }
    }

    fn init_block_signer(&mut self,
                         beacon_block: SignedBeaconBlock,
                         shard_block: SignedShardBlock,
                         block_index: BlockIndex,
    ) {
        self.beacon_block = Some(beacon_block);
        self.shard_block = Some(shard_block);
        // Hacky way to get num_authorities
        self.num_authorities = Some(beacon_block.signature.authority_mask.len());
        self.block_index = Some(block_index);
        self.has_confirmed = false;

        self.reset_cooldown();

        for a in 0..self.num_authorities {
            if a != self.owner_id {
                self.send_signature(a);
            }
        }
    }

    fn has_signed(&self, authority: AuthorityId) -> bool {
        if let Some(beacon_block) = &self.beacon_block {
            beacon_block.signature.authority_mask[authority]
        } else {
            panic!("Block signer task is not running");
        }
    }

    // TODO:
    fn can_confirm(&self) -> bool {

    }

    fn reset_cooldown(&mut self) {
        let now = Instant::now();
        self.cooldown_delay = Some(Delay::new(now + Duration::from_millis(COOLDOWN_MS)));
    }

    // TODO:
    fn send_signature(&self, authority: AuthorityId) {
        
    }

    fn send_gossip(&self, gossip: BlockSignerGossip) {
        let task = self.out_gossips_tx
            .clone()
            .send(gossip)
            .map(|_| ())
            .map_err(|e| error!("Fail to send gossip"));
        tokio::spawn(task);
    }

    fn process_gossip(&mut self, gossip: BlockSignerGossip) {
        if gossip.block_index != self.block_index.as_ref().unwrap() {
            return ();
        }

        match gossip.body {
            BlockSignerGossipBody::Request => self.send_signature(gossip.sender_id),

            BlockSignerGossipBody::Reply(signature) => {
                if !self.has_confirmed {
                    self.beacon_block.as_mut().unwrap().add_signature(&signature);
                    self.shard_block.as_mut().unwrap().add_signature(&signature);

                    if self.can_confirm() {
                        self.has_confirmed = true;
                        let task = self.confirmation_tx
                            .send(Confirmation(self.beacon_block.unwrap(), self.shard_block.unwrap()))
                            .map(|_| ())
                            .map_err(|e| error!("Fail sending confirmation"));
                        tokio::spawn(task);
                        self.stop();
                    }
                }
            }
        }
    }

    fn request_signature(&self) {
        for a in 0..self.num_authorities {
            if !self.has_signed(a) {
                let gossip = BlockSignerGossip {
                    sender_id: self.owner_id,
                    receiver_id: a,
                    body: BlockSignerGossipBody::Request,
                    block_index: self.block_index.as_ref().unwrap().clone(),
                };
                self.send_gossip(gossip);
            }
        }
    }

    fn stop(&mut self) {
        self.beacon_block = None;
        self.shard_block = None;
        self.num_authorities = None;
        self.block_index = None;
        self.cooldown_delay = None;
    }

    fn is_running(&self) -> bool {
        self.beacon_block.is_some()
    }
}

impl Future for BlockSignerTask {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match self.control_rx.poll() {
                Ok(Async::Ready(Some(BlockSignerControl::Reset(signed_block, shard_block, block_index)))) => {
                    self.init_block_signer(signed_block, shard_block, block_index);
                    break;
                }
                Ok(Async::Ready(Some(BlockSignerControl::Stop))) => {
                    self.stop();
                    continue;
                }
                Ok(Async::Ready(None)) => {
                    warn!(target: "block_signer", "Control channel was dropped");
                    return Ok(Async::Ready(None));
                }
                Ok(Async::NotReady) => {
                    if self.is_running() {
                        // If there is a state then we do not care about the control.
                        break;
                    } else {
                        // If it is no running then we cannot proceed, we return NotReady which
                        // will park the task and wait until we get the state over the control
                        // channel.
                        return Ok(Async::NotReady);
                    }
                }
                Err(err) => {
                    error!(target: "block_signer", "Failed to read from the control channel {:?}", err)
                }
            }
        }

        loop {
            match self.inc_gossips_rx.poll() {
                Ok(Async::Ready(Some(gossip))) => {
                    self.process_gossip(gossip);
                }
                Ok(Async::Ready(None)) => {
                    // Incoming gossips channel was dropped
                    return Ok(Async::Ready(None));
                }
                Ok(Async::NotReady) => break,
                Err(err) => error!(target: "block_signer", "Failed to receive a gossip {:?}", err),
            }
        }

        // Send your state if the cooldown has passed
        if let Some(ref mut d) = self.cooldown_delay {
            try_ready!(d.poll().map_err(|e| error!("Cooldown timer error {}", e)));
        }

        self.request_signature();
        self.reset_cooldown();

        Ok(Async::Ready(Some(())))
    }
}
