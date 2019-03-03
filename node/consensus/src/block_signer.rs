use std::time::Duration;
use std::time::Instant;

use futures::Async;
use futures::Future;
use futures::Poll;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc::{Receiver, Sender};
use tokio::timer::Delay;

use primitives::aggregate_signature::BlsPublicKey;
use primitives::beacon::SignedBeaconBlock;
use primitives::block_traits::SignedBlock;
use primitives::chain::SignedShardBlock;
use primitives::types::AuthorityId;
use primitives::types::BlockIndex;
use primitives::types::PartialSignature;

const COOLDOWN_MS: u64 = 200;

struct Confirmation(SignedBeaconBlock, SignedShardBlock);

enum BlockSignerGossipBody {
    /// Signal to request signatures for Beacon/Shard blocks
    Request,
    /// Signatures of the Beacon/Shard blocks respectively
    Reply(PartialSignature, PartialSignature),
}

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
    beacon_signature: Option<PartialSignature>,
    shard_signature: Option<PartialSignature>,
    bls_public_keys: Vec<BlsPublicKey>,

    beacon_block: Option<SignedBeaconBlock>,
    shard_block: Option<SignedShardBlock>,

    num_authorities: Option<usize>,
    block_index: Option<BlockIndex>,

    inc_gossips_rx: Receiver<BlockSignerGossip>,
    out_gossips_tx: Sender<BlockSignerGossip>,
    confirmation_tx: Sender<Confirmation>,

    cooldown_delay: Option<Delay>,
}

impl BlockSignerTask {
    fn new(owner_id: AuthorityId,
           inc_gossips_rx: Receiver<BlockSignerGossip>,
           out_gossips_tx: Sender<BlockSignerGossip>,
           confirmation_tx: Sender<Confirmation>) -> Self {
        Self {
            owner_id,
            beacon_signature: None,
            shard_signature: None,
            bls_public_keys: vec![],
            beacon_block: None,
            shard_block: None,
            num_authorities: None,
            block_index: None,
            inc_gossips_rx,
            out_gossips_tx,
            confirmation_tx,
            cooldown_delay: None,
        }
    }

    fn start(&mut self,
             beacon_signature: PartialSignature,
             shard_signature: PartialSignature,
             bls_public_keys: Vec<BlsPublicKey>,
             beacon_block: SignedBeaconBlock,
             shard_block: SignedShardBlock,
             block_index: BlockIndex,
    ) {
        // Hack to get `num_authorities`
        self.num_authorities = Some(beacon_block.signature.authority_mask.len());
        self.beacon_signature = Some(beacon_signature);
        self.shard_signature = Some(shard_signature);
        self.bls_public_keys = bls_public_keys;
        self.beacon_block = Some(beacon_block);
        self.shard_block = Some(shard_block);
        self.block_index = Some(block_index);

        self.reset_cooldown();

        for a in 0..*self.num_authorities.as_ref().unwrap() {
            if a != self.owner_id {
                self.send_signature(a);
            }
        }
    }

    fn stop(&mut self) {
        self.beacon_signature = None;
        self.shard_signature = None;
        self.bls_public_keys = vec![];
        self.beacon_block = None;
        self.shard_block = None;
        self.num_authorities = None;
        self.block_index = None;
        self.cooldown_delay = None;
    }

    fn has_signed(&self, authority: AuthorityId) -> bool {
        self.beacon_block.as_ref().unwrap().signature.authority_mask[authority]
    }

    fn can_confirm(&self) -> bool {
        let collected_signatures = self.beacon_block.as_ref().unwrap().signature.authority_count();
        collected_signatures > self.num_authorities.as_ref().unwrap() * 2 / 3
    }

    fn reset_cooldown(&mut self) {
        let now = Instant::now();
        self.cooldown_delay = Some(Delay::new(now + Duration::from_millis(COOLDOWN_MS)));
    }

    fn send_signature(&self, authority: AuthorityId) {
        let gossip = BlockSignerGossip {
            receiver_id: authority,
            sender_id: self.owner_id,
            body: BlockSignerGossipBody::Reply(
                self.beacon_signature.as_ref().unwrap().clone(),
                self.shard_signature.as_ref().unwrap().clone(),
            ),
            block_index: self.block_index.as_ref().unwrap().clone(),
        };

        tokio::spawn(self.out_gossips_tx
            .clone()
            .send(gossip)
            .map(|_| ())
            .map_err(|_| error!("Fail sending signature")));
    }

    fn send_gossip(&self, gossip: BlockSignerGossip) {
        let task = self.out_gossips_tx
            .clone()
            .send(gossip)
            .map(|_| ())
            .map_err(|e| error!("Fail to send gossip. {}", e));
        tokio::spawn(task);
    }

    fn verify_signatures(&self, author: AuthorityId, beacon_signature: &PartialSignature, shard_signature: &PartialSignature) -> bool {
        let pk = &self.bls_public_keys[author];

        let beacon_hash = self.beacon_block.as_ref().unwrap().hash.as_ref();
        let shard_hash = self.shard_block.as_ref().unwrap().hash.as_ref();

        pk.verify(beacon_hash, beacon_signature) && pk.verify(shard_hash, shard_signature)
    }

    fn process_gossip(&mut self, gossip: BlockSignerGossip) {
        if &gossip.block_index != self.block_index.as_ref().unwrap() {
            return ();
        }

        match gossip.body {
            BlockSignerGossipBody::Request => self.send_signature(gossip.sender_id),

            BlockSignerGossipBody::Reply(beacon_signature, shard_signature) => {
                let author = gossip.sender_id;

                if !self.verify_signatures(gossip.sender_id, &beacon_signature, &shard_signature) {
                    return ();
                }

                self.beacon_block.as_mut().unwrap().add_signature(&beacon_signature, author);
                self.shard_block.as_mut().unwrap().add_signature(&shard_signature, author);

                if self.can_confirm() {
                    let beacon_block = self.beacon_block.take().unwrap();
                    let shard_block = self.shard_block.take().unwrap();

                    let task = self.confirmation_tx
                        .clone()
                        .send(Confirmation(beacon_block, shard_block))
                        .map(|_| ())
                        .map_err(|e| error!("Fail sending confirmation. {}", e));
                    tokio::spawn(task);
                    self.stop();
                }
            }
        }
    }

    fn request_signature(&self) {
        for a in 0..*self.num_authorities.as_ref().unwrap() {
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

    /// Check if this task is currently waiting for signatures
    fn is_running(&self) -> bool {
        self.beacon_block.is_some()
    }
}

impl Stream for BlockSignerTask {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if !self.is_running() {
            return Ok(Async::NotReady);
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

#[cfg(test)]
mod test {
    #[test]
    fn test() {}
}