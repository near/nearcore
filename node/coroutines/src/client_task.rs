use client::BlockImportingResult;
use client::BlockProductionResult;
use client::Client;
use futures::future::Future;
use futures::sink::Sink;
use futures::sync::mpsc::Receiver;
use futures::sync::mpsc::Sender;
use futures::Async;
use futures::Poll;
use futures::Stream;
use nightshade::nightshade::ConsensusBlockProposal;
use nightshade::nightshade_task::Control;
use primitives::aggregate_signature::BlsSignature;
use primitives::beacon::SignedBeaconBlock;
use primitives::block_traits::SignedBlock;
use primitives::chain::ChainPayload;
use primitives::chain::PayloadRequest;
use primitives::chain::PayloadResponse;
use primitives::chain::SignedShardBlock;
use primitives::consensus::JointBlockBLS;
use primitives::hash::CryptoHash;
use primitives::signer::BlockSigner;
use primitives::types::AccountId;
use primitives::types::AuthorityId;
use primitives::types::BlockId;
use primitives::types::BlockIndex;
use primitives::types::PartialSignature;
use shard::ShardBlockExtraInfo;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::timer::Interval;

pub struct ClientTask {
    client: Arc<Client>,
    /// Incoming blocks produced by other peer that might be imported by this peer.
    incoming_block_rx: Receiver<(SignedBeaconBlock, SignedShardBlock)>,
    /// Outgoing blocks produced by this peer that can be imported by other peers.
    out_block_tx: Sender<(SignedBeaconBlock, SignedShardBlock)>,
    /// Consensus created by the current instance of Nightshade or pass-through consensus.
    consensus_rx: Receiver<ConsensusBlockProposal>,
    /// Control that we send to the consensus task.
    control_tx: Sender<Control>,
    /// Request from the Nightshade task to retrieve the payload.
    retrieve_payload_rx: Receiver<(AuthorityId, CryptoHash)>,
    /// Request from mempool task to the peer to return the payload.
    payload_request_tx: Sender<PayloadRequest>,
    /// Responses from the peers with the payload.
    payload_response_rx: Receiver<PayloadResponse>,

    // Periodic tasks.
    /// Channel into which we announce payloads.
    payload_announce_tx: Sender<(AuthorityId, ChainPayload)>,
    /// Interval at which we announce the payload.
    payload_announce_int: Interval,
    /// Sends partial signatures of beacon and shard blocks down this channel.
    final_signatures_tx: Sender<JointBlockBLS>,
    /// Receives partial signatures of beacon and shard blocks from this channel.
    final_signatures_rx: Receiver<JointBlockBLS>,
    final_signatures_int: Interval,

    // Internal containers.
    /// Blocks for which the consensus was achieved for the set of transactions, but the computed
    /// state was not yet double-signed with BLS.
    unfinalized_beacon_blocks: HashMap<CryptoHash, SignedBeaconBlock>,
    unfinalized_shard_blocks: HashMap<CryptoHash, (SignedShardBlock, ShardBlockExtraInfo)>,
}

impl Stream for ClientTask {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut new_block_index = None;
        let mut block_production_ended = false;
        let mut block_importing_ended = false;
        let mut retrieve_payload_ended = false;
        let mut payload_response_ended = false;
        let mut final_signatures_ended = false;
        // We exit this loop when for each polled channel it has either ended or it is not ready.
        loop {
            match self.consensus_rx.poll() {
                Ok(Async::Ready(Some(c))) => {
                    if c.index == self.client.beacon_chain.chain.best_index() + 1 {
                        let (beacon_block, shard_block, shard_extra) = self.prepare_block(c);
                        let beacon_hash = beacon_block.hash;
                        let shard_hash = shard_block.hash;
                        self.unfinalized_beacon_blocks.insert(beacon_block.hash, beacon_block);
                        self.unfinalized_shard_blocks
                            .insert(shard_block.hash, (shard_block, shard_extra));
                        if let idx @ Some(_) = self.try_import_produced(beacon_hash, shard_hash) {
                            new_block_index = idx;
                        }
                    } else {
                        info!(target: "client", "Ignoring consensus for {} because current block index is {}",
                              c.index, self.client.beacon_chain.chain.best_index());
                    }
                    continue;
                }
                Ok(Async::Ready(None)) => {
                    block_production_ended = true;
                }
                Ok(Async::NotReady) => (),
                Err(_) => {
                    warn!(target: "client", "Error reading from consensus stream");
                    continue;
                }
            }

            match self.incoming_block_rx.poll() {
                Ok(Async::Ready(Some(b))) => {
                    if let ind @ Some(_) = self.try_import_block(b.0, b.1) {
                        new_block_index = ind;
                    }
                    continue;
                }
                Ok(Async::Ready(None)) => {
                    block_importing_ended = true;
                }
                Ok(Async::NotReady) => (),
                Err(_) => {
                    warn!(target: "client", "Error reading from incoming blocks");
                    continue;
                }
            }

            match self.retrieve_payload_rx.poll() {
                Ok(Async::Ready(Some((id, hash)))) => {
                    self.get_or_request_payload(id, hash);
                    continue;
                }
                Ok(Async::Ready(None)) => {
                    retrieve_payload_ended = true;
                }
                Ok(Async::NotReady) => (),
                Err(_) => {
                    warn!(target: "client", "Error reading retrieve payload channel");
                    continue;
                }
            }

            match self.payload_response_rx.poll() {
                Ok(Async::Ready(Some(r))) => {
                    self.process_payload_response(r);
                    continue;
                }
                Ok(Async::Ready(None)) => {
                    payload_response_ended = true;
                }
                Ok(Async::NotReady) => (),
                Err(_) => {
                    warn!(target: "client", "Error reading payload response channel");
                    continue;
                }
            }

            match self.payload_announce_int.poll() {
                Ok(Async::Ready(Some(_))) => {
                    self.announce_payload();
                    continue;
                }
                Ok(Async::Ready(None)) => {
                    panic!("Interval stream is not expected to ever end");
                }
                Ok(Async::NotReady) => (),
                Err(e) => {
                    warn!(target: "client", "Interval stream error {}", e);
                    continue;
                }
            }

            match self.final_signatures_int.poll() {
                Ok(Async::Ready(Some(_))) => {
                    for (beacon_hash, beacon_block) in self.unfinalized_beacon_blocks.iter() {
                        let (owner_uid, _) =
                            self.client.get_uid_to_authority_map(beacon_block.index());
                        let owner_uid = match owner_uid {
                            Some(id) => id,
                            None => continue,
                        };
                        for (auth_id, auth_present) in
                            beacon_block.signature.authority_mask.iter().enumerate()
                        {
                            if !*auth_present {
                                tokio::spawn(
                                    self.final_signatures_tx
                                        .clone()
                                        .send(JointBlockBLS::Request {
                                            beacon_hash: *beacon_hash,
                                            shard_hash: beacon_block.body.header.shard_block_hash,
                                            sender_id: owner_uid,
                                            receiver_id: auth_id,
                                        })
                                        .map(|_| ())
                                        .map_err(
                                            |e| error!(target: "client", "Error sending final BLS parts: {}", e),
                                        ),
                                );
                            }
                        }
                    }
                }
                Ok(Async::Ready(None)) => {
                    panic!("Interval stream is not expected to ever end");
                }
                Ok(Async::NotReady) => (),
                Err(e) => {
                    warn!(target: "client", "Interval stream error {}", e);
                    continue;
                }
            }

            match self.final_signatures_rx.poll() {
                Ok(Async::Ready(Some(JointBlockBLS::General {
                    beacon_hash,
                    shard_hash,
                    beacon_sig,
                    shard_sig,
                    sender_id,
                    receiver_id,
                }))) => {
                    if let idx @ Some(_) = self.try_add_signatures(
                        beacon_hash,
                        shard_hash,
                        beacon_sig,
                        shard_sig,
                        sender_id,
                    ) {
                        new_block_index = idx;
                    }
                }
                Ok(Async::Ready(Some(JointBlockBLS::Request {
                    beacon_hash,
                    shard_hash,
                    sender_id,
                    receiver_id,
                }))) => {
                    self.reply_with_bls(beacon_hash, shard_hash, sender_id, receiver_id);
                }
                Ok(Async::Ready(None)) => {
                    final_signatures_ended = true;
                }
                Ok(Async::NotReady) => (),
                Err(_) => {
                    continue;
                }
                _ => (),
            }

            // If we reached here than each channel is either not ready or has ended.
            break;
        }

        // First, send the control, if applicable.
        if let Some(ind) = new_block_index {
            let hash = self.client.shard_client.pool.snapshot_payload();
            let next_index = ind + 1;
            let control = self.get_control(next_index, hash);
            tokio::spawn(
                self.control_tx
                    .clone()
                    .send(control)
                    .map(|_| ())
                    .map_err(|e| error!(target: "client", "Error sending control: {}", e)),
            );
        }

        // Second, decide on whether to return not ready or stop the stream.
        if block_production_ended
            && block_importing_ended
            && retrieve_payload_ended
            && payload_response_ended
            && final_signatures_ended
        {
            Ok(Async::Ready(None))
        } else {
            Ok(Async::NotReady)
        }
    }
}

impl ClientTask {
    pub fn new(
        gossip_interval: Duration,
        client: Arc<Client>,
        incoming_block_rx: Receiver<(SignedBeaconBlock, SignedShardBlock)>,
        out_block_tx: Sender<(SignedBeaconBlock, SignedShardBlock)>,
        consensus_rx: Receiver<ConsensusBlockProposal>,
        control_tx: Sender<Control>,
        retrieve_payload_rx: Receiver<(AuthorityId, CryptoHash)>,
        payload_request_tx: Sender<PayloadRequest>,
        payload_response_rx: Receiver<PayloadResponse>,
        payload_announce_tx: Sender<(AuthorityId, ChainPayload)>,
        final_signatures_tx: Sender<JointBlockBLS>,
        final_signatures_rx: Receiver<JointBlockBLS>,
    ) -> Self {
        let res = Self {
            client,
            incoming_block_rx,
            out_block_tx,
            consensus_rx,
            control_tx,
            retrieve_payload_rx,
            payload_request_tx,
            payload_response_rx,
            payload_announce_tx,
            payload_announce_int: Interval::new_interval(gossip_interval),
            unfinalized_beacon_blocks: Default::default(),
            unfinalized_shard_blocks: Default::default(),
            final_signatures_tx,
            final_signatures_rx,
            final_signatures_int: Interval::new_interval(gossip_interval),
        };
        res.spawn_kickoff();
        res
    }

    /// Tries producing block from the given consensus. If succeeds returns index of the block with
    /// the highest index.
    fn prepare_block(
        &mut self,
        consensus_block_header: ConsensusBlockProposal,
    ) -> (SignedBeaconBlock, SignedShardBlock, ShardBlockExtraInfo) {
        info!(target: "consensus", "Producing block for account_id={:?}, index {}", self.client.account_id, consensus_block_header.index);
        let payload = match self
            .client
            .shard_client
            .pool
            .pop_payload_snapshot(&consensus_block_header.proposal.hash)
        {
            Some(p) => p,
            None => {
                // Assumption: This else should never be reached, if an authority achieves consensus on some block hash
                // then it is because this block can be retrieved from the mempool.
                panic!(
                    "Authority: {} Failed to find payload for {} from authority {}",
                    self.client.account_id,
                    consensus_block_header.proposal.hash,
                    consensus_block_header.proposal.author
                );
            }
        };
        let (mut beacon_block, mut shard_block, shard_extra) =
            self.client.prepare_block(consensus_block_header.index, payload);
        let (owner_uid, mapping) =
            self.client.get_uid_to_authority_map(consensus_block_header.index);
        if let Some(o) = owner_uid {
            let beacon_sig = beacon_block.sign(self.client.signer.clone());
            beacon_block.add_signature(&beacon_sig, o);
            let shard_sig = shard_block.sign(self.client.signer.clone());
            shard_block.add_signature(&shard_sig, o);
            for other_id in mapping.keys() {
                if *other_id == o {
                    continue;
                }
                tokio::spawn(
                    self.final_signatures_tx
                        .clone()
                        .send(JointBlockBLS::General {
                            beacon_hash: beacon_block.hash,
                            shard_hash: shard_block.hash,
                            beacon_sig: beacon_sig.clone(),
                            shard_sig: shard_sig.clone(),
                            sender_id: o,
                            receiver_id: *other_id,
                        })
                        .map(|_| ())
                        .map_err(
                            |e| error!(target: "client", "Error sending final BLS parts: {}", e),
                        ),
                );
            }
        }
        (beacon_block, shard_block, shard_extra)
    }

    /// Imports produced block.
    fn try_import_produced(
        &mut self,
        beacon_hash: CryptoHash,
        shard_hash: CryptoHash,
    ) -> Option<u64> {
        // Check if it has sufficient number of signatures.
        let beacon_block = match self.unfinalized_beacon_blocks.get(&beacon_hash) {
            Some(b) => b,
            None => return None,
        };
        let idx = beacon_block.index();
        let num_authorities = self.client.get_uid_to_authority_map(idx).1.len();
        let present: usize =
            beacon_block.signature.authority_mask.iter().map(|b| *b as usize).sum();
        if present < 2 * num_authorities / 3 + 1 {
            println!("AAAA {} vs {}", present, num_authorities);
            return None;
        }

        let beacon_block = self.unfinalized_beacon_blocks.remove(&beacon_hash).unwrap();
        let (shard_block, shard_block_info) =
            self.unfinalized_shard_blocks.remove(&shard_hash).unwrap();
        tokio::spawn(
            self.out_block_tx
                .clone()
                .send((beacon_block.clone(), shard_block.clone()))
                .map(|_| ())
                .map_err(|e| error!(target: "client", "Error sending block announcement: {}", e)),
        );
        self.client.try_import_produced(beacon_block, shard_block, shard_block_info);
        Some(self.client.beacon_chain.chain.best_block().index())
    }

    /// Tries importing block. If succeeds returns index of the block with the highest index.
    fn try_import_block(
        &mut self,
        beacon_block: SignedBeaconBlock,
        shard_block: SignedShardBlock,
    ) -> Option<BlockIndex> {
        if let BlockImportingResult::Success { new_index } =
            self.client.try_import_blocks(beacon_block, shard_block)
        {
            info!(
                "Successfully imported block(s) up to {}, account_id={:?}",
                new_index, self.client.account_id
            );
            let mut beacons_to_remove = vec![];
            let mut shards_to_remove = vec![];
            for (h, b) in &self.unfinalized_beacon_blocks {
                if b.index() <= new_index {
                    beacons_to_remove.push(*h);
                    shards_to_remove.push(b.body.header.shard_block_hash);
                }
            }
            for h in beacons_to_remove {
                self.unfinalized_beacon_blocks.remove(&h);
            }
            for h in shards_to_remove {
                self.unfinalized_shard_blocks.remove(&h);
            }
            Some(new_index)
        } else {
            None
        }
    }

    /// Try adding partial signatures to unfinalized block. If succeeds and the number of authorities
    /// is sufficient returns  index of the block with the highest index.
    fn try_add_signatures(
        &mut self,
        beacon_hash: CryptoHash,
        shard_hash: CryptoHash,
        beacon_sig: BlsSignature,
        shard_sig: BlsSignature,
        authority_id: AuthorityId,
    ) -> Option<u64> {
        let beacon_block = match self.unfinalized_beacon_blocks.get_mut(&beacon_hash) {
            Some(b) => b,
            _ => return None,
        };

        let (shard_block, _) = match self.unfinalized_shard_blocks.get_mut(&shard_hash) {
            Some(b) => b,
            _ => return None,
        };
        let block_index = beacon_block.index();
        // Make sure this authority is actually supposed to sign this block.
        let (_, mapping) = self.client.get_uid_to_authority_map(block_index);
        let stake = match mapping.get(&authority_id) {
            Some(s) => s,
            None => return None,
        };
        // Make sure the signature is correct.
        if !stake.bls_public_key.verify(beacon_hash.as_ref(), &beacon_sig)
            || !stake.bls_public_key.verify(shard_hash.as_ref(), &shard_sig)
        {
            return None;
        }
        beacon_block.signature.add_signature(&beacon_sig, authority_id);
        shard_block.signature.add_signature(&shard_sig, authority_id);
        self.try_import_produced(beacon_hash, shard_hash)
    }

    fn get_or_request_payload(&self, authority_id: AuthorityId, hash: CryptoHash) {
        let pool = &self.client.shard_client.pool;
        info!(
            target: "mempool",
            "Payload confirmation for {} from {}",
            hash,
            authority_id,
        );
        if !pool.contains_payload_snapshot(&hash) {
            tokio::spawn(
                self.payload_request_tx
                    .clone()
                    .send(PayloadRequest::BlockProposal(authority_id, hash))
                    .map(|_| ())
                    .map_err(|e| warn!(target: "mempool", "Error sending message: {}", e)),
            );
            pool.add_pending(authority_id, hash);
        } else {
            let send_confirmation = self
                .control_tx
                .clone()
                .send(Control::PayloadConfirmation(authority_id, hash))
                .map(|_| ())
                .map_err(
                    |_| error!(target: "mempool", "Fail sending control signal to nightshade"),
                );
            tokio::spawn(send_confirmation);
        }
    }

    /// Process incoming payload response.
    fn process_payload_response(&self, payload_response: PayloadResponse) {
        let pool = &self.client.shard_client.pool;
        if let Err(e) = match payload_response {
            PayloadResponse::General(payload) => pool.add_payload(payload),
            PayloadResponse::BlockProposal(authority_id, payload) => {
                pool.add_payload_snapshot(authority_id, payload)
            }
        } {
            warn!(target: "mempool", "Failed to add incoming payload: {}", e);
        }

        for (authority_id, hash) in pool.ready_snapshots() {
            let send_confirmation = self
                .control_tx
                .clone()
                .send(Control::PayloadConfirmation(authority_id, hash))
                .map(|_| ())
                .map_err(
                    |_| error!(target: "mempool", "Fail sending control signal to nightshade"),
                );
            tokio::spawn(send_confirmation);
        }
    }

    /// Returns control.
    fn get_control(&self, block_index: u64, proposal_hash: CryptoHash) -> Control {
        // TODO: Get authorities for the correct block index. For now these are the same authorities
        // that built the first block. In other words use `block_index` instead of `mock_block_index`.
        let mock_block_index = 2;
        let (owner_uid, uid_to_authority_map) =
            self.client.get_uid_to_authority_map(mock_block_index);
        if owner_uid.is_none() {
            return Control::Stop;
        }
        let owner_uid = owner_uid.unwrap();
        let num_authorities = uid_to_authority_map.len();

        let mut public_keys = vec![];
        let mut bls_public_keys = vec![];
        for i in 0..num_authorities {
            info!(
                "Authority #{}: account_id={:?} me={} block_index={}",
                i,
                uid_to_authority_map[&i].account_id,
                i == owner_uid,
                block_index
            );
            public_keys.push(uid_to_authority_map[&i].public_key);
            bls_public_keys.push(uid_to_authority_map[&i].bls_public_key.clone());
        }

        Control::Reset { owner_uid, block_index, hash: proposal_hash, public_keys, bls_public_keys }
    }

    fn announce_payload(&self) {
        let pool = &self.client.shard_client.pool;
        if let Some((authority_id, payload)) = pool.prepare_payload_announce() {
            info!(
                target: "mempool",
                "Payload confirmed from {}",
                authority_id
            );
            tokio::spawn(
                self.payload_announce_tx
                    .clone()
                    .send((authority_id, payload))
                    .map(|_| ())
                    .map_err(|e| warn!(target: "mempool", "Error sending message: {}", e)),
            );
        }
    }

    fn reply_with_bls(
        &self,
        beacon_hash: CryptoHash,
        shard_hash: CryptoHash,
        sender_id: AuthorityId,
        receiver_id: AuthorityId,
    ) {
        if !self.unfinalized_shard_blocks.contains_key(&shard_hash)
            && !self.client.shard_client.chain.is_known(&shard_hash)
        {
            return;
        }
        let block_idx = match self.unfinalized_beacon_blocks.get(&beacon_hash) {
            Some(b) => b.index(),
            None => match self.client.beacon_chain.chain.get_block(&BlockId::Hash(beacon_hash)) {
                Some(b) => b.index(),
                None => return,
            },
        };
        let (owner_uid, mapping) = self.client.get_uid_to_authority_map(block_idx);
        if let Some(o) = owner_uid {
            let beacon_sig = self.client.signer.bls_sign(beacon_hash.as_ref());
            let shard_sig = self.client.signer.bls_sign(shard_hash.as_ref());
            tokio::spawn(
                self.final_signatures_tx
                    .clone()
                    .send(JointBlockBLS::General {
                        beacon_hash,
                        shard_hash,
                        beacon_sig,
                        shard_sig,
                        sender_id: receiver_id,
                        receiver_id: sender_id,
                    })
                    .map(|_| ())
                    .map_err(|e| error!(target: "client", "Error sending final BLS parts: {}", e)),
            );
        }
    }

    /// Spawn a kick-off task.
    fn spawn_kickoff(&self) {
        let hash = self.client.shard_client.pool.snapshot_payload();
        let next_index = self.client.beacon_chain.chain.best_block().index() + 1;
        let control = self.get_control(next_index, hash);

        // Send mempool control.
        let task =
            self.control_tx.clone().send(control).map(|_| ()).map_err(
                |e| error!(target: "consensus", "Error sending consensus control: {:?}", e),
            );
        tokio::spawn(task);
    }

    pub fn spawn(self) {
        tokio::spawn(self.for_each(|_| Ok(())));
    }
}
