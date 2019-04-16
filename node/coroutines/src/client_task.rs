use std::cmp::max;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::Future;
use futures::sink::Sink;
use futures::sync::mpsc::Receiver;
use futures::sync::mpsc::Sender;
use futures::Async;
use futures::Poll;
use futures::Stream;
use tokio::timer::Delay;

use client::BlockImportingResult;
use client::Client;
use mempool::payload_gossip::PayloadGossip;
use nightshade::nightshade::ConsensusBlockProposal;
use nightshade::nightshade_task::Control;
use primitives::beacon::SignedBeaconBlock;
use primitives::block_traits::SignedBlock;
use primitives::chain::{ChainState, PayloadRequest, PayloadResponse, SignedShardBlock};
use primitives::crypto::signer::{AccountSigner, BLSSigner, EDSigner};
use primitives::hash::CryptoHash;
use primitives::types::{AuthorityId, BlockIndex, PeerId};
use shard::ShardBlockExtraInfo;

const MUST_HAVE_A_POOL: &str = "Must have a pool";
const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub struct ClientTask<T> {
    client: Arc<Client<T>>,
    /// Incoming blocks produced by other peer that might be imported by this peer.
    incoming_block_rx: Receiver<(PeerId, Vec<(SignedBeaconBlock, SignedShardBlock)>, BlockIndex)>,
    /// Outgoing blocks produced by this peer that can be imported by other peers.
    out_block_tx: Sender<(Vec<PeerId>, (SignedBeaconBlock, SignedShardBlock))>,
    /// Consensus created by the current instance of Nightshade or pass-through consensus.
    consensus_rx: Receiver<ConsensusBlockProposal>,
    /// Control that we send to the consensus task.
    control_tx: Sender<Control>,
    /// Request from the Nightshade task to retrieve the payload.
    retrieve_payload_rx: Receiver<(AuthorityId, CryptoHash)>,
    /// Request from mempool task to the peer to return the payload.
    payload_request_tx: Sender<(BlockIndex, PayloadRequest)>,
    /// Responses from the peers with the payload.
    payload_response_rx: Receiver<PayloadResponse>,
    /// Gossips for payloads.
    inc_payload_gossip_rx: Receiver<PayloadGossip>,
    /// Incoming information about chain from other peers.
    inc_chain_state_rx: Receiver<(PeerId, ChainState)>,
    /// Sending requests for blocks to peers.
    out_block_fetch_tx: Sender<(PeerId, BlockIndex, BlockIndex)>,

    /// Channel into which we gossip payloads.
    out_payload_gossip_tx: Sender<PayloadGossip>,
    /// Delay until the next time we gossip payloads.
    payload_gossip_interval: Delay,

    gossip_interval: Duration,

    /// Last block index per peer. Might not be the real block index the peer is it, in the cases:
    /// The value is underestimated if when the remote node managed to progress several blocks
    /// ahead without us knowing it. The value is overestimated when we send an announcement or reply
    /// to peer state which gets lost over the network.
    assumed_peer_last_index: HashMap<PeerId, BlockIndex>,
}

impl<T: AccountSigner + BLSSigner + EDSigner + 'static> Stream for ClientTask<T> {
    type Item = ();
    type Error = ();

    #[allow(clippy::cognitive_complexity)]
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut new_block_index = None;
        let mut block_production_ended = false;
        let mut block_importing_ended = false;
        let mut retrieve_payload_ended = false;
        let mut payload_response_ended = false;
        let mut payload_gossip_ended = false;
        let mut peer_sync_ended = false;
        // We exit this loop when for each polled channel it has either ended or it is not ready.
        loop {
            match self.consensus_rx.poll() {
                Ok(Async::Ready(Some(c))) => {
                    if c.index == self.client.shard_client.chain.best_index() + 1 {
                        let (beacon_block, shard_block, shard_extra) = self.prepare_block(c);
                        let beacon_hash = beacon_block.hash;
                        let shard_hash = shard_block.hash;
                        debug!(target: "client", "[{:?}] Block #{} produced: {}, {}",
                              self.client.account_id(), beacon_block.body.header.index, beacon_hash, shard_hash);
                        let (beacon_block, shard_block) = self.client.try_import_produced(beacon_block, shard_block, shard_extra);
                        self.announce_block(beacon_block, shard_block);
                        new_block_index = Some(self.client.shard_client.chain.best_index());
                    } else {
                        info!(target: "client", "[{:?}] Ignoring consensus for {} because current block index is {}",
                              self.client.account_id(), c.index, self.client.shard_client.chain.best_index());
                    }
                    continue;
                }
                Ok(Async::Ready(None)) => {
                    block_production_ended = true;
                }
                Ok(Async::NotReady) => (),
                Err(()) => {
                    warn!(target: "client", "Error reading from consensus stream");
                    continue;
                }
            }

            match self.inc_chain_state_rx.poll() {
                Ok(Async::Ready(Some((peer_id, chain_state)))) => {
                    self.process_peer_state(peer_id, chain_state);
                }
                Ok(Async::Ready(None)) => {
                    peer_sync_ended = true;
                }
                Ok(Async::NotReady) => (),
                Err(()) => {
                    warn!(target: "client", "Error reading from peer sync");
                    continue;
                }
            }

            match self.incoming_block_rx.poll() {
                Ok(Async::Ready(Some((peer_id, blocks, best_index)))) => {
                    if let Some(index) = self.try_import_blocks(peer_id, blocks) {
                        if index < best_index {
                            tokio_utils::spawn(
                                self.out_block_fetch_tx
                                    .clone()
                                    .send((peer_id, self.client.beacon_client.chain.best_index() + 1, best_index))
                                    .map(|_| ())
                                    .map_err(|e| error!(target: "client", "Error sending request to fetch blocks from peer: {}", e))
                            );
                            continue;
                        }
                        new_block_index = Some(index);
                    }
                    continue;
                }
                Ok(Async::Ready(None)) => {
                    block_importing_ended = true;
                }
                Ok(Async::NotReady) => (),
                Err(()) => {
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
                Err(()) => {
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
                Err(()) => {
                    warn!(target: "client", "Error reading payload response channel");
                    continue;
                }
            }

            match self.inc_payload_gossip_rx.poll() {
                Ok(Async::Ready(Some(p))) => {
                    self.process_payload_gossip(p);
                    continue;
                }
                Ok(Async::Ready(None)) => {
                    payload_gossip_ended = true;
                }
                Ok(Async::NotReady) => (),
                Err(()) => {
                    warn!(target: "client", "Error reading payload gossip channel");
                    continue;
                }
            }

            match self.payload_gossip_interval.poll() {
                Ok(Async::Ready(())) => {
                    self.gossip_payload();
                    self.payload_gossip_interval.reset(Instant::now() + self.gossip_interval);
                    continue;
                }
                Ok(Async::NotReady) => (),
                Err(err) => {
                    warn!(target: "client", "Interval stream error: {}", err);
                    continue;
                }
            }

            // If we reached here than each channel is either not ready or has ended.
            break;
        }

        // First, send the control, if applicable.
        if let Some(index) = new_block_index {
            let next_index = index + 1;
            let control = self.restart_pool_nightshade(next_index);
            tokio_utils::spawn(
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
            && payload_gossip_ended
            && peer_sync_ended
        {
            Ok(Async::Ready(None))
        } else {
            Ok(Async::NotReady)
        }
    }
}

impl<T: AccountSigner + EDSigner + BLSSigner + 'static> ClientTask<T> {
    pub fn new(
        client: Arc<Client<T>>,
        incoming_block_rx: Receiver<(
            PeerId,
            Vec<(SignedBeaconBlock, SignedShardBlock)>,
            BlockIndex,
        )>,
        out_block_tx: Sender<(Vec<PeerId>, (SignedBeaconBlock, SignedShardBlock))>,
        consensus_rx: Receiver<ConsensusBlockProposal>,
        control_tx: Sender<Control>,
        retrieve_payload_rx: Receiver<(AuthorityId, CryptoHash)>,
        payload_request_tx: Sender<(BlockIndex, PayloadRequest)>,
        payload_response_rx: Receiver<PayloadResponse>,
        inc_payload_gossip_rx: Receiver<PayloadGossip>,
        out_payload_gossip_tx: Sender<PayloadGossip>,
        inc_chain_state_rx: Receiver<(PeerId, ChainState)>,
        out_block_fetch_tx: Sender<(PeerId, BlockIndex, BlockIndex)>,
        gossip_interval: Duration,
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
            inc_payload_gossip_rx,
            out_payload_gossip_tx,
            inc_chain_state_rx,
            out_block_fetch_tx,
            payload_gossip_interval: Delay::new(Instant::now() + gossip_interval),
            gossip_interval,
            assumed_peer_last_index: Default::default(),
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
        let proposal = &consensus_block_header.proposal_with_proof.bare_state.endorses;
        info!(target: "client", "[{:?}] Producing block for proposal {:?}, index {}", self.client.account_id(), proposal.hash, consensus_block_header.index);
        let payload = match self
            .client
            .shard_client
            .pool
            .clone()
            .expect(MUST_HAVE_A_POOL)
            .write()
            .expect(POISONED_LOCK_ERR)
            .pop_payload_snapshot(&proposal.hash)
        {
            Some(p) => p,
            None => {
                // Assumption: This else should never be reached, if an authority achieves consensus on some block hash
                // then it is because this block can be retrieved from the mempool.
                panic!(
                    "[{:?}] Failed to find payload for {} from authority {}",
                    self.client.account_id(),
                    proposal.hash,
                    proposal.author
                );
            }
        };
        let (mut beacon_block, mut shard_block, shard_extra) = self.client.prepare_block(payload, consensus_block_header.proposal_with_proof);
        let (owner_uid, mapping) = self.client.get_uid_to_authority_map(beacon_block.index());
        if let Some(owner) = owner_uid {
            let signer = self.client.signer.clone().expect("Must have signer to prepare blocks");
            // TODO fail somewhere much earlier if our keys don't match chain spec
            assert_eq!(mapping.get(&owner).as_ref().unwrap().public_key, signer.public_key());
            assert_eq!(
                mapping.get(&owner).as_ref().unwrap().bls_public_key,
                signer.bls_public_key()
            );
        } else {
            panic!(format!(
                "Preparing block while not being authority for this block: {:?}",
                beacon_block
            ));
        }
        (beacon_block, shard_block, shard_extra)
    }

    /// Tries importing block. If succeeds returns index of the block with the highest index.
    /// If some blocks are missing, requests network to fetch them.
    fn try_import_blocks(
        &mut self,
        peer_id: PeerId,
        blocks: Vec<(SignedBeaconBlock, SignedShardBlock)>,
    ) -> Option<BlockIndex> {
        if blocks.is_empty() {
            return None;
        }

        let latest_index = blocks.iter().map(|(b, _)| b.index()).max().unwrap();
        self.assumed_peer_last_index.insert(peer_id, latest_index);
        // TODO: clonning here sucks, is there a better way?
        let (last_beacon_block, last_shard_block) = blocks.last().unwrap().clone();
        match self.client.try_import_blocks(blocks) {
            BlockImportingResult::Success { new_index } => {
                info!(target: "client",
                    "[{:?}] Successfully imported block(s) up to {}",
                    self.client.account_id(), new_index,
                );
                self.announce_block(last_beacon_block, last_shard_block);
                Some(new_index)
            }
            BlockImportingResult::MissingParent { missing_indices, .. } => {
                warn!(target: "client", "[{:?}] Missing indicies {:?}", self.client.account_id(), missing_indices);
                let (from_index, til_index) =
                    (missing_indices.iter().min().unwrap(), missing_indices.iter().max().unwrap());
                tokio_utils::spawn(
                    self.out_block_fetch_tx.clone().send((peer_id, *from_index, *til_index)).map(|_| ()).map_err(|e| error!(target: "client", "Error sending request to fetch blocks from peer: {}", e))
                );
                None
            }
            // TODO: if block is invalid, we can extend `out_block_fetch_tx` channel to also handle banning actions.
            _ => None,
        }
    }

    fn process_peer_state(&mut self, peer_id: PeerId, chain_state: ChainState) {
        self.assumed_peer_last_index.insert(peer_id, chain_state.last_index);
        if chain_state.last_index > self.client.beacon_client.chain.best_index() {
            // TODO: we should keep track of already fetching stuff.
            info!(target: "client", "[{:?}] Missing blocks {}..{} (from {})",
                  self.client.account_id(),
                  self.client.beacon_client.chain.best_index(),
                  chain_state.last_index, peer_id);
            tokio_utils::spawn(
                self.out_block_fetch_tx
                    .clone()
                    .send((peer_id, self.client.beacon_client.chain.best_index() + 1, chain_state.last_index))
                    .map(|_| ())
                    .map_err(|e| error!(target: "client", "Error sending request to fetch blocks from peer: {}", e))
            );
        }
    }

    fn announce_block(&mut self, beacon_block: SignedBeaconBlock, shard_block: SignedShardBlock) {
        let peer_ids = self
            .assumed_peer_last_index
            .iter()
            .filter_map(
                |(peer_id, last_index)| {
                    if *last_index < beacon_block.index() {
                        Some(*peer_id)
                    } else {
                        None
                    }
                },
            )
            .collect();
        for last_index in self.assumed_peer_last_index.values_mut() {
            *last_index = max(*last_index, beacon_block.index());
        }
        info!(
            "[{:?}] Announcing block {} to {:?}, peer_last_index: {:?}",
            self.client.account_id(),
            beacon_block.index(),
            peer_ids,
            self.assumed_peer_last_index
        );
        tokio_utils::spawn(
            self.out_block_tx
                .clone()
                .send((peer_ids, (beacon_block, shard_block)))
                .map(|_| ())
                .map_err(|e| error!(target: "client", "Error sending block announcement: {}", e)),
        );
    }

    fn get_or_request_payload(&self, authority_id: AuthorityId, hash: CryptoHash) {
        let block_index = self.client.shard_client.chain.best_index() + 1;
        let pool = &self.client.shard_client.pool.clone().expect("Must have pool");
        debug!(
            target: "client",
            "[{:?}] Checking payload confirmation, authority_id={} for hash={} from other authority_id={}",
            self.client.account_id(),
            pool.read().expect(POISONED_LOCK_ERR).authority_id.unwrap(),
            hash,
            authority_id,
        );
        if !pool.read().expect(POISONED_LOCK_ERR).contains_payload_snapshot(&hash) {
            tokio_utils::spawn(
                self.payload_request_tx
                    .clone()
                    .send((block_index, PayloadRequest::BlockProposal(authority_id, hash)))
                    .map(|_| ())
                    .map_err(|e| warn!(target: "mempool", "Error sending message: {}", e)),
            );
        } else {
            let send_confirmation = self
                .control_tx
                .clone()
                .send(Control::PayloadConfirmation(authority_id, hash))
                .map(|_| ())
                .map_err(
                    |_| error!(target: "mempool", "Fail sending control signal to nightshade"),
                );
            tokio_utils::spawn(send_confirmation);
        }
    }

    /// Process incoming payload response.
    fn process_payload_response(&self, payload_response: PayloadResponse) {
        let pool = &self.client.shard_client.pool.clone().expect("Must have pool");
        match payload_response {
            PayloadResponse::General(authority_id, response) => {
                match pool
                    .write()
                    .expect(POISONED_LOCK_ERR)
                    .add_missing_payload(authority_id, response)
                {
                    Ok(snapshot_hash) => {
                        let send_confirmation = self
                            .control_tx
                            .clone()
                            .send(Control::PayloadConfirmation(authority_id, snapshot_hash))
                            .map(|_| ())
                            .map_err(
                                |_| error!(target: "mempool", "Fail sending control signal to nightshade"),
                            );
                        tokio_utils::spawn(send_confirmation);
                    }
                    Err(e) => warn!(target: "mempool", "Fail to add missing payload: {}", e),
                }
            }
            PayloadResponse::BlockProposal(authority_id, snapshot) => {
                let hash = snapshot.get_hash();
                if let Some(request) = pool
                    .write()
                    .expect(POISONED_LOCK_ERR)
                    .add_payload_snapshot(authority_id, snapshot)
                {
                    let block_index = self.client.shard_client.chain.best_index() + 1;
                    tokio_utils::spawn(
                        self.payload_request_tx
                            .clone()
                            .send((block_index, PayloadRequest::General(authority_id, request)))
                            .map(|_| ())
                            .map_err(|e| warn!(target: "mempool", "Error sending message: {}", e)),
                    );
                } else {
                    let send_confirmation = self
                        .control_tx
                        .clone()
                        .send(Control::PayloadConfirmation(authority_id, hash))
                        .map(|_| ())
                        .map_err(
                            |_| error!(target: "mempool", "Fail sending control signal to nightshade"),
                        );
                    tokio_utils::spawn(send_confirmation);
                }
            }
        }
    }

    /// Resets MemPool and returns NS control for next block.
    fn restart_pool_nightshade(&self, block_index: BlockIndex) -> Control {
        let index = self.client.shard_client.chain.best_index() + 1;
        let (owner_uid, uid_to_authority_map) = self.client.get_uid_to_authority_map(index);

        if owner_uid.is_none() {
            if let Some(pool) = &self.client.shard_client.pool {
                pool.write().expect(POISONED_LOCK_ERR).reset(None, None);
            }
            return Control::Stop;
        }
        let owner_uid = owner_uid.unwrap();
        let num_authorities = uid_to_authority_map.len();

        let mut public_keys = vec![];
        let mut bls_public_keys = vec![];
        for i in 0..num_authorities {
            debug!(
                "Authority #{}: account_id={:?} me={} block_index={}",
                i,
                uid_to_authority_map[&i].account_id,
                i == owner_uid,
                block_index
            );
            public_keys.push(uid_to_authority_map[&i].public_key);
            bls_public_keys.push(uid_to_authority_map[&i].bls_public_key.clone());
        }

        let pool = self.client.shard_client.pool.clone().expect(MUST_HAVE_A_POOL);
        let mut pool_guard = pool.write().expect(POISONED_LOCK_ERR);
        pool_guard.reset(Some(owner_uid), Some(num_authorities));
        let hash = pool_guard.snapshot_payload();
        Control::Reset { owner_uid, block_index, hash, public_keys, bls_public_keys }
    }

    fn gossip_payload(&self) {
        let block_index = self.client.shard_client.chain.best_index() + 1;
        let (owner_uid, _) = self.client.get_uid_to_authority_map(block_index);
        if owner_uid.is_some() {
            let pool = &self.client.shard_client.pool.clone().expect("Must have pool");
            for payload_gossip in pool.write().expect(POISONED_LOCK_ERR).prepare_payload_gossip(block_index) {
                tokio_utils::spawn(
                    self.out_payload_gossip_tx
                        .clone()
                        .send(payload_gossip)
                        .map(|_| ())
                        .map_err(|e| warn!(target: "mempool", "Error sending message: {}", e)),
                );
            }
        }
    }

    fn process_payload_gossip(&self, payload_gossip: PayloadGossip) {
        let pool = self.client.shard_client.pool.clone().expect(MUST_HAVE_A_POOL);
        let res = pool
            .write()
            .expect(POISONED_LOCK_ERR)
            .add_payload_with_author(payload_gossip.payload, payload_gossip.sender_id);
        if let Err(err) = res {
            warn!(target: "client", "Failed to process payload gossip: {}", err);
        }
    }

    /// Spawn a kick-off task.
    fn spawn_kickoff(&self) {
        let next_index = self.client.shard_client.chain.best_index() + 1;
        let control = self.restart_pool_nightshade(next_index);

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
