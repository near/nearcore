use std::sync::Arc;
use std::time::Duration;

use futures::Async;
use futures::future::Future;
use futures::Poll;
use futures::sink::Sink;
use futures::Stream;
use futures::sync::mpsc::Receiver;
use futures::sync::mpsc::Sender;
use tokio::timer::Interval;

use client::BlockImportingResult;
use client::BlockProductionResult;
use client::Client;
use mempool::payload_gossip::PayloadGossip;
use nightshade::nightshade::ConsensusBlockProposal;
use nightshade::nightshade_task::Control;
use primitives::beacon::SignedBeaconBlock;
use primitives::block_traits::SignedBlock;
use primitives::chain::{PayloadRequest, PayloadResponse, SignedShardBlock};
use primitives::hash::CryptoHash;
use primitives::types::{AuthorityId, BlockIndex};

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
    /// Gossips for payloads.
    inc_payload_gossip_rx: Receiver<PayloadGossip>,

    // Periodic tasks.
    /// Channel into which we gossip payloads.
    out_payload_gossip_tx: Sender<PayloadGossip>,
    /// Interval at which we gossip payloads.
    payload_gossip_interval: Interval,
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
        let mut payload_gossip_ended = false;
        // We exit this loop when for each polled channel it has either ended or it is not ready.
        loop {
            match self.consensus_rx.poll() {
                Ok(Async::Ready(Some(c))) => {
                    if c.index == self.client.beacon_chain.chain.best_index() + 1 {
                        if let ind @ Some(_) = self.try_produce_block(c) {
                            new_block_index = ind;
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
                Err(()) => {
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
                Ok(Async::Ready(Some(_))) => {
                    self.gossip_payload();
                    continue;
                }
                Ok(Async::Ready(None)) => {
                    panic!("Interval stream is not expected to ever end");
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
            && payload_gossip_ended
        {
            Ok(Async::Ready(None))
        } else {
            Ok(Async::NotReady)
        }
    }
}

impl ClientTask {
    pub fn new(
        client: Arc<Client>,
        incoming_block_rx: Receiver<(SignedBeaconBlock, SignedShardBlock)>,
        out_block_tx: Sender<(SignedBeaconBlock, SignedShardBlock)>,
        consensus_rx: Receiver<ConsensusBlockProposal>,
        control_tx: Sender<Control>,
        retrieve_payload_rx: Receiver<(AuthorityId, CryptoHash)>,
        payload_request_tx: Sender<PayloadRequest>,
        payload_response_rx: Receiver<PayloadResponse>,
        inc_payload_gossip_rx: Receiver<PayloadGossip>,
        out_payload_gossip_tx: Sender<PayloadGossip>,
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
            payload_gossip_interval: Interval::new_interval(gossip_interval),
        };
        res.spawn_kickoff();
        res
    }

    /// Tries producing block from the given consensus. If succeeds returns index of the block with
    /// the highest index.
    fn try_produce_block(
        &mut self,
        consensus_block_header: ConsensusBlockProposal,
    ) -> Option<BlockIndex> {
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

        if let BlockProductionResult::Success(produced_beacon_block, produced_shard_block) =
            self.client.try_produce_block(consensus_block_header.index, payload)
        {
            // Send block announcement.
            tokio::spawn(
                self.out_block_tx
                    .clone()
                    .send((produced_beacon_block, produced_shard_block))
                    .map(|_| ())
                    .map_err(
                        |e| error!(target: "client", "Error sending block announcement: {}", e),
                    ),
            );
            let new_best_block = self.client.beacon_chain.chain.best_block();
            Some(new_best_block.index())
        } else {
            None
        }
    }

    /// Tries importing block. If succeeds returns index of the block with the highest index.
    fn try_import_block(
        &mut self,
        beacon_block: SignedBeaconBlock,
        shard_block: SignedShardBlock,
    ) -> Option<BlockIndex> {
        if let BlockImportingResult::Success { new_index } =
            self.client.try_import_blocks(beacon_block, shard_block) {

            info!(
                "Successfully imported block(s) up to {}, account_id={:?}",
                new_index, self.client.account_id
            );
            Some(new_index)
        } else {
            None
        }
    }

    fn get_or_request_payload(&self, authority_id: AuthorityId, hash: CryptoHash) {
        let pool = &self.client.shard_client.pool;
        info!(
            target: "mempool",
            "Checking payload confirmation, authority_id={} for hash={} from other authority_id={}",
            pool.authority_id.read().expect("Lock is poisoned").unwrap(),
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

    /// Resets MemPool and returns NS control for next block.
    fn restart_pool_nightshade(&self, block_index: BlockIndex) -> Control {
        // TODO: Get authorities for the correct block index. For now these are the same authorities
        // that built the first block. In other words use `block_index` instead of `mock_block_index`.
        let mock_block_index = 2;
        let (owner_uid, uid_to_authority_map) =
            self.client.get_uid_to_authority_map(mock_block_index);

        if owner_uid.is_none() {
            self.client.shard_client.pool.reset(None, None);
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

        self.client.shard_client.pool.reset(Some(owner_uid), Some(num_authorities));
        let hash = self.client.shard_client.pool.snapshot_payload();
        Control::Reset { owner_uid, block_index, hash, public_keys, bls_public_keys }
    }

    fn gossip_payload(&self) {
        let pool = &self.client.shard_client.pool;
        for payload_gossip in pool.prepare_payload_gossip() {
            tokio::spawn(
                self.out_payload_gossip_tx
                    .clone()
                    .send(payload_gossip)
                    .map(|_| ())
                    .map_err(|e| warn!(target: "mempool", "Error sending message: {}", e)),
            );
        }
    }

    fn process_payload_gossip(&self, payload_gossip: PayloadGossip) {
        let pool = &self.client.shard_client.pool;
        if let Err(err) = pool.add_payload_with_author(payload_gossip.payload, payload_gossip.sender_id) {
            warn!(target: "client", "Failed to process payload gossip: {}", err);
        }
    }

    /// Spawn a kick-off task.
    fn spawn_kickoff(&self) {
        let next_index = self.client.beacon_chain.chain.best_block().index() + 1;
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
