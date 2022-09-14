use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use near_chain::{types::Tip, validate::validate_chunk_proofs, Chain, ChainStore, RuntimeAdapter};
use near_chunks_primitives::Error;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_network_primitives::types::{AccountIdOrPeerTrackingShard, PartialEncodedChunkRequestMsg};
use near_primitives::{
    hash::CryptoHash,
    merkle::merklize,
    receipt::Receipt,
    sharding::{
        EncodedShardChunk, PartialEncodedChunk, PartialEncodedChunkPart, PartialEncodedChunkV1,
        PartialEncodedChunkV2, ReceiptProof, ReedSolomonWrapper, ShardChunk, ShardChunkHeader,
        ShardProof,
    },
    time::Clock,
    types::{AccountId, BlockHeight, BlockHeightDelta, EpochId, ShardId},
};
use rand::{seq::IteratorRandom, Rng};
use tracing::{debug, error, warn};

const CHUNK_REQUEST_SWITCH_TO_OTHERS_MS: u64 = 400;
const CHUNK_REQUEST_SWITCH_TO_FULL_FETCH_MS: u64 = 3_000;
const CHUNK_REQUEST_RETRY_MAX_MS: u64 = 1_000_000;
// Only request chunks from peers whose latest height >= chunk_height - CHUNK_REQUEST_PEER_HORIZON
const CHUNK_REQUEST_PEER_HORIZON: BlockHeightDelta = 5;

pub enum ChunkPartReceivingMethod {
    ViaPartialEncodedChunk,
    ViaPartialEncodedChunkResponse,
    ViaPartialEncodedChunkForward,
}

pub enum ChunkHeaderValidationState {
    NotValidated,
    PartiallyValidated,
    FullyValidated,
}

pub struct ChunkPartState {
    pub part: PartialEncodedChunkPart,
    pub time_received: Instant,
    pub receiving_method: ChunkPartReceivingMethod,
    pub forwarded: bool,
}

pub struct ChunkNeededInfo {
    pub parts_needed: HashSet<u64>,
    pub receipt_proofs_needed: HashSet<ShardId>,
    pub cares_about_shard: bool,
}

pub struct ChunkState {
    pub insertion_time: Instant,

    pub header: Option<ShardChunkHeader>,
    pub parts: HashMap<u64, ChunkPartState>,
    pub receipts: HashMap<ShardId, ReceiptProof>,
    pub ancestor_block_hash: Option<CryptoHash>,

    pub header_validation_state: ChunkHeaderValidationState,
    pub confirmed_epoch_id: Option<EpochId>,
    pub guessed_epoch_id: Option<EpochId>,
    pub needed: Option<ChunkNeededInfo>,
    pub completed_partial_chunk: Option<PartialEncodedChunk>,
    pub completed_chunk: Option<(ShardChunk, EncodedShardChunk)>,
    pub errors: RefCell<Vec<Error>>,

    pub next_request_due: Option<Instant>,
}

impl ChunkState {
    pub fn new() -> Self {
        Self {
            insertion_time: Instant::now(),

            header: None,
            parts: HashMap::new(),
            receipts: HashMap::new(),
            ancestor_block_hash: None,

            header_validation_state: ChunkHeaderValidationState::NotValidated,
            confirmed_epoch_id: None,
            guessed_epoch_id: None,
            needed: None,
            completed_partial_chunk: None,
            completed_chunk: None,
            errors: RefCell::new(vec![]),

            next_request_due: None,
        }
    }

    fn ok_or_none<T, E>(&self, result: Result<T, E>) -> Option<T>
    where
        E: Into<Error>,
    {
        match result {
            Ok(value) => Some(value),
            Err(err) => {
                self.errors.borrow_mut().push(err.into());
                None
            }
        }
    }

    pub fn add_header(&mut self, header: &ShardChunkHeader) {
        if self.header.is_none() {
            self.header = Some(header.clone());
        }
    }

    pub fn add_part(
        &mut self,
        part: PartialEncodedChunkPart,
        receiving_method: ChunkPartReceivingMethod,
    ) {
        self.parts.entry(part.part_ord).or_insert_with(|| ChunkPartState {
            part,
            time_received: Instant::now(),
            receiving_method,
            forwarded: false,
        });
    }

    pub fn add_receipt_proof(&mut self, receipt_proof: ReceiptProof) {
        self.receipts.insert(receipt_proof.1.to_shard_id, receipt_proof);
    }

    pub fn add_ancestor_block_hash(&mut self, ancestor_block_hash: CryptoHash) {
        self.ancestor_block_hash = Some(ancestor_block_hash);
    }

    /// Returns true if we need this part to sign the block.
    fn need_part(
        epoch_id: &EpochId,
        part_ord: u64,
        me: &Option<AccountId>,
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> bool {
        me == &Some(runtime_adapter.get_part_owner(&epoch_id, part_ord).unwrap())
    }

    pub fn cares_about_shard_this_or_next_epoch(
        account_id: &Option<AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> bool {
        runtime_adapter.cares_about_shard(account_id.as_ref(), parent_hash, shard_id, is_me)
            || runtime_adapter.will_care_about_shard(
                account_id.as_ref(),
                parent_hash,
                shard_id,
                is_me,
            )
    }

    fn need_receipt(
        prev_block_hash: &CryptoHash,
        shard_id: ShardId,
        me: &Option<AccountId>,
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> bool {
        Self::cares_about_shard_this_or_next_epoch(
            me,
            prev_block_hash,
            shard_id,
            true,
            runtime_adapter,
        )
    }

    fn get_or_compute_needed(
        &mut self,
        me: &Option<AccountId>,
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> Option<&ChunkNeededInfo> {
        if self.needed.is_none() {
            let epoch_id = self.get_or_compute_confirmed_epoch_id(runtime_adapter)?.clone();
            let prev_block_hash = self.header.as_ref().unwrap().prev_block_hash().clone();
            let mut parts_needed = HashSet::new();
            for part_ord in 0..runtime_adapter.num_total_parts() as u64 {
                if Self::need_part(&epoch_id, part_ord, me, runtime_adapter) {
                    parts_needed.insert(part_ord);
                }
            }
            let mut receipt_proofs_needed = HashSet::new();
            let num_shards = self.ok_or_none(runtime_adapter.num_shards(&epoch_id)).unwrap_or(0);
            for shard_id in 0..num_shards {
                if Self::need_receipt(&prev_block_hash, shard_id, me, runtime_adapter) {
                    receipt_proofs_needed.insert(shard_id);
                }
            }
            let reconstruction_needed = Self::cares_about_shard_this_or_next_epoch(
                me,
                &prev_block_hash,
                self.header.as_ref().unwrap().shard_id(),
                true,
                runtime_adapter,
            );
            self.needed = Some(ChunkNeededInfo {
                parts_needed,
                receipt_proofs_needed,
                cares_about_shard: reconstruction_needed,
            });
        }
        self.needed.as_ref()
    }

    fn get_or_compute_confirmed_epoch_id(
        &mut self,
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> Option<&EpochId> {
        if self.confirmed_epoch_id.is_none() {
            let header = self.header.as_ref()?;
            let epoch_id =
                runtime_adapter.get_epoch_id_from_prev_block(header.prev_block_hash()).ok()?;
            self.confirmed_epoch_id = Some(epoch_id);
        }
        self.confirmed_epoch_id.as_ref()
    }

    fn get_or_compute_guessed_epoch_id(
        &mut self,
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> Option<&EpochId> {
        if self.guessed_epoch_id.is_none() {
            let ancestor_hash = self.ancestor_block_hash.as_ref()?;
            let epoch_id = runtime_adapter.get_epoch_id_from_prev_block(ancestor_hash).ok()?;
            self.guessed_epoch_id = Some(epoch_id);
        }
        self.guessed_epoch_id.as_ref()
    }

    fn get_or_compute_completed_chunk(
        &mut self,
        me: &Option<AccountId>,
        runtime_adapter: &dyn RuntimeAdapter,
        rs: &mut ReedSolomonWrapper,
        chain_store: &mut ChainStore,
    ) -> Option<&(ShardChunk, EncodedShardChunk)> {
        if self.completed_chunk.is_none() {
            self.get_or_compute_needed(me, runtime_adapter)?;
            self.get_or_compute_confirmed_epoch_id(runtime_adapter)?;
            let needed = self.needed.as_ref()?;
            let header = self.header.as_ref()?;
            let epoch_id = self.confirmed_epoch_id.as_ref()?;
            let can_reconstruct = self.parts.len() >= runtime_adapter.num_data_parts();
            if can_reconstruct {
                let protocol_version =
                    self.ok_or_none(runtime_adapter.get_epoch_protocol_version(&epoch_id))?;
                let mut encoded_chunk = EncodedShardChunk::from_header(
                    header.clone(),
                    runtime_adapter.num_total_parts(),
                    protocol_version,
                );

                for (part_ord, part_entry) in self.parts.iter() {
                    encoded_chunk.content_mut().parts[*part_ord as usize] =
                        Some(part_entry.part.part.clone());
                }

                match self.ok_or_none(Self::decode_encoded_chunk(
                    &mut encoded_chunk,
                    rs,
                    runtime_adapter,
                )) {
                    Some(shard_chunk) => {
                        if needed.cares_about_shard {
                            self.ok_or_none(Self::persist_chunk(shard_chunk.clone(), chain_store))?;
                        }
                        self.completed_chunk = Some((shard_chunk, encoded_chunk));
                    }
                    None => {
                        self.ok_or_none(Self::persist_invalid_chunk(encoded_chunk, chain_store));
                    }
                }
            }
        }
        self.completed_chunk.as_ref()
    }

    fn make_outgoing_receipts_proofs(
        chunk_header: &ShardChunkHeader,
        outgoing_receipts: &[Receipt],
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> Result<impl Iterator<Item = ReceiptProof>, near_chunks_primitives::Error> {
        let shard_id = chunk_header.shard_id();
        let shard_layout =
            runtime_adapter.get_shard_layout_from_prev_block(chunk_header.prev_block_hash())?;

        let hashes = Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout);
        let (root, proofs) = merklize(&hashes);
        assert_eq!(chunk_header.outgoing_receipts_root(), root);

        let mut receipts_by_shard =
            Chain::group_receipts_by_shard(outgoing_receipts.to_vec(), &shard_layout);
        let it = proofs.into_iter().enumerate().map(move |(proof_shard_id, proof)| {
            let proof_shard_id = proof_shard_id as u64;
            let receipts = receipts_by_shard.remove(&proof_shard_id).unwrap_or_else(Vec::new);
            let shard_proof =
                ShardProof { from_shard_id: shard_id, to_shard_id: proof_shard_id, proof };
            ReceiptProof(receipts, shard_proof)
        });
        Ok(it)
    }

    fn get_or_compute_completed_partial_chunk(
        &mut self,
        me: &Option<AccountId>,
        runtime_adapter: &dyn RuntimeAdapter,
        rs: &mut ReedSolomonWrapper,
        chain_store: &mut ChainStore,
    ) -> Option<&PartialEncodedChunk> {
        if self.completed_partial_chunk.is_none() {
            self.get_or_compute_needed(me, runtime_adapter)?;
            self.get_or_compute_completed_chunk(me, runtime_adapter, rs, chain_store)?;
            let needed = self.needed.as_ref()?;
            let (header, parts, receipts) =
                if let Some((shard_chunk, encoded_chunk)) = self.completed_chunk.as_ref() {
                    let header = encoded_chunk.cloned_header();
                    let (merkle_root, merkle_paths) =
                        encoded_chunk.content().get_merkle_hash_and_paths();
                    let receipts = self
                        .ok_or_none(Self::make_outgoing_receipts_proofs(
                            &header,
                            shard_chunk.receipts(),
                            runtime_adapter,
                        ))?
                        .collect::<Vec<_>>();
                    (
                        header,
                        encoded_chunk
                            .content()
                            .parts
                            .clone()
                            .into_iter()
                            .zip(merkle_paths)
                            .enumerate()
                            .map(|(part_ord, (part, merkle_proof))| {
                                let part_ord = part_ord as u64;
                                let part = part.unwrap();
                                PartialEncodedChunkPart { part_ord, part, merkle_proof }
                            })
                            .collect::<Vec<_>>(),
                        receipts,
                    )
                } else {
                    if needed.cares_about_shard {
                        // If we need reconstruction, we need the get_or_compute_completed_chunk code path
                        // to compute the partial encoded chunk.
                        return None;
                    }
                    let header = self.header.as_ref()?;
                    for part_ord in needed.parts_needed.iter() {
                        if !self.parts.contains_key(part_ord) {
                            return None;
                        }
                    }
                    for shard_id in needed.receipt_proofs_needed.iter() {
                        if !self.receipts.contains_key(shard_id) {
                            return None;
                        }
                    }
                    (
                        header.clone(),
                        self.parts.values().map(|v| v.part.clone()).collect(),
                        self.receipts.values().cloned().collect(),
                    )
                };
            let parts = parts
                .into_iter()
                .filter(|part| {
                    needed.cares_about_shard || needed.parts_needed.contains(&part.part_ord)
                })
                .collect::<Vec<_>>();
            let receipts = receipts
                .into_iter()
                .filter(|receipt_proof| {
                    needed.cares_about_shard
                        || needed.receipt_proofs_needed.contains(&receipt_proof.1.to_shard_id)
                })
                .collect::<Vec<_>>();
            let partial_chunk = match header.clone() {
                ShardChunkHeader::V1(header) => {
                    PartialEncodedChunk::V1(PartialEncodedChunkV1 { header, parts, receipts })
                }
                header => {
                    PartialEncodedChunk::V2(PartialEncodedChunkV2 { header, parts, receipts })
                }
            };
            self.ok_or_none(Self::persist_partial_chunk(partial_chunk.clone(), chain_store))?;
            self.completed_partial_chunk = Some(partial_chunk);
        }
        self.completed_partial_chunk.as_ref()
    }

    fn decode_encoded_chunk(
        encoded_chunk: &mut EncodedShardChunk,
        rs: &mut ReedSolomonWrapper,
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> Result<ShardChunk, Error> {
        encoded_chunk.content_mut().reconstruct(rs).map_err(|_| Error::InvalidChunk)?;
        let (merkle_root, _) = encoded_chunk.content().get_merkle_hash_and_paths();
        if merkle_root != encoded_chunk.encoded_merkle_root() {
            return Err(Error::InvalidChunk);
        }
        let shard_chunk = encoded_chunk.decode_chunk(runtime_adapter.num_data_parts())?;
        if !validate_chunk_proofs(&shard_chunk, &*runtime_adapter)? {
            return Err(Error::InvalidChunk);
        }
        Ok(shard_chunk)
    }

    fn persist_chunk(chunk: ShardChunk, chain_store: &mut ChainStore) -> Result<(), Error> {
        let mut store_update = chain_store.store_update();
        store_update.save_chunk(chunk.clone());
        store_update.commit()?;
        Ok(())
    }

    fn persist_partial_chunk(
        partial_chunk: PartialEncodedChunk,
        chain_store: &mut ChainStore,
    ) -> Result<(), Error> {
        let mut store_update = chain_store.store_update();
        store_update.save_partial_chunk(partial_chunk.clone());
        store_update.commit()?;
        Ok(())
    }

    fn persist_invalid_chunk(
        chunk: EncodedShardChunk,
        chain_store: &mut ChainStore,
    ) -> Result<(), Error> {
        let mut store_update = chain_store.store_update();
        store_update.save_invalid_chunk(chunk);
        store_update.commit()?;
        Ok(())
    }

    fn maybe_send_request(
        &mut self,
        me: &Option<AccountId>,
        current_head: &Tip,
        runtime_adapter: &dyn RuntimeAdapter,
        peer_manager_adapter: &dyn PeerManagerAdapter,
        chunk_request_retry_period: Duration,
    ) -> Option<()> {
        if self.completed_partial_chunk.is_some() {
            return None;
        }
        self.get_or_compute_guessed_epoch_id(runtime_adapter)?;
        let needed = self.needed.as_ref()?;
        let header = self.header.as_ref()?;
        let ancestor_hash = self.ancestor_block_hash.as_ref()?;
        let guessed_epoch_id = self.guessed_epoch_id.as_ref()?;
        let check_forwarding_delay = if let Some(next_request_due) = &self.next_request_due {
            if &Instant::now() < next_request_due {
                return None;
            }
            false
        } else {
            true
        };

        let prev_block_hash = header.prev_block_hash().clone();
        let fetch_from_archival = runtime_adapter
            .chunk_needs_to_be_fetched_from_archival(&ancestor_hash, &current_head.last_block_hash).unwrap_or_else(|err| {
            error!(target: "chunks", "Error during requesting partial encoded chunk. Cannot determine whether to request from an archival node, defaulting to not: {}", err);
            false
        });
        let old_block = current_head.last_block_hash != prev_block_hash
            && current_head.prev_block_hash != prev_block_hash;

        if check_forwarding_delay {
            // First time deciding whether to request. If chunks forwarding is enabled,
            // we purposely do not send chunk request messages right away for new blocks. Such requests
            // will eventually be sent because of the `resend_chunk_requests` loop. However,
            // we want to give some time for any `PartialEncodedChunkForward` messages to arrive
            // before we send requests.
            let should_wait_for_chunk_forwarding =
                Self::should_wait_for_chunk_forwarding(guessed_epoch_id, ancestor_hash, header.shard_id(), header.height_created()+1, me, runtime_adapter).unwrap_or_else(|_| {
                    // ancestor_hash must be accepted because we don't request missing chunks through this
                    // this function for orphans
                    debug_assert!(false, "{:?} must be accepted", ancestor_hash);
                    error!(target:"chunks", "requesting chunk whose ancestor_hash {:?} is not accepted", ancestor_hash);
                    false
                });
            if should_wait_for_chunk_forwarding && !fetch_from_archival && !old_block {
                self.next_request_due = Some(Instant::now() + chunk_request_retry_period);
                return None;
            }
        }

        let request_full = needed.cares_about_shard
            || self.insertion_time.elapsed()
                > Duration::from_millis(CHUNK_REQUEST_SWITCH_TO_FULL_FETCH_MS);
        let request_own_parts_from_others = self.insertion_time.elapsed()
            > Duration::from_millis(CHUNK_REQUEST_SWITCH_TO_OTHERS_MS);

        let chunk_producer_account_id = self.ok_or_none(runtime_adapter.get_chunk_producer(
            guessed_epoch_id,
            header.height_created(),
            header.shard_id(),
        ))?;

        // In the following we compute which target accounts we should request parts and receipts from
        // First we choose a shard representative target which is either the original chunk producer
        // or a random block producer tracks the shard.
        // If request_from_archival is true (indicating we are requesting a chunk not from the current
        // or the last epoch), request all parts and receipts from the shard representative target
        // For each part, if we are the part owner, we request the part from the shard representative
        // target, otherwise, the part owner
        // For receipts, request them from the shard representative target
        //
        // Also note that the target accounts decided is not necessarily the final destination
        // where requests are sent. We use them to construct AccountIdOrPeerTrackingShard struct,
        // which will be passed to PeerManagerActor. PeerManagerActor will try to request either
        // from the target account or any eligible peer of the node (See comments in
        // AccountIdOrPeerTrackingShard for when target account is used or peer is used)

        // A account that is either the original chunk producer or a random block producer tracking
        // the shard
        let shard_representative_target = if !request_own_parts_from_others
            && !fetch_from_archival
            && &Some(chunk_producer_account_id.clone()) != me
        {
            Some(chunk_producer_account_id)
        } else {
            self.ok_or_none(Self::get_random_target_tracking_shard(
                guessed_epoch_id,
                ancestor_hash,
                header.shard_id(),
                me,
                runtime_adapter,
            ))?
        };

        let mut bp_to_parts = HashMap::<_, Vec<u64>>::new();
        for part_ord in 0..runtime_adapter.num_total_parts() as u64 {
            if self.parts.contains_key(&part_ord) {
                continue;
            }
            if !request_full && !needed.parts_needed.contains(&part_ord) {
                continue;
            }

            let fetch_from = if fetch_from_archival {
                shard_representative_target.clone()
            } else {
                let part_owner =
                    self.ok_or_none(runtime_adapter.get_part_owner(guessed_epoch_id, part_ord))?;

                if Some(&part_owner) == me.as_ref() {
                    // If missing own part, request it from the chunk producer / node tracking shard
                    shard_representative_target.clone()
                } else {
                    Some(part_owner)
                }
            };

            bp_to_parts.entry(fetch_from).or_default().push(part_ord);
        }

        let shards_to_fetch_receipts =
        // TODO: only keep shards for which we don't have receipts yet
            if request_full { HashSet::new() } else { Self::get_tracking_shards(guessed_epoch_id, ancestor_hash, me, runtime_adapter) };

        // The loop below will be sending PartialEncodedChunkRequestMsg to various block producers.
        // We need to send such a message to the original chunk producer if we do not have the receipts
        //     for some subset of shards, even if we don't need to request any parts from the original
        //     chunk producer.
        if !shards_to_fetch_receipts.is_empty() {
            bp_to_parts.entry(shard_representative_target.clone()).or_default();
        }

        let no_account_id = me.is_none();
        debug!(target: "chunks", "Will send {} requests to fetch chunk parts.", bp_to_parts.len());
        for (target_account, part_ords) in bp_to_parts {
            // extra check that we are not sending request to ourselves.
            if no_account_id || me != &target_account {
                let parts_count = part_ords.len();
                let request = PartialEncodedChunkRequestMsg {
                    chunk_hash: header.chunk_hash(),
                    part_ords,
                    tracking_shards: if target_account == shard_representative_target {
                        shards_to_fetch_receipts.clone()
                    } else {
                        HashSet::new()
                    },
                };
                let target = AccountIdOrPeerTrackingShard {
                    account_id: target_account,
                    prefer_peer: fetch_from_archival || rand::thread_rng().gen::<bool>(),
                    shard_id: header.shard_id(),
                    only_archival: fetch_from_archival,
                    min_height: header.height_created().saturating_sub(CHUNK_REQUEST_PEER_HORIZON),
                };
                debug!(target: "chunks", "Requesting {} parts for shard {} from {:?} prefer {}", parts_count, header.shard_id(), target.account_id, target.prefer_peer);

                peer_manager_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::PartialEncodedChunkRequest {
                        target,
                        request,
                        create_time: Clock::instant().into(),
                    },
                ));
            } else {
                warn!(target: "client", "{:?} requests parts {:?} for chunk {:?} from self",
                    me, part_ords, header.chunk_hash(),
                );
            }
        }

        Some(())
    }

    /// Check whether the node should wait for chunk parts being forwarded to it
    /// The node will wait if it's a block producer or a chunk producer that is responsible
    /// for producing the next chunk in this shard.
    /// `prev_hash`: previous block hash of the chunk that we are requesting
    /// `next_chunk_height`: height of the next chunk of the chunk that we are requesting
    fn should_wait_for_chunk_forwarding(
        epoch_id: &EpochId,
        prev_hash: &CryptoHash,
        shard_id: ShardId,
        next_chunk_height: BlockHeight,
        me: &Option<AccountId>,
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> Result<bool, Error> {
        // chunks will not be forwarded to non-validators
        let me = match me {
            None => return Ok(false),
            Some(it) => it,
        };
        let block_producers =
            runtime_adapter.get_epoch_block_producers_ordered(epoch_id, prev_hash)?;
        for (bp, _) in block_producers {
            if bp.account_id() == me {
                return Ok(true);
            }
        }
        let chunk_producer =
            runtime_adapter.get_chunk_producer(epoch_id, next_chunk_height, shard_id)?;
        if &chunk_producer == me {
            return Ok(true);
        }
        Ok(false)
    }

    /// Get a random shard block producer that is not me.
    fn get_random_target_tracking_shard(
        epoch_id: &EpochId,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        me: &Option<AccountId>,
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> Result<Option<AccountId>, near_chain::Error> {
        let block_producers = runtime_adapter
            .get_epoch_block_producers_ordered(&epoch_id, parent_hash)?
            .into_iter()
            .filter_map(|(validator_stake, is_slashed)| {
                let account_id = Some(validator_stake.take_account_id());
                if !is_slashed
                    && Self::cares_about_shard_this_or_next_epoch(
                        &account_id,
                        parent_hash,
                        shard_id,
                        false,
                        runtime_adapter,
                    )
                    && me != &account_id
                {
                    account_id
                } else {
                    None
                }
            });

        Ok(block_producers.choose(&mut rand::thread_rng()))
    }

    fn get_tracking_shards(
        epoch_id: &EpochId,
        parent_hash: &CryptoHash,
        me: &Option<AccountId>,
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> HashSet<ShardId> {
        (0..runtime_adapter.num_shards(&epoch_id).unwrap())
            .filter(|chunk_shard_id| {
                Self::cares_about_shard_this_or_next_epoch(
                    me,
                    parent_hash,
                    *chunk_shard_id,
                    true,
                    runtime_adapter,
                )
            })
            .collect::<HashSet<_>>()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_a() {}
}
