use std::{collections::HashMap, time::Instant};

use near_chain::{validate::validate_chunk_proofs, Chain, ChainStore, RuntimeAdapter};
use near_chunks_primitives::Error;
use near_primitives::{
    hash::CryptoHash,
    merkle::{merklize, MerklePath},
    receipt::Receipt,
    sharding::{
        EncodedShardChunk, PartialEncodedChunk, PartialEncodedChunkPart, PartialEncodedChunkV1,
        PartialEncodedChunkV2, ReceiptProof, ReedSolomonWrapper, ShardChunk, ShardChunkHeader,
        ShardProof,
    },
    types::{AccountId, EpochId, ShardId},
};

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
    pub parts_needed: Vec<u64>,
    pub receipt_proofs_needed: Vec<ShardId>,
    pub cares_about_shard: bool,
}

pub struct ChunkState {
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
    pub errors: Vec<Error>,

    pub next_request_due: Option<Instant>,
}

impl ChunkState {
    pub fn new() -> Self {
        Self {
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
            errors: vec![],

            next_request_due: None,
        }
    }

    fn ok_or_none<T, E>(&mut self, result: Result<T, E>) -> Option<T>
    where
        E: Into<Error>,
    {
        match result {
            Ok(value) => Some(value),
            Err(err) => {
                self.errors.push(err.into());
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

    fn get_or_compute_whats_needed(
        &mut self,
        me: &Option<AccountId>,
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> Option<&ChunkNeededInfo> {
        if self.needed.is_none() {
            let epoch_id = self.get_or_compute_confirmed_epoch_id(runtime_adapter)?;
            let prev_block_hash = self.header.as_ref().unwrap().prev_block_hash().clone();
            let mut parts_needed = vec![];
            for part_ord in 0..runtime_adapter.num_total_parts() as u64 {
                if Self::need_part(&epoch_id, part_ord, me, runtime_adapter) {
                    parts_needed.push(part_ord);
                }
            }
            let mut receipt_proofs_needed = vec![];
            let num_shards = self.ok_or_none(runtime_adapter.num_shards(&epoch_id)).unwrap_or(0);
            for shard_id in 0..num_shards {
                if Self::need_receipt(&prev_block_hash, shard_id, me, runtime_adapter) {
                    receipt_proofs_needed.push(shard_id);
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
            match &self.header {
                None => return None,
                Some(header) => {
                    let epoch_id = runtime_adapter
                        .get_epoch_id_from_prev_block(header.prev_block_hash())
                        .ok()?;
                    self.confirmed_epoch_id = Some(epoch_id);
                }
            }
        }
        self.confirmed_epoch_id.as_ref()
    }

    fn get_or_compute_completed_chunk(
        &mut self,
        me: &Option<AccountId>,
        runtime_adapter: &dyn RuntimeAdapter,
        rs: &mut ReedSolomonWrapper,
        chain_store: &mut ChainStore,
    ) -> Option<&(ShardChunk, EncodedShardChunk)> {
        if self.completed_chunk.is_none() {
            let needed = self.get_or_compute_whats_needed(me, runtime_adapter)?;
            let header = self.header.as_ref()?;
            let epoch_id = self.get_or_compute_confirmed_epoch_id(runtime_adapter)?;
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
                    encoded_chunk,
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
            let needed = self.get_or_compute_whats_needed(me, runtime_adapter)?;
            let (header, parts, receipts) = if let Some((shard_chunk, encoded_chunk)) =
                self.get_or_compute_completed_chunk(me, runtime_adapter, rs, chain_store)
            {
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
            let filtered_parts = parts
                .into_iter()
                .filter(|part| {
                    needed.cares_about_shard || needed.parts_needed.contains(&part.part_ord)
                })
                .collect::<Vec<_>>();
            let filtered_receipts = receipts
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
        encoded_chunk: EncodedShardChunk,
        rs: &mut ReedSolomonWrapper,
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> Result<ShardChunk, Error> {
        let chunk_hash = encoded_chunk.chunk_hash();
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
}
