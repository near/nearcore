use super::state_request_tracker::StateRequestTracker;
use crate::store::utils::{
    get_block_header_on_chain_by_height, get_chunk_clone_from_header,
    get_incoming_receipts_for_shard,
};
use crate::types::{RuntimeAdapter, StatePartValidationResult, StateRootNodeValidationResult};
use crate::validate::validate_chunk_proofs;
use crate::{ReceiptFilter, byzantine_assert, metrics};
use near_async::time::{Clock, Instant};
use near_chain_primitives::error::{Error, LogTransientStorageError};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::{BlockHeader, Tip};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, verify_path, verify_path_with_index};
use near_primitives::sharding::{
    ChunkHashHeight, ReceiptList, ReceiptProof, ShardChunk, ShardChunkHeader, ShardProof,
};
use near_primitives::state_part::{StatePart, StatePartId, StatePartIndex};
use near_primitives::state_sync::{
    ReceiptProofResponse, RootProof, ShardStateSyncResponseHeader, ShardStateSyncResponseHeaderV2,
    ShardStateSyncResponseHeaderV3, SpiceRootProof, StateHeaderKey, StatePartKey,
    get_num_state_parts,
};
use near_primitives::types::{
    ChunkExecutionResult, ChunkExecutionRoots, ShardId, SpiceChunkId, sorted_chunk_execution_roots,
};
use near_primitives::views::RequestedStatePartsView;
use near_store::DBCol;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashSet;
use std::sync::Arc;
use time::ext::InstantExt as _;

fn shard_id_out_of_bounds(shard_id: ShardId) -> Error {
    Error::InvalidStateRequest(format!("shard_id {shard_id:?} out of bounds").into())
}

pub struct ChainStateSyncAdapter {
    clock: Clock,
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime_adapter: Arc<dyn RuntimeAdapter>,

    /// Used to store state parts already requested along with elapsed time
    /// to create the parts. This information is used for debugging.
    requested_state_parts: StateRequestTracker,
}

impl ChainStateSyncAdapter {
    pub fn new(
        clock: Clock,
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
    ) -> Self {
        Self {
            clock,
            chain_store,
            epoch_manager,
            runtime_adapter,
            requested_state_parts: StateRequestTracker::new(),
        }
    }

    /// Computes ShardStateSyncResponseHeader.
    pub fn compute_state_response_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponseHeader, Error> {
        // Consistency rules:
        // 1. Everything prefixed with `sync_` indicates new epoch, for which we are syncing.
        // 1a. `sync_prev` means the last of the prev epoch.
        // 2. Empty prefix means the height where chunk was applied last time in the prev epoch.
        //    Let's call it `current`.
        // 2a. `prev_` means we're working with height before current.
        // 3. In inner loops we use all prefixes with no relation to the context described above.
        let sync_block = self
            .chain_store
            .get_block(&sync_hash)
            .log_storage_error("block has already been checked for existence")?;
        let sync_block_header = sync_block.header();
        let sync_block_epoch_id = sync_block_header.epoch_id();
        let shard_ids = self.epoch_manager.shard_ids(sync_block_epoch_id)?;
        if !shard_ids.contains(&shard_id) {
            return Err(shard_id_out_of_bounds(shard_id));
        }
        if sync_block_header.is_spice() {
            return self.compute_spice_state_response_header(shard_id, sync_hash);
        }

        // The chunk was applied at height `chunk_header.height_included`.
        // Getting the `current` state.
        // TODO(current_epoch_state_sync): check that the sync block is what we would expect. So, either the first
        // block of an epoch, or the first block where there have been two new chunks in the epoch
        let sync_prev_block = self.chain_store.get_block(sync_block_header.prev_hash())?;

        let shard_layout = self.epoch_manager.get_shard_layout(sync_block_epoch_id)?;
        let prev_epoch_id = sync_prev_block.header().epoch_id();
        let prev_shard_layout = self.epoch_manager.get_shard_layout(&prev_epoch_id)?;
        let prev_shard_index = prev_shard_layout.get_shard_index(shard_id)?;

        // Chunk header here is the same chunk header as at the `current` height.
        let sync_prev_hash = sync_prev_block.hash();
        let chunks = sync_prev_block.chunks();
        let chunk_header = chunks.get(prev_shard_index).ok_or(Error::InvalidShardId(shard_id))?;
        let (chunk_headers_root, chunk_proofs) = merklize(
            &sync_prev_block
                .chunks()
                .iter()
                .map(|shard_chunk| {
                    ChunkHashHeight(shard_chunk.chunk_hash().clone(), shard_chunk.height_included())
                })
                .collect::<Vec<ChunkHashHeight>>(),
        );
        assert_eq!(&chunk_headers_root, sync_prev_block.header().chunk_headers_root());

        // If the node was not tracking the shard it may not have the chunk in storage.
        let chunk = get_chunk_clone_from_header(&self.chain_store.chunk_store(), chunk_header)?;
        let chunk_proof =
            chunk_proofs.get(prev_shard_index).ok_or(Error::InvalidShardId(shard_id))?.clone();
        let block_header = get_block_header_on_chain_by_height(
            &self.chain_store,
            &sync_hash,
            chunk_header.height_included(),
        )?;

        // Collecting the `prev` state.
        let (prev_chunk_header, prev_chunk_proof, prev_chunk_height_included) =
            match self.chain_store.get_block(block_header.prev_hash()) {
                Ok(prev_block) => {
                    let prev_chunk_header = prev_block
                        .chunks()
                        .get(prev_shard_index)
                        .ok_or(Error::InvalidShardId(shard_id))?
                        .clone();
                    let (prev_chunk_headers_root, prev_chunk_proofs) = merklize(
                        &prev_block
                            .chunks()
                            .iter()
                            .map(|shard_chunk| {
                                ChunkHashHeight(
                                    shard_chunk.chunk_hash().clone(),
                                    shard_chunk.height_included(),
                                )
                            })
                            .collect::<Vec<ChunkHashHeight>>(),
                    );
                    assert_eq!(&prev_chunk_headers_root, prev_block.header().chunk_headers_root());

                    let prev_chunk_proof = prev_chunk_proofs
                        .get(prev_shard_index)
                        .ok_or(Error::InvalidShardId(shard_id))?
                        .clone();
                    let prev_chunk_height_included = prev_chunk_header.height_included();

                    (Some(prev_chunk_header), Some(prev_chunk_proof), prev_chunk_height_included)
                }
                Err(e) => match e {
                    Error::DBNotFoundErr(_) => {
                        if block_header.is_genesis() {
                            (None, None, 0)
                        } else {
                            return Err(e);
                        }
                    }
                    _ => return Err(e),
                },
            };

        // Getting all existing incoming_receipts from prev_chunk height up to the sync hash.
        let incoming_receipts_proofs = get_incoming_receipts_for_shard(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            shard_id,
            &shard_layout,
            sync_hash,
            prev_chunk_height_included,
            ReceiptFilter::All,
        )?;

        // Collecting proofs for incoming receipts.
        let mut root_proofs = vec![];
        for receipt_response in &incoming_receipts_proofs {
            let ReceiptProofResponse(block_hash, receipt_proofs) = receipt_response;
            let block_header = self.chain_store.get_block_header(&block_hash)?.clone();
            let block = self.chain_store.get_block(&block_hash)?;
            let block_shard_layout =
                self.epoch_manager.get_shard_layout(block_header.epoch_id())?;
            let block_chunks = block.chunks();
            let (block_receipts_root, block_receipts_proofs) = merklize(
                &block_chunks
                    .iter()
                    .map(|chunk| *chunk.prev_outgoing_receipts_root())
                    .collect::<Vec<CryptoHash>>(),
            );

            let mut root_proofs_cur = vec![];
            if receipt_proofs.len() != block_header.chunks_included() as usize {
                // Incoming receipts are saved to the store during block processing.
                // If the node did not process the required blocks or was not tracking
                // any shards during that time, it won't have the incoming receipts.
                return Err(Error::Other("Store is missing incoming receipts".to_owned()));
            }
            for receipt_proof in receipt_proofs.iter() {
                let ReceiptProof(receipts, shard_proof) = receipt_proof;
                let ShardProof { from_shard_id, to_shard_id: _, proof } = shard_proof;
                let receipts_hash = CryptoHash::hash_borsh(ReceiptList(shard_id, receipts));
                // `block_receipts_proofs` is merklized over this block's chunks, so the leaf index
                // comes from this block's layout. `get_incoming_receipts_for_shard` walks back
                // across epoch boundaries, so it need not be the sync block's layout.
                let from_shard_index = block_shard_layout.get_shard_index(*from_shard_id)?;

                let (Some(from_chunk), Some(block_receipts_proof)) = (
                    block_chunks.get(from_shard_index),
                    block_receipts_proofs.get(from_shard_index),
                ) else {
                    return Err(Error::InvalidReceiptsProof);
                };
                let root_proof = *from_chunk.prev_outgoing_receipts_root();
                if block_header.prev_chunk_outgoing_receipts_root() != &block_receipts_root
                    || !verify_path(root_proof, proof, &receipts_hash)
                    || !verify_path(block_receipts_root, block_receipts_proof, &root_proof)
                {
                    return Err(Error::InvalidReceiptsProof);
                }
                root_proofs_cur.push(RootProof(root_proof, block_receipts_proof.clone()));
            }
            root_proofs.push(root_proofs_cur);
        }

        let state_root_node = self.runtime_adapter.get_state_root_node(
            shard_id,
            sync_prev_hash,
            &chunk_header.prev_state_root(),
        )?;

        let (chunk, prev_chunk_header) = match chunk {
            ShardChunk::V1(chunk) => {
                let prev_chunk_header =
                    prev_chunk_header.and_then(|prev_header| match prev_header {
                        ShardChunkHeader::V1(header) => Some(ShardChunkHeader::V1(header)),
                        ShardChunkHeader::V2(_) => None,
                        ShardChunkHeader::V3(_) => None,
                    });
                let chunk = ShardChunk::V1(chunk);
                (chunk, prev_chunk_header)
            }
            chunk @ ShardChunk::V2(_) => (chunk, prev_chunk_header),
        };

        let shard_state_header = ShardStateSyncResponseHeaderV2 {
            chunk,
            chunk_proof,
            prev_chunk_header,
            prev_chunk_proof,
            incoming_receipts_proofs,
            root_proofs,
            state_root_node,
        };

        Ok(ShardStateSyncResponseHeader::V2(shard_state_header))
    }

    /// The spice state sync header for `shard_id` at `sync_hash`.
    ///
    /// `sync_hash` is the epoch's first block and the state being synced is the one its chunk
    /// leaves behind, so the header carries no chunk to apply afterwards - only the state root
    /// node and the proof that binds its root to the chain.
    ///
    /// A spice chunk header commits no state root of its own, since the chunk executes after
    /// the block that carries it. The root comes from the chunk's `ChunkExecutionResult`
    /// instead, which a later block commits through the `chunk_execution_root` field of its
    /// header. Header sync precedes state sync, so the syncing node already holds that header
    /// and only the leaf and the path to it have to travel.
    fn compute_spice_state_response_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponseHeader, Error> {
        let chunk_id = SpiceChunkId { block_hash: sync_hash, shard_id };
        let (execution_result, state_root_proof) = self.build_spice_root_proof(&chunk_id)?;
        let state_root = *execution_result.chunk_extra.state_root();
        let state_root_node =
            self.runtime_adapter.get_state_root_node(shard_id, &sync_hash, &state_root)?;
        Ok(ShardStateSyncResponseHeader::V3(ShardStateSyncResponseHeaderV3 {
            state_root_node,
            state_root_proof,
            execution_result,
        }))
    }

    /// The chunk's certified execution result together with a proof of its `ChunkExecutionRoots`
    /// leaf against the committing block's `chunk_execution_root`.
    ///
    /// The committing block comes from the `chunk_certifying_block` index, which is written when
    /// a block becomes final, so serving a header at all means the chunk is certified.
    fn build_spice_root_proof(
        &self,
        chunk_id: &SpiceChunkId,
    ) -> Result<(ChunkExecutionResult, SpiceRootProof), Error> {
        let Some(committing_block_hash) = self.chain_store.get_chunk_certifying_block(chunk_id)
        else {
            return Err(Error::Other(format!(
                "no certifying block for chunk {chunk_id:?}; its execution result is not certified yet"
            )));
        };
        let committing_block = self.chain_store.get_block(&committing_block_hash)?;
        let core_statements = committing_block.spice_core_statements();
        let Some((_, execution_result)) =
            core_statements.iter_execution_results().find(|(id, _)| *id == chunk_id)
        else {
            return Err(Error::Other(format!(
                "block {committing_block_hash} does not commit an execution result for {chunk_id:?}"
            )));
        };
        let leaves: Vec<ChunkExecutionRoots> =
            sorted_chunk_execution_roots(core_statements.iter_execution_results());
        let index = leaves
            .iter()
            .position(|leaf| leaf.chunk_id() == chunk_id)
            .expect("the leaves cover every execution result the block commits");
        let (root, proofs) = merklize(&leaves);
        if Some(root) != committing_block.header().chunk_execution_root() {
            return Err(Error::Other(format!(
                "block {committing_block_hash} chunk_execution_root does not match its own execution results"
            )));
        }
        let proof = SpiceRootProof {
            committing_block_hash,
            roots: leaves[index].clone(),
            proof: proofs[index].clone(),
        };
        Ok((execution_result.clone(), proof))
    }

    /// Returns ShardStateSyncResponseHeader for the given epoch and shard.
    /// If the header is already available in the DB, returns the cached version and doesn't recompute it.
    /// If the header was computed then it also gets cached in the DB.
    pub fn get_state_response_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponseHeader, Error> {
        // Check cache
        let key = borsh::to_vec(&StateHeaderKey(shard_id, sync_hash)).unwrap();
        if let Some(header) = self.chain_store.store().get_ser(DBCol::StateHeaders, &key) {
            return Ok(header);
        }

        let shard_state_header = self.compute_state_response_header(shard_id, sync_hash)?;

        // Saving the header data
        let mut store_update = self.chain_store.store().store_update();
        store_update.set_ser(DBCol::StateHeaders, &key, &shard_state_header);
        store_update.commit();

        Ok(shard_state_header)
    }

    pub fn get_state_response_part(
        &mut self,
        shard_id: ShardId,
        part_idx: StatePartIndex,
        sync_hash: CryptoHash,
    ) -> Result<StatePart, Error> {
        let _span = tracing::debug_span!(
            target: "sync",
            "get_state_response_part",
            %shard_id,
            part_idx,
            ?sync_hash)
        .entered();
        let block = self
            .chain_store
            .get_block(&sync_hash)
            .log_storage_error("block has already been checked for existence")?;
        let header = block.header();
        let epoch_id = block.header().epoch_id();
        // Check cache
        let key = borsh::to_vec(&StatePartKey(sync_hash, shard_id, part_idx)).unwrap();
        if let Some(bytes) = self.chain_store.store_ref().get(DBCol::StateParts, &key) {
            metrics::STATE_PART_CACHE_HIT.inc();
            let state_part = StatePart::from_bytes(bytes.to_vec())?;
            return Ok(state_part);
        }
        metrics::STATE_PART_CACHE_MISS.inc();

        let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
        let shard_ids = self.epoch_manager.shard_ids(epoch_id)?;
        if !shard_ids.contains(&shard_id) {
            return Err(shard_id_out_of_bounds(shard_id));
        }
        // The two hashes below are the block the state root belongs to and the block the
        // snapshot is keyed by. Non-spice syncs the state from before the sync-prev block's
        // chunk ran, whose snapshot sits at the block before that; spice syncs the state from
        // after the sync block's own chunk ran, and snapshots it at the sync block itself.
        let (state_root, root_node_hash, snapshot_hash) = if header.is_spice() {
            let chunk_id = SpiceChunkId { block_hash: sync_hash, shard_id };
            let (execution_result, _) = self.build_spice_root_proof(&chunk_id)?;
            (*execution_result.chunk_extra.state_root(), sync_hash, sync_hash)
        } else {
            let prev_block = self.chain_store.get_block(header.prev_hash())?;
            let shard_index = shard_layout.get_shard_index(shard_id)?;
            let state_root = prev_block
                .chunks()
                .get(shard_index)
                .ok_or(Error::InvalidShardId(shard_id))?
                .prev_state_root();
            (state_root, *prev_block.hash(), *prev_block.header().prev_hash())
        };
        let state_root_node = self
            .runtime_adapter
            .get_state_root_node(shard_id, &root_node_hash, &state_root)
            .log_storage_error("get_state_root_node fail")?;
        let num_parts = get_num_state_parts(state_root_node.memory_usage);
        if part_idx >= num_parts {
            return Err(shard_id_out_of_bounds(shard_id));
        }
        let current_time = Instant::now();
        let state_part = self
            .runtime_adapter
            .obtain_state_part(
                shard_id,
                &snapshot_hash,
                &state_root,
                StatePartId::new(part_idx, num_parts),
            )
            .log_storage_error("obtain_state_part fail")?;

        let elapsed_ms = (self.clock.now().signed_duration_since(current_time))
            .whole_milliseconds()
            .max(0) as u128;
        self.requested_state_parts
            .save_state_part_elapsed(&sync_hash, &shard_id, &part_idx, elapsed_ms);

        // Cache the part data, but only if the corresponding header is also cached.
        // At epoch boundaries, clear_all_downloaded_parts() deletes all cached headers
        // and parts. Since serving runs on a separate actor, a part request can arrive
        // after the clear and re-create a StatePartKey without its StateHeaderKey,
        // which the storage validator treats as an inconsistency.
        let header_key = borsh::to_vec(&StateHeaderKey(shard_id, sync_hash)).unwrap();
        if self.chain_store.store_ref().exists(DBCol::StateHeaders, &header_key) {
            let mut store_update = self.chain_store.store().store_update();
            let bytes = state_part.to_bytes();
            store_update.set(DBCol::StateParts, &key, &bytes);
            store_update.commit();
        }

        Ok(state_part)
    }

    pub fn get_state_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponseHeader, Error> {
        self.chain_store.get_state_header(shard_id, sync_hash)
    }

    pub fn set_state_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        shard_state_header: ShardStateSyncResponseHeader,
    ) -> Result<(), Error> {
        let sync_block_header = self.chain_store.get_block_header(&sync_hash)?;

        if sync_block_header.is_spice() {
            return self.set_spice_state_header(shard_id, sync_hash, shard_state_header);
        }

        let Some(chunk) = shard_state_header.cloned_chunk() else {
            return Err(Error::Other(
                "set_shard_state failed: a non-spice header must carry a chunk".into(),
            ));
        };
        let prev_chunk_header = shard_state_header.cloned_prev_chunk_header();

        // 1-2. Checking chunk validity
        if !validate_chunk_proofs(&chunk, self.epoch_manager.as_ref())? {
            byzantine_assert!(false);
            return Err(Error::Other(
                "set_shard_state failed: chunk header proofs are invalid".into(),
            ));
        }

        // Consider chunk itself is valid.

        // 3. Checking that chunks `chunk` and `prev_chunk` are included in appropriate blocks
        // 3a. Checking that chunk `chunk` is included into block at last height before sync_hash
        // 3aa. Also checking chunk.height_included
        let sync_prev_block_header =
            self.chain_store.get_block_header(sync_block_header.prev_hash())?;
        let chunk_proof = shard_state_header
            .chunk_proof()
            .ok_or_else(|| Error::Other("set_shard_state failed: missing chunk proof".into()))?;
        if !verify_path(
            *sync_prev_block_header.chunk_headers_root(),
            chunk_proof,
            &ChunkHashHeight(chunk.chunk_hash().clone(), chunk.height_included()),
        ) {
            byzantine_assert!(false);
            return Err(Error::Other(
                "set_shard_state failed: chunk isn't included into block".into(),
            ));
        }

        let block_header = get_block_header_on_chain_by_height(
            &self.chain_store,
            &sync_hash,
            chunk.height_included(),
        )?;
        // 3b. Checking that chunk `prev_chunk` is included into block at height before chunk.height_included
        // 3ba. Also checking prev_chunk.height_included - it's important for getting correct incoming receipts
        match (&prev_chunk_header, shard_state_header.prev_chunk_proof()) {
            (Some(prev_chunk_header), Some(prev_chunk_proof)) => {
                let prev_block_header =
                    self.chain_store.get_block_header(block_header.prev_hash())?;
                if !verify_path(
                    *prev_block_header.chunk_headers_root(),
                    prev_chunk_proof,
                    &ChunkHashHeight(prev_chunk_header.chunk_hash().clone(), prev_chunk_header.height_included()),
                ) {
                    byzantine_assert!(false);
                    return Err(Error::Other(
                        "set_shard_state failed: prev_chunk isn't included into block".into(),
                    ));
                }
            }
            (None, None) => {
                if chunk.height_included() != 0 {
                    return Err(Error::Other(
                    "set_shard_state failed: received empty state response for a chunk that is not at height 0".into()
                ));
                }
            }
            _ =>
                return Err(Error::Other("set_shard_state failed: `prev_chunk_header` and `prev_chunk_proof` must either both be present or both absent".into()))
        };

        // 4. Proving incoming receipts validity
        // 4a. Checking len of proofs
        if shard_state_header.root_proofs().len()
            != shard_state_header.incoming_receipts_proofs().len()
        {
            byzantine_assert!(false);
            return Err(Error::Other("set_shard_state failed: invalid proofs".into()));
        }
        let mut hash_to_compare = sync_hash;
        for (i, receipt_response) in
            shard_state_header.incoming_receipts_proofs().iter().enumerate()
        {
            let ReceiptProofResponse(block_hash, receipt_proofs) = receipt_response;

            // 4b. Checking that there is a valid sequence of continuous blocks
            if *block_hash != hash_to_compare {
                byzantine_assert!(false);
                return Err(Error::Other(
                    "set_shard_state failed: invalid incoming receipts".into(),
                ));
            }
            let header = self.chain_store.get_block_header(&hash_to_compare)?;
            hash_to_compare = *header.prev_hash();

            let block_header = self.chain_store.get_block_header(block_hash)?;
            let block_shard_layout =
                self.epoch_manager.get_shard_layout(block_header.epoch_id())?;
            // 4c. Checking len of receipt_proofs for current block
            if receipt_proofs.len() != shard_state_header.root_proofs()[i].len()
                || receipt_proofs.len() != block_header.chunks_included() as usize
            {
                byzantine_assert!(false);
                return Err(Error::Other("set_shard_state failed: invalid proofs".into()));
            }
            // We know there were exactly `block_header.chunks_included` chunks included
            // on the height of block `block_hash`.
            // There were no other proofs except for included chunks.
            // According to Pigeonhole principle, it's enough to ensure all receipt_proofs are distinct
            // to prove that all receipts were received and no receipts were hidden.
            let mut visited_shard_ids = HashSet::<ShardId>::new();
            for (j, receipt_proof) in receipt_proofs.iter().enumerate() {
                let ReceiptProof(receipts, shard_proof) = receipt_proof;
                let ShardProof { from_shard_id, to_shard_id: _, proof } = shard_proof;
                // 4d. Checking uniqueness for set of `from_shard_id`
                match visited_shard_ids.get(from_shard_id) {
                    Some(_) => {
                        byzantine_assert!(false);
                        return Err(Error::Other("set_shard_state failed: invalid proofs".into()));
                    }
                    _ => visited_shard_ids.insert(*from_shard_id),
                };
                let RootProof(root, block_proof) = &shard_state_header.root_proofs()[i][j];
                let receipts_hash = CryptoHash::hash_borsh(ReceiptList(shard_id, receipts));
                // 4e. Proving the set of receipts is the subset of outgoing_receipts of shard `shard_id`
                if !verify_path(*root, proof, &receipts_hash) {
                    byzantine_assert!(false);
                    return Err(Error::Other("set_shard_state failed: invalid proofs".into()));
                }
                // 4f. Proving the outgoing_receipts_root matches that in the block, at the chunk
                // index `from_shard_id` names. No merkle root covers that field, so the index check
                // is what binds it. A shard with no new chunk keeps the previous chunk header, so
                // its leaf repeats an older root; the index must name a chunk this block included.
                let from_shard_index = block_shard_layout.get_shard_index(*from_shard_id)?;
                let has_new_chunk =
                    block_header.chunk_mask().get(from_shard_index).copied().unwrap_or(false);
                if !has_new_chunk
                    || !verify_path_with_index(
                        *block_header.prev_chunk_outgoing_receipts_root(),
                        block_proof,
                        root,
                        from_shard_index as u64,
                        block_shard_layout.num_shards(),
                    )
                {
                    byzantine_assert!(false);
                    return Err(Error::Other("set_shard_state failed: invalid proofs".into()));
                }
            }
        }
        // 4g. Checking that there are no more heights to get incoming_receipts
        let header = self.chain_store.get_block_header(&hash_to_compare)?;
        if header.height() != prev_chunk_header.map_or(0, |h| h.height_included()) {
            byzantine_assert!(false);
            return Err(Error::Other("set_shard_state failed: invalid incoming receipts".into()));
        }

        // 5. Checking that state_root_node is valid
        let chunk_inner = chunk.take_header().take_inner();
        if matches!(
            self.runtime_adapter.validate_state_root_node(
                shard_state_header.state_root_node(),
                chunk_inner.prev_state_root(),
            ),
            StateRootNodeValidationResult::Invalid
        ) {
            byzantine_assert!(false);
            return Err(Error::Other("set_shard_state failed: state_root_node is invalid".into()));
        }

        // Saving the header data.
        let mut store_update = self.chain_store.store().store_update();
        let key = borsh::to_vec(&StateHeaderKey(shard_id, sync_hash)).unwrap();
        store_update.set_ser(DBCol::StateHeaders, &key, &shard_state_header);
        store_update.commit();

        Ok(())
    }

    /// Verifies and stores a spice state sync header.
    ///
    /// There is no chunk and no incoming receipts to check here: the state being synced already
    /// includes the sync block's chunk, so nothing is applied on top of it. What has to hold is
    /// that the state root node describes the root the chain committed for that chunk, which
    /// `SpiceRootProof` establishes against a block header the node already has from header sync.
    fn set_spice_state_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        shard_state_header: ShardStateSyncResponseHeader,
    ) -> Result<(), Error> {
        let (Some(root_proof), Some(execution_result)) =
            (shard_state_header.spice_root_proof(), shard_state_header.spice_execution_result())
        else {
            byzantine_assert!(false);
            return Err(Error::Other(
                "set_shard_state failed: a spice header must carry a root proof".into(),
            ));
        };

        // 1. The leaf must be the one for this shard's chunk in the sync block. Without this a
        // valid leaf from an unrelated chunk would satisfy the path.
        let chunk_id = SpiceChunkId { block_hash: sync_hash, shard_id };
        if root_proof.roots.chunk_id() != &chunk_id {
            byzantine_assert!(false);
            return Err(Error::Other(
                "set_shard_state failed: root proof is for a different chunk".into(),
            ));
        }

        // 2. The committing block must descend from the sync block. Execution results are
        // committed by a later block, and only a descendant's commitment says anything about
        // the chain the node is syncing to.
        let committing_header =
            self.chain_store.get_block_header(&root_proof.committing_block_hash)?;
        if !self.descends_from_sync_block(&committing_header, &sync_hash)? {
            byzantine_assert!(false);
            return Err(Error::Other(
                "set_shard_state failed: committing block does not follow the sync block".into(),
            ));
        }

        // 3. The leaf must be committed by that block's `chunk_execution_root`.
        let Some(chunk_execution_root) = committing_header.chunk_execution_root() else {
            byzantine_assert!(false);
            return Err(Error::Other(
                "set_shard_state failed: committing block commits no execution roots".into(),
            ));
        };
        if !verify_path(chunk_execution_root, &root_proof.proof, &root_proof.roots) {
            byzantine_assert!(false);
            return Err(Error::Other(
                "set_shard_state failed: root proof does not verify against chunk_execution_root"
                    .into(),
            ));
        }

        // 4. The execution result the node will record as the sync block's `ChunkExtra` must be
        // the one behind the proven leaf. The leaf carries `execution_result_hash`, so
        // re-deriving it covers the result in full - the three roots and every other
        // `ChunkExtra` field, gas limit and congestion info included.
        if ChunkExecutionRoots::from_execution_result(&chunk_id, execution_result)
            != root_proof.roots
        {
            byzantine_assert!(false);
            return Err(Error::Other(
                "set_shard_state failed: execution result does not match the proven leaf".into(),
            ));
        }

        // 5. And the state root node must be the root of the trie the parts will rebuild.
        if matches!(
            self.runtime_adapter.validate_state_root_node(
                shard_state_header.state_root_node(),
                root_proof.roots.state_root(),
            ),
            StateRootNodeValidationResult::Invalid
        ) {
            byzantine_assert!(false);
            return Err(Error::Other("set_shard_state failed: state_root_node is invalid".into()));
        }

        let mut store_update = self.chain_store.store().store_update();
        let key = borsh::to_vec(&StateHeaderKey(shard_id, sync_hash)).unwrap();
        store_update.set_ser(DBCol::StateHeaders, &key, &shard_state_header);
        store_update.commit();
        Ok(())
    }

    /// Whether `header` sits above `sync_hash` on the same chain, walking back from it. The
    /// walk is short because a chunk is certified within a few blocks of the one carrying it.
    ///
    /// This does not require the block to be on the canonical chain, which the node cannot
    /// tell during state sync. It does not have to: a fork descending from the sync block
    /// could only commit a different execution result for the same chunk if two thirds of the
    /// validators endorsed two results for it.
    fn descends_from_sync_block(
        &self,
        header: &BlockHeader,
        sync_hash: &CryptoHash,
    ) -> Result<bool, Error> {
        let sync_height = self.chain_store.get_block_header(sync_hash)?.height();
        let mut hash = *header.hash();
        loop {
            let header = self.chain_store.get_block_header(&hash)?;
            if header.height() <= sync_height {
                return Ok(false);
            }
            hash = *header.prev_hash();
            if &hash == sync_hash {
                return Ok(true);
            }
        }
    }

    pub fn set_state_part(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: StatePartId,
        part: &StatePart,
    ) -> Result<(), Error> {
        let shard_state_header = self.get_state_header(shard_id, sync_hash)?;
        let state_root = shard_state_header.synced_state_root();
        if matches!(
            self.runtime_adapter.validate_state_part(shard_id, &state_root, part_id, part),
            StatePartValidationResult::Invalid
        ) {
            byzantine_assert!(false);
            return Err(Error::Other(format!(
                "set_state_part failed: validate_state_part failed. state_root={:?}",
                state_root
            )));
        }
        // Saving the part data.
        let mut store_update = self.chain_store.store().store_update();
        let key = borsh::to_vec(&StatePartKey(sync_hash, shard_id, part_id.index)).unwrap();
        let bytes = part.to_bytes();
        store_update.set(DBCol::StateParts, &key, &bytes);
        store_update.commit();
        Ok(())
    }

    pub fn get_requested_state_parts(&self) -> Vec<RequestedStatePartsView> {
        self.requested_state_parts.get_requested_state_parts()
    }

    /// Returns whether `tip.last_block_hash` is the block that will appear immediately before the "sync_hash" block.
    pub fn is_sync_prev_hash(&self, tip: &Tip) -> Result<bool, Error> {
        crate::state_sync::utils::is_sync_prev_hash(&self.chain_store, tip)
    }
}
