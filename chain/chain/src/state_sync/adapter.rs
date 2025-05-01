use super::state_request_tracker::StateRequestTracker;
use crate::store::utils::{
    get_block_header_on_chain_by_height, get_chunk_clone_from_header,
    get_incoming_receipts_for_shard,
};
use crate::types::RuntimeAdapter;
use crate::validate::validate_chunk_proofs;
use crate::{ReceiptFilter, byzantine_assert, metrics};
use near_async::time::{Clock, Instant};
use near_chain_primitives::error::{Error, LogTransientStorageError};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, verify_path};
use near_primitives::sharding::{
    ChunkHashHeight, ReceiptList, ReceiptProof, ShardChunk, ShardChunkHeader, ShardProof,
};
use near_primitives::state_part::PartId;
use near_primitives::state_sync::{
    ReceiptProofResponse, RootProof, ShardStateSyncResponseHeader, ShardStateSyncResponseHeaderV2,
    StateHeaderKey, StatePartKey, get_num_state_parts,
};
use near_primitives::types::ShardId;
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
                .iter_deprecated()
                .map(|shard_chunk| {
                    ChunkHashHeight(shard_chunk.chunk_hash(), shard_chunk.height_included())
                })
                .collect::<Vec<ChunkHashHeight>>(),
        );
        assert_eq!(&chunk_headers_root, sync_prev_block.header().chunk_headers_root());

        // If the node was not tracking the shard it may not have the chunk in storage.
        let chunk = get_chunk_clone_from_header(&self.chain_store, chunk_header)?;
        let chunk_proof =
            chunk_proofs.get(prev_shard_index).ok_or(Error::InvalidShardId(shard_id))?.clone();
        let block_header = get_block_header_on_chain_by_height(
            &self.chain_store,
            &sync_hash,
            chunk_header.height_included(),
        )?;

        // Collecting the `prev` state.
        let (prev_chunk_header, prev_chunk_proof, prev_chunk_height_included) = match self
            .chain_store
            .get_block(block_header.prev_hash())
        {
            Ok(prev_block) => {
                let prev_chunk_header = prev_block
                    .chunks()
                    .get(prev_shard_index)
                    .ok_or(Error::InvalidShardId(shard_id))?
                    .clone();
                let (prev_chunk_headers_root, prev_chunk_proofs) = merklize(
                    &prev_block
                        .chunks()
                        .iter_deprecated()
                        .map(|shard_chunk| {
                            ChunkHashHeight(shard_chunk.chunk_hash(), shard_chunk.height_included())
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
            let (block_receipts_root, block_receipts_proofs) = merklize(
                &block
                    .chunks()
                    .iter_deprecated()
                    .map(|chunk| chunk.prev_outgoing_receipts_root())
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
                let from_shard_index = prev_shard_layout.get_shard_index(*from_shard_id)?;

                let root_proof = block.chunks()[from_shard_index].prev_outgoing_receipts_root();
                root_proofs_cur
                    .push(RootProof(root_proof, block_receipts_proofs[from_shard_index].clone()));

                // Make sure we send something reasonable.
                assert_eq!(block_header.prev_chunk_outgoing_receipts_root(), &block_receipts_root);
                assert!(verify_path(root_proof, proof, &receipts_hash));
                assert!(verify_path(
                    block_receipts_root,
                    &block_receipts_proofs[from_shard_index],
                    &root_proof,
                ));
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

    /// Returns ShardStateSyncResponseHeader for the given epoch and shard.
    /// If the header is already available in the DB, returns the cached version and doesn't recompute it.
    /// If the header was computed then it also gets cached in the DB.
    pub fn get_state_response_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponseHeader, Error> {
        // Check cache
        let key = borsh::to_vec(&StateHeaderKey(shard_id, sync_hash))?;
        if let Ok(Some(header)) = self.chain_store.store().get_ser(DBCol::StateHeaders, &key) {
            return Ok(header);
        }

        let shard_state_header = self.compute_state_response_header(shard_id, sync_hash)?;

        // Saving the header data
        let mut store_update = self.chain_store.store().store_update();
        store_update.set_ser(DBCol::StateHeaders, &key, &shard_state_header)?;
        store_update.commit()?;

        Ok(shard_state_header)
    }

    pub fn get_state_response_part(
        &mut self,
        shard_id: ShardId,
        part_id: u64,
        sync_hash: CryptoHash,
    ) -> Result<Vec<u8>, Error> {
        let _span = tracing::debug_span!(
            target: "sync",
            "get_state_response_part",
            ?shard_id,
            part_id,
            ?sync_hash)
        .entered();
        // Check cache
        let key = borsh::to_vec(&StatePartKey(sync_hash, shard_id, part_id))?;
        if let Ok(Some(state_part)) = self.chain_store.store_ref().get(DBCol::StateParts, &key) {
            metrics::STATE_PART_CACHE_HIT.inc();
            return Ok(state_part.into());
        }
        metrics::STATE_PART_CACHE_MISS.inc();

        let block = self
            .chain_store
            .get_block(&sync_hash)
            .log_storage_error("block has already been checked for existence")?;
        let header = block.header();
        let epoch_id = block.header().epoch_id();
        let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
        let shard_ids = self.epoch_manager.shard_ids(epoch_id)?;
        if !shard_ids.contains(&shard_id) {
            return Err(shard_id_out_of_bounds(shard_id));
        }
        let prev_block = self.chain_store.get_block(header.prev_hash())?;
        let shard_index = shard_layout.get_shard_index(shard_id)?;
        let state_root = prev_block
            .chunks()
            .get(shard_index)
            .ok_or(Error::InvalidShardId(shard_id))?
            .prev_state_root();
        let prev_hash = *prev_block.hash();
        let prev_prev_hash = *prev_block.header().prev_hash();
        let state_root_node = self
            .runtime_adapter
            .get_state_root_node(shard_id, &prev_hash, &state_root)
            .log_storage_error("get_state_root_node fail")?;
        let num_parts = get_num_state_parts(state_root_node.memory_usage);
        if part_id >= num_parts {
            return Err(shard_id_out_of_bounds(shard_id));
        }
        let current_time = Instant::now();
        let state_part = self
            .runtime_adapter
            .obtain_state_part(
                shard_id,
                &prev_prev_hash,
                &state_root,
                PartId::new(part_id, num_parts),
            )
            .log_storage_error("obtain_state_part fail")?;

        let elapsed_ms = (self.clock.now().signed_duration_since(current_time))
            .whole_milliseconds()
            .max(0) as u128;
        self.requested_state_parts
            .save_state_part_elapsed(&sync_hash, &shard_id, &part_id, elapsed_ms);

        // Saving the part data
        let mut store_update = self.chain_store.store().store_update();
        store_update.set(DBCol::StateParts, &key, &state_part);
        store_update.commit()?;

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

        let chunk = shard_state_header.cloned_chunk();
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
        if !verify_path(
            *sync_prev_block_header.chunk_headers_root(),
            shard_state_header.chunk_proof(),
            &ChunkHashHeight(chunk.chunk_hash(), chunk.height_included()),
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
                    &ChunkHashHeight(prev_chunk_header.chunk_hash(), prev_chunk_header.height_included()),
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
                // 4f. Proving the outgoing_receipts_root matches that in the block
                if !verify_path(
                    *block_header.prev_chunk_outgoing_receipts_root(),
                    block_proof,
                    root,
                ) {
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
        if !self.runtime_adapter.validate_state_root_node(
            shard_state_header.state_root_node(),
            chunk_inner.prev_state_root(),
        ) {
            byzantine_assert!(false);
            return Err(Error::Other("set_shard_state failed: state_root_node is invalid".into()));
        }

        // Saving the header data.
        let mut store_update = self.chain_store.store().store_update();
        let key = borsh::to_vec(&StateHeaderKey(shard_id, sync_hash))?;
        store_update.set_ser(DBCol::StateHeaders, &key, &shard_state_header)?;
        store_update.commit()?;

        Ok(())
    }

    pub fn set_state_part(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: PartId,
        data: &[u8],
    ) -> Result<(), Error> {
        let shard_state_header = self.get_state_header(shard_id, sync_hash)?;
        let chunk = shard_state_header.take_chunk();
        let state_root = *chunk.take_header().take_inner().prev_state_root();
        if !self.runtime_adapter.validate_state_part(&state_root, part_id, data) {
            byzantine_assert!(false);
            return Err(Error::Other(format!(
                "set_state_part failed: validate_state_part failed. state_root={:?}",
                state_root
            )));
        }

        // Saving the part data.
        let mut store_update = self.chain_store.store().store_update();
        let key = borsh::to_vec(&StatePartKey(sync_hash, shard_id, part_id.idx))?;
        store_update.set(DBCol::StateParts, &key, data);
        store_update.commit()?;
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
