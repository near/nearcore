extern crate log;

use std::collections::hash_map::Entry;
use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use actix::Recipient;
use cached::Cached;
use cached::SizedCache;
use log::{debug, error};
use rand::Rng;

use near_chain::{byzantine_assert, collect_receipts, ErrorKind, RuntimeAdapter, ValidTransaction};
use near_crypto::BlsSigner;
use near_network::types::{ChunkOnePartRequestMsg, ChunkPartMsg, ChunkPartRequestMsg, PeerId};
use near_network::NetworkRequests;
use near_pool::TransactionPool;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, verify_path, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{
    ChunkHash, ChunkOnePart, EncodedShardChunk, ReceiptProof, ShardChunk, ShardChunkHeader,
    ShardChunkHeaderInner, ShardProof,
};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{
    AccountId, Balance, BlockIndex, EpochId, Gas, ShardId, ValidatorStake,
};
use near_store::{Store, COL_CHUNKS, COL_CHUNK_ONE_PARTS};

pub use crate::types::Error;

mod types;

const MAX_CHUNK_REQUESTS_TO_KEEP_PER_SHARD: usize = 128;
const ORPHANED_ONE_PART_CACHE_SIZE: usize = 1024;
const REQUEST_ONE_PARTS_CACHE_SIZE: usize = 1024;
const REQUEST_CHUNKS_CACHE_SIZE: usize = 1024;

/// Adapter to break dependency of sub-components on the network requests.
/// For tests use MockNetworkAdapter that accumulates the requests to network.
pub trait NetworkAdapter: Sync + Send {
    fn send(&self, msg: NetworkRequests);
}

pub struct NetworkRecipient {
    network_recipient: Recipient<NetworkRequests>,
}

unsafe impl Sync for NetworkRecipient {}

impl NetworkRecipient {
    pub fn new(network_recipient: Recipient<NetworkRequests>) -> Self {
        Self { network_recipient }
    }
}

impl NetworkAdapter for NetworkRecipient {
    fn send(&self, msg: NetworkRequests) {
        let _ = self.network_recipient.do_send(msg);
    }
}

#[derive(PartialEq, Eq)]
pub enum ChunkStatus {
    Complete(Vec<MerklePath>),
    Incomplete,
    Invalid,
}

pub struct ShardsManager {
    me: Option<AccountId>,

    tx_pools: HashMap<ShardId, TransactionPool>,

    runtime_adapter: Arc<dyn RuntimeAdapter>,
    network_adapter: Arc<dyn NetworkAdapter>,
    store: Arc<Store>,

    encoded_chunks: HashMap<ChunkHash, EncodedShardChunk>,
    merkle_paths: HashMap<(ChunkHash, u64), MerklePath>,
    block_hash_to_chunk_headers: HashMap<CryptoHash, Vec<(ShardId, ShardChunkHeader)>>,

    orphaned_one_parts: SizedCache<(CryptoHash, ShardId, u64), ChunkOnePart>,

    requested_one_parts: SizedCache<ChunkHash, ()>,
    requested_chunks: SizedCache<ChunkHash, ()>,

    requests_fifo: VecDeque<(ShardId, ChunkHash, PeerId, u64)>,
    requests: HashMap<(ShardId, ChunkHash, u64), HashSet<(PeerId)>>,
}

impl ShardsManager {
    pub fn new(
        me: Option<AccountId>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Arc<dyn NetworkAdapter>,
        store: Arc<Store>,
    ) -> Self {
        Self {
            me,
            tx_pools: HashMap::new(),
            runtime_adapter,
            network_adapter,
            store,
            encoded_chunks: HashMap::new(),
            merkle_paths: HashMap::new(),
            block_hash_to_chunk_headers: HashMap::new(),
            orphaned_one_parts: SizedCache::with_size(ORPHANED_ONE_PART_CACHE_SIZE),
            requested_one_parts: SizedCache::with_size(REQUEST_ONE_PARTS_CACHE_SIZE),
            requested_chunks: SizedCache::with_size(REQUEST_CHUNKS_CACHE_SIZE),
            requests_fifo: VecDeque::new(),
            requests: HashMap::new(),
        }
    }

    pub fn prepare_transactions(
        &mut self,
        shard_id: ShardId,
        expected_weight: u32,
    ) -> Result<Vec<SignedTransaction>, Error> {
        if let Some(tx_pool) = self.tx_pools.get_mut(&shard_id) {
            tx_pool.prepare_transactions(expected_weight).map_err(|err| err.into())
        } else {
            Ok(vec![])
        }
    }

    pub fn cares_about_shard_this_or_next_epoch(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool {
        self.runtime_adapter.cares_about_shard(account_id.clone(), parent_hash, shard_id, is_me)
            || self.runtime_adapter.will_care_about_shard(account_id, parent_hash, shard_id, is_me)
    }

    fn request_one_part(
        &self,
        epoch_id: &EpochId,
        height: BlockIndex,
        shard_id: ShardId,
        part_id: u64,
        chunk_hash: ChunkHash,
        tracking_shards: HashSet<ShardId>,
    ) -> Result<(), Error> {
        self.network_adapter.send(NetworkRequests::ChunkOnePartRequest {
            account_id: self.runtime_adapter.get_chunk_producer(epoch_id, height, shard_id)?,
            one_part_request: ChunkOnePartRequestMsg {
                shard_id,
                chunk_hash,
                height,
                part_id,
                tracking_shards,
            },
        });
        Ok(())
    }

    pub fn request_chunks(
        &mut self,
        chunks_to_request: Vec<ShardChunkHeader>,
    ) -> Result<(), Error> {
        for chunk_header in chunks_to_request {
            let ShardChunkHeader {
                inner:
                    ShardChunkHeaderInner {
                        shard_id,
                        prev_block_hash: parent_hash,
                        height_created: height,
                        ..
                    },
                ..
            } = chunk_header;
            let chunk_hash = chunk_header.chunk_hash();
            let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(&parent_hash)?;
            let tracking_shards = (0..self.runtime_adapter.num_shards())
                .filter(|chunk_shard_id| {
                    self.cares_about_shard_this_or_next_epoch(
                        self.me.as_ref(),
                        &parent_hash,
                        *chunk_shard_id,
                        true,
                    )
                })
                .collect::<HashSet<_>>();

            if !self.encoded_chunks.contains_key(&chunk_hash) {
                if self.requested_one_parts.cache_get(&chunk_hash).is_some() {
                    continue;
                }
                self.requested_one_parts.cache_set(chunk_hash.clone(), ());

                let mut requested_one_part = false;
                if let Some(me) = self.me.as_ref() {
                    for part_id in 0..self.runtime_adapter.num_total_parts(&parent_hash) {
                        let part_id = part_id as u64;
                        if &self.runtime_adapter.get_part_owner(&parent_hash, part_id)? == me {
                            requested_one_part = true;
                            self.request_one_part(
                                &epoch_id,
                                height,
                                shard_id,
                                part_id,
                                chunk_hash.clone(),
                                tracking_shards.clone(),
                            )?;
                        }
                    }

                    if self.runtime_adapter.cares_about_shard(
                        self.me.as_ref(),
                        &parent_hash,
                        shard_id,
                        true,
                    ) {
                        assert!(requested_one_part)
                    };
                }

                if !requested_one_part {
                    let mut rng = rand::thread_rng();

                    let part_id = rng.gen::<u64>()
                        % (self.runtime_adapter.num_total_parts(&parent_hash) as u64);
                    self.request_one_part(
                        &epoch_id,
                        height,
                        shard_id,
                        part_id,
                        chunk_hash.clone(),
                        tracking_shards.clone(),
                    )?;
                }
            } else {
                if self.requested_chunks.cache_get(&chunk_hash).is_some() {
                    continue;
                }
                self.requested_chunks.cache_set(chunk_hash.clone(), ());

                for part_id in 0..self.runtime_adapter.num_total_parts(&parent_hash) {
                    let part_id = part_id as u64;
                    let to_whom = self.runtime_adapter.get_part_owner(&parent_hash, part_id)?;
                    if Some(&to_whom) != self.me.as_ref() {
                        self.network_adapter.send(NetworkRequests::ChunkPartRequest {
                            account_id: to_whom,
                            part_request: ChunkPartRequestMsg {
                                shard_id,
                                chunk_hash: chunk_hash.clone(),
                                height,
                                part_id,
                            },
                        });
                    }
                }
            }
        }
        Ok(())
    }

    pub fn prepare_chunks(
        &mut self,
        prev_block_hash: CryptoHash,
    ) -> Vec<(ShardId, ShardChunkHeader)> {
        self.block_hash_to_chunk_headers.remove(&prev_block_hash).unwrap_or_else(|| vec![])
    }

    pub fn insert_transaction(&mut self, shard_id: ShardId, tx: ValidTransaction) {
        self.tx_pools
            .entry(shard_id)
            .or_insert_with(TransactionPool::default)
            .insert_transaction(tx);
    }

    pub fn remove_transactions(
        &mut self,
        shard_id: ShardId,
        transactions: &Vec<SignedTransaction>,
    ) {
        if let Some(pool) = self.tx_pools.get_mut(&shard_id) {
            pool.remove_transactions(transactions)
        }
    }

    pub fn reintroduce_transactions(
        &mut self,
        shard_id: ShardId,
        transactions: &Vec<SignedTransaction>,
    ) {
        self.tx_pools
            .entry(shard_id)
            .or_insert_with(TransactionPool::default)
            .reintroduce_transactions(transactions);
    }

    pub fn process_chunk_part_request(
        &mut self,
        request: ChunkPartRequestMsg,
        peer_id: PeerId,
    ) -> Result<(), Error> {
        let mut served = false;
        if let Some(chunk) = self.encoded_chunks.get(&request.chunk_hash) {
            if request.part_id as usize >= chunk.content.parts.len() {
                return Err(Error::InvalidChunkPartId);
            }

            if chunk.content.parts[request.part_id as usize].is_some() {
                served = true;
                self.network_adapter.send(NetworkRequests::ChunkPart {
                    peer_id: peer_id.clone(),
                    part: chunk.create_chunk_part_msg(
                        request.part_id,
                        // Part should never exist in the chunk content if the merkle path for it is
                        //    not in merkle_paths, so `unwrap` here
                        self.merkle_paths
                            .get(&(request.chunk_hash.clone(), request.part_id))
                            .unwrap()
                            .clone(),
                    ),
                });
            }
        }
        if !served
        // TODO: uncomment and fix the get_part_owner
        /*&& Some(self.runtime_adapter.get_part_owner(request.height, request.part_id)?)
        == self.me*/
        {
            while self.requests_fifo.len() + 1 > MAX_CHUNK_REQUESTS_TO_KEEP_PER_SHARD {
                let (r_shard_id, r_hash, r_peer, r_part_id) =
                    self.requests_fifo.pop_front().unwrap();
                self.requests.entry((r_shard_id, r_hash.clone(), r_part_id)).and_modify(|v| {
                    let _ = v.remove(&r_peer);
                });
                if self
                    .requests
                    .get(&(r_shard_id, r_hash.clone(), r_part_id))
                    .map_or_else(|| false, |x| x.is_empty())
                {
                    self.requests.remove(&(r_shard_id, r_hash, r_part_id));
                }
            }
            if self
                .requests
                .entry((request.shard_id, request.chunk_hash.clone(), request.part_id))
                .or_insert_with(HashSet::default)
                .insert(peer_id.clone())
            {
                self.requests_fifo.push_back((
                    request.shard_id,
                    request.chunk_hash,
                    peer_id,
                    request.part_id,
                ));
            }
        }

        Ok(())
    }

    fn receipts_recipient_filter(
        &self,
        from_shard_id: ShardId,
        tracking_shards: &HashSet<ShardId>,
        receipts: &Vec<Receipt>,
        proofs: &Vec<MerklePath>,
    ) -> Vec<ReceiptProof> {
        let mut one_part_receipt_proofs = vec![];
        for to_shard_id in 0..self.runtime_adapter.num_shards() {
            if tracking_shards.contains(&to_shard_id) {
                one_part_receipt_proofs.push(ReceiptProof(
                    receipts
                        .iter()
                        .filter(|&receipt| {
                            self.runtime_adapter.account_id_to_shard_id(&receipt.receiver_id)
                                == to_shard_id
                        })
                        .cloned()
                        .collect(),
                    ShardProof(from_shard_id, proofs[to_shard_id as usize].clone()),
                ))
            }
        }
        one_part_receipt_proofs
    }

    pub fn process_chunk_one_part_request(
        &mut self,
        request: ChunkOnePartRequestMsg,
        peer_id: PeerId,
    ) -> Result<(), Error> {
        if let Some(encoded_chunk) = self.encoded_chunks.get(&request.chunk_hash) {
            if request.part_id as usize >= encoded_chunk.content.parts.len() {
                return Err(Error::InvalidChunkPartId);
            }

            // TODO: should have a ref to ChainStore instead, and use the cache
            if let Some(chunk) =
                self.store.get_ser::<ShardChunk>(COL_CHUNKS, request.chunk_hash.as_ref())?
            {
                let receipts_hashes =
                    self.runtime_adapter.build_receipts_hashes(&chunk.receipts)?;
                let (receipts_root, receipts_proofs) = merklize(&receipts_hashes);
                let one_part_receipt_proofs = self.receipts_recipient_filter(
                    request.shard_id,
                    &request.tracking_shards,
                    &chunk.receipts,
                    &receipts_proofs,
                );

                assert_eq!(chunk.header.inner.outgoing_receipts_root, receipts_root);
                self.network_adapter.send(NetworkRequests::ChunkOnePartResponse {
                    peer_id,
                    header_and_part: encoded_chunk.create_chunk_one_part(
                        request.part_id,
                        one_part_receipt_proofs,
                        // It should be impossible to have a part but not the merkle path
                        self.merkle_paths
                            .get(&(request.chunk_hash.clone(), request.part_id))
                            .unwrap()
                            .clone(),
                    ),
                });
            }
        }

        Ok(())
    }

    pub fn check_chunk_complete(
        data_parts: usize,
        total_parts: usize,
        chunk: &mut EncodedShardChunk,
    ) -> ChunkStatus {
        let parity_parts = total_parts - data_parts;
        if chunk.content.num_fetched_parts() >= data_parts {
            chunk.content.reconstruct(data_parts, parity_parts);

            let (merkle_root, paths) = chunk.content.get_merkle_hash_and_paths();
            if merkle_root == chunk.header.inner.encoded_merkle_root {
                ChunkStatus::Complete(paths)
            } else {
                ChunkStatus::Invalid
            }
        } else {
            ChunkStatus::Incomplete
        }
    }
    // Returns the hash of the enclosing block if a chunk part was not known previously, and the chunk is complete after receiving it
    // Once it receives the last part necessary to reconstruct, the chunk gets reconstructed and fills in all the remaining parts,
    //     thus once the remaining parts arrive, they do not trigger returning the hash again.
    pub fn process_chunk_part(&mut self, part: ChunkPartMsg) -> Result<Option<CryptoHash>, Error> {
        let cares_about_shard = if let Some(chunk) = self.encoded_chunks.get(&part.chunk_hash) {
            let prev_block_hash = chunk.header.inner.prev_block_hash;
            let shard_id = part.shard_id;
            self.cares_about_shard_this_or_next_epoch(
                self.me.as_ref(),
                &prev_block_hash,
                shard_id,
                true,
            )
        } else {
            false
        };

        if let Some(chunk) = self.encoded_chunks.get_mut(&part.chunk_hash) {
            let prev_block_hash = chunk.header.inner.prev_block_hash;
            if chunk.header.inner.shard_id != part.shard_id {
                return Err(Error::InvalidChunkShardId);
            }
            if (part.part_id as usize) < chunk.content.parts.len() {
                if chunk.content.parts[part.part_id as usize].is_none() {
                    // We have the chunk but haven't seen the part, so actually need to process it
                    // First validate the merkle proof
                    if !verify_path(
                        chunk.header.inner.encoded_merkle_root,
                        &part.merkle_path,
                        &part.part,
                    ) {
                        return Err(Error::InvalidMerkleProof);
                    }

                    chunk.content.parts[part.part_id as usize] = Some(part.part);
                    self.merkle_paths
                        .insert((part.chunk_hash.clone(), part.part_id), part.merkle_path);

                    match ShardsManager::check_chunk_complete(
                        self.runtime_adapter.num_data_parts(&prev_block_hash),
                        self.runtime_adapter.num_total_parts(&prev_block_hash),
                        chunk,
                    ) {
                        ChunkStatus::Complete(merkle_paths) => {
                            let mut store_update = self.store.store_update();
                            if let Ok(shard_chunk) = chunk
                                .decode_chunk(self.runtime_adapter.num_data_parts(&prev_block_hash))
                            {
                                debug!(target: "chunks", "Reconstructed and decoded chunk {}, encoded length was {}, num txs: {}, I'm {:?}", chunk.header.chunk_hash().0, chunk.header.inner.encoded_length, shard_chunk.transactions.len(), self.me);
                                // Decoded a valid chunk, store it in the permanent store ...
                                store_update.set_ser(
                                    COL_CHUNKS,
                                    part.chunk_hash.as_ref(),
                                    &shard_chunk,
                                )?;
                                store_update.commit()?;
                                // ... and include into the block if we are the producer
                                if cares_about_shard {
                                    self.block_hash_to_chunk_headers
                                        .entry(chunk.header.inner.prev_block_hash)
                                        .or_insert_with(|| vec![])
                                        .push((part.shard_id, chunk.header.clone()));
                                }

                                for (part_id, merkle_path) in merkle_paths.iter().enumerate() {
                                    let part_id = part_id as u64;
                                    self.merkle_paths.insert(
                                        (part.chunk_hash.clone(), part_id),
                                        merkle_path.clone(),
                                    );
                                }

                                return Ok(Some(chunk.header.inner.prev_block_hash));
                            } else {
                                error!(target: "chunks", "Reconstructed but failed to decoded chunk {}", chunk.header.chunk_hash().0);
                                // Can't decode chunk, ignore it
                                for i in 0..self
                                    .runtime_adapter
                                    .num_total_parts(&chunk.header.inner.prev_block_hash)
                                {
                                    self.merkle_paths.remove(&(part.chunk_hash.clone(), i as u64));
                                }
                                self.encoded_chunks.remove(&part.chunk_hash);
                                return Ok(None);
                            }
                        }
                        ChunkStatus::Incomplete => return Ok(None),
                        ChunkStatus::Invalid => {
                            for i in 0..self
                                .runtime_adapter
                                .num_total_parts(&chunk.header.inner.prev_block_hash)
                            {
                                self.merkle_paths.remove(&(part.chunk_hash.clone(), i as u64));
                            }
                            self.encoded_chunks.remove(&part.chunk_hash);
                            return Ok(None);
                        }
                    };
                }
            }
        } else {
            debug!(target: "shards", "Received part {} for unknown chunk {:?}, declining", part.part_id, part.chunk_hash);
        }
        Ok(None)
    }

    /// Returns true if the chunk_one_part was not previously known
    pub fn process_chunk_one_part(&mut self, one_part: ChunkOnePart) -> Result<bool, Error> {
        let chunk_hash = one_part.chunk_hash.clone();
        let prev_block_hash = one_part.header.inner.prev_block_hash;

        match self.runtime_adapter.get_epoch_id_from_prev_block(&prev_block_hash) {
            Ok(_) => {}
            Err(err) => {
                self.orphaned_one_parts
                    .cache_set((prev_block_hash, one_part.shard_id, one_part.part_id), one_part);
                return Err(err.into());
            }
        }

        if !self.runtime_adapter.verify_chunk_header_signature(&one_part.header)? {
            byzantine_assert!(false);
            return Err(Error::InvalidChunkSignature);
        }

        if one_part.shard_id != one_part.header.inner.shard_id {
            byzantine_assert!(false);
            return Err(Error::InvalidChunkShardId);
        }

        if !verify_path(
            one_part.header.inner.encoded_merkle_root,
            &one_part.merkle_path,
            &one_part.part,
        ) {
            // TODO: this is a slashable behavior
            byzantine_assert!(false);
            return Err(Error::InvalidMerkleProof);
        }

        // Checking one_part's receipts validity here
        let receipts = collect_receipts(&one_part.receipt_proofs);
        let receipts_hashes = self.runtime_adapter.build_receipts_hashes(&receipts)?;
        let mut proof_index = 0;
        for shard_id in 0..self.runtime_adapter.num_shards() {
            if self.cares_about_shard_this_or_next_epoch(
                self.me.as_ref(),
                &prev_block_hash,
                shard_id,
                true,
            ) {
                if proof_index == one_part.receipt_proofs.len()
                    || !verify_path(
                        one_part.header.inner.outgoing_receipts_root,
                        &(one_part.receipt_proofs[proof_index].1).1,
                        &receipts_hashes[shard_id as usize],
                    )
                {
                    byzantine_assert!(false);
                    return Err(Error::ChainError(ErrorKind::InvalidReceiptsProof.into()));
                }
                proof_index += 1;
            }
        }
        if proof_index != one_part.receipt_proofs.len() {
            byzantine_assert!(false);
            return Err(Error::ChainError(ErrorKind::InvalidReceiptsProof.into()));
        }

        if let Some(send_to) =
            self.requests.remove(&(one_part.shard_id, chunk_hash.clone(), one_part.part_id))
        {
            for whom in send_to {
                self.network_adapter.send(NetworkRequests::ChunkPart {
                    peer_id: whom,
                    part: ChunkPartMsg {
                        shard_id: one_part.shard_id,
                        chunk_hash: chunk_hash.clone(),
                        part_id: one_part.part_id,
                        part: one_part.part.clone(),
                        merkle_path: one_part.merkle_path.clone(),
                    },
                });
            }
        }

        let (ret, store_receipts) = match self.encoded_chunks.entry(one_part.chunk_hash.clone()) {
            Entry::Occupied(entry) => {
                (entry.get().content.parts[one_part.part_id as usize].is_none(), false)
            }
            Entry::Vacant(entry) => {
                entry.insert(EncodedShardChunk::from_header(
                    one_part.header.clone(),
                    self.runtime_adapter.num_total_parts(&prev_block_hash),
                ));
                (true, true)
            }
        };
        self.merkle_paths
            .insert((one_part.chunk_hash.clone(), one_part.part_id), one_part.merkle_path.clone());

        let mut store_update = self.store.store_update();
        if store_receipts {
            store_update.set_ser(COL_CHUNK_ONE_PARTS, chunk_hash.as_ref(), &one_part)?;
        }

        // If we do not follow this shard, having the one part is sufficient to include the chunk in the block
        if !self.cares_about_shard_this_or_next_epoch(
            self.me.as_ref(),
            &prev_block_hash,
            one_part.shard_id,
            true,
        ) {
            self.block_hash_to_chunk_headers
                .entry(one_part.header.inner.prev_block_hash)
                .or_insert_with(|| vec![])
                .push((one_part.shard_id, one_part.header.clone()));
        }
        store_update.commit()?;

        if let Some(encoded_chunk) = self.encoded_chunks.get_mut(&one_part.chunk_hash) {
            encoded_chunk.content.parts[one_part.part_id as usize] = Some(one_part.part);
        }

        Ok(ret)
    }

    pub fn process_orphaned_one_parts(&mut self, block_hash: CryptoHash) -> bool {
        let mut ret = false;
        for shard_id in 0..self.runtime_adapter.num_shards() {
            let shard_id = shard_id as ShardId;
            for part_id in 0..self.runtime_adapter.num_total_parts(&block_hash) {
                let part_id = part_id as u64;
                if let Some(a) =
                    self.orphaned_one_parts.cache_remove(&(block_hash, shard_id, part_id))
                {
                    let shard_id = a.shard_id;
                    let chunk_header = a.header.clone();
                    if let Ok(cur) = self.process_chunk_one_part(a) {
                        ret |= cur;

                        if self.cares_about_shard_this_or_next_epoch(
                            self.me.as_ref(),
                            &block_hash,
                            shard_id,
                            true,
                        ) {
                            self.request_chunks(vec![chunk_header]).unwrap();
                        }
                    } else {
                        byzantine_assert!(false); /* ignore error */
                    }
                }
            }
        }
        ret
    }

    pub fn create_encoded_shard_chunk(
        &mut self,
        prev_block_hash: CryptoHash,
        prev_state_root: CryptoHash,
        height: u64,
        shard_id: ShardId,
        gas_used: Gas,
        gas_limit: Gas,
        rent_paid: Balance,
        validator_proposals: Vec<ValidatorStake>,
        transactions: &Vec<SignedTransaction>,
        receipts: &Vec<Receipt>,
        receipts_root: CryptoHash,
        tx_root: CryptoHash,
        signer: Arc<dyn BlsSigner>,
    ) -> Result<EncodedShardChunk, Error> {
        let total_parts = self.runtime_adapter.num_total_parts(&prev_block_hash);
        let data_parts = self.runtime_adapter.num_data_parts(&prev_block_hash);
        let (new_chunk, merkle_paths) = EncodedShardChunk::new(
            prev_block_hash,
            prev_state_root,
            height,
            shard_id,
            total_parts,
            data_parts,
            gas_used,
            gas_limit,
            rent_paid,
            tx_root,
            validator_proposals,
            transactions,
            receipts,
            receipts_root,
            signer,
        )?;

        for (part_id, merkle_path) in merkle_paths.iter().enumerate() {
            let part_id = part_id as u64;
            self.merkle_paths.insert((new_chunk.chunk_hash(), part_id), merkle_path.clone());
        }

        Ok(new_chunk)
    }

    pub fn distribute_encoded_chunk(
        &mut self,
        encoded_chunk: EncodedShardChunk,
        receipts: Vec<Receipt>,
    ) {
        // TODO: if the number of validators exceeds the number of parts, this logic must be changed
        let prev_block_hash = encoded_chunk.header.inner.prev_block_hash;
        let mut processed_one_part = false;
        let chunk_hash = encoded_chunk.chunk_hash();
        let shard_id = encoded_chunk.header.inner.shard_id;
        let receipts_hashes = self.runtime_adapter.build_receipts_hashes(&receipts).unwrap();
        let (receipts_root, receipts_proofs) = merklize(&receipts_hashes);
        assert_eq!(encoded_chunk.header.inner.outgoing_receipts_root, receipts_root);
        for part_ord in 0..self.runtime_adapter.num_total_parts(&prev_block_hash) {
            let part_ord = part_ord as u64;
            let to_whom = self.runtime_adapter.get_part_owner(&prev_block_hash, part_ord).unwrap();
            let tracking_shards = (0..self.runtime_adapter.num_shards())
                .filter(|chunk_shard_id| {
                    self.cares_about_shard_this_or_next_epoch(
                        Some(&to_whom),
                        &prev_block_hash,
                        *chunk_shard_id,
                        false,
                    )
                })
                .collect();

            let one_part_receipt_proofs = self.receipts_recipient_filter(
                shard_id,
                &tracking_shards,
                &receipts,
                &receipts_proofs,
            );
            let one_part = encoded_chunk.create_chunk_one_part(
                part_ord,
                one_part_receipt_proofs,
                // It should be impossible to have a part but not the merkle path
                self.merkle_paths.get(&(chunk_hash.clone(), part_ord)).unwrap().clone(),
            );

            // 1/2 This is a weird way to introduce the chunk to the producer's storage
            if Some(&to_whom) == self.me.as_ref() {
                if !processed_one_part {
                    processed_one_part = true;
                    self.process_chunk_one_part(one_part.clone()).unwrap();
                }
            } else {
                self.network_adapter.send(NetworkRequests::ChunkOnePartMessage {
                    account_id: to_whom.clone(),
                    header_and_part: one_part,
                });
            }
        }

        // 2/2 This is a weird way to introduce the chunk to the producer's storage
        for (part_id, _) in encoded_chunk.content.parts.iter().enumerate() {
            let part_id = part_id as u64;
            self.process_chunk_part(encoded_chunk.create_chunk_part_msg(
                part_id,
                // It should be impossible to have a part but not the merkle path
                self.merkle_paths.get(&(chunk_hash.clone(), part_id)).unwrap().clone(),
            ))
            .unwrap();
        }
    }
}
