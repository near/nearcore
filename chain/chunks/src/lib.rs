extern crate log;

use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::Recipient;
use log::{debug, error};
use rand::Rng;

use near_chain::validate::validate_chunk_proofs;
use near_chain::{
    byzantine_assert, collect_receipts, ChainStore, ChainStoreAccess, ErrorKind, RuntimeAdapter,
    ValidTransaction,
};
use near_crypto::Signer;
use near_network::types::{ChunkOnePartRequestMsg, ChunkPartMsg, ChunkPartRequestMsg, PeerId};
use near_network::NetworkRequests;
use near_pool::TransactionPool;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, verify_path, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{
    ChunkHash, ChunkOnePart, EncodedShardChunk, ReceiptProof, ShardChunkHeader,
    ShardChunkHeaderInner, ShardProof,
};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{
    AccountId, Balance, BlockIndex, EpochId, Gas, ShardId, StateRoot, ValidatorStake,
};

pub use crate::types::Error;

mod types;

const MAX_CHUNK_REQUESTS_TO_KEEP_PER_SHARD: usize = 128;
const CHUNK_REQUEST_RETRY_MS: u64 = 100;
const CHUNK_REQUEST_RETRY_MAX_MS: u64 = 100_000;

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

pub enum ProcessChunkOnePartResult {
    Known,
    HaveAllOneParts,
    NeedMoreOneParts,
}

#[derive(Clone)]
struct ChunkRequestInfo {
    height: BlockIndex,
    parent_hash: CryptoHash,
    shard_id: ShardId,
    added: Instant,
    last_requested: Instant,
}

struct RequestPool {
    retry_duration: Duration,
    max_duration: Duration,
    requests: HashMap<ChunkHash, ChunkRequestInfo>,
}

impl RequestPool {
    pub fn new(retry_duration: Duration, max_duration: Duration) -> Self {
        Self { retry_duration, max_duration, requests: HashMap::default() }
    }
    pub fn contains_key(&self, chunk_hash: &ChunkHash) -> bool {
        self.requests.contains_key(chunk_hash)
    }

    pub fn insert(&mut self, chunk_hash: ChunkHash, chunk_request: ChunkRequestInfo) {
        self.requests.insert(chunk_hash, chunk_request);
    }

    pub fn remove(&mut self, chunk_hash: &ChunkHash) {
        let _ = self.requests.remove(chunk_hash);
    }

    pub fn fetch(&mut self) -> Vec<(ChunkHash, ChunkRequestInfo)> {
        let mut removed_requests = HashSet::<ChunkHash>::default();
        let mut requests = Vec::new();
        for (chunk_hash, mut chunk_request) in self.requests.iter_mut() {
            if chunk_request.added.elapsed() > self.max_duration {
                debug!(target: "chunks", "Evicted chunk requested that was never fetched {} (shard_id: {})", chunk_hash.0, chunk_request.shard_id);
                removed_requests.insert(chunk_hash.clone());
                continue;
            }
            if chunk_request.last_requested.elapsed() > self.retry_duration {
                chunk_request.last_requested = Instant::now();
                requests.push((chunk_hash.clone(), chunk_request.clone()));
            }
        }
        for chunk_hash in removed_requests {
            self.requests.remove(&chunk_hash);
        }
        requests
    }
}

pub struct ShardsManager {
    me: Option<AccountId>,

    tx_pools: HashMap<ShardId, TransactionPool>,

    runtime_adapter: Arc<dyn RuntimeAdapter>,
    network_adapter: Arc<dyn NetworkAdapter>,

    encoded_chunks: HashMap<ChunkHash, EncodedShardChunk>,
    merkle_paths: HashMap<(ChunkHash, u64), MerklePath>,
    block_hash_to_chunk_headers: HashMap<CryptoHash, Vec<(ShardId, ShardChunkHeader)>>,

    requested_one_parts: RequestPool,
    requested_chunks: RequestPool,

    requests_fifo: VecDeque<(ShardId, ChunkHash, PeerId, u64)>,
    requests: HashMap<(ShardId, ChunkHash, u64), HashSet<(PeerId)>>,
}

impl ShardsManager {
    pub fn new(
        me: Option<AccountId>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Arc<dyn NetworkAdapter>,
    ) -> Self {
        Self {
            me,
            tx_pools: HashMap::new(),
            runtime_adapter,
            network_adapter,
            encoded_chunks: HashMap::new(),
            merkle_paths: HashMap::new(),
            block_hash_to_chunk_headers: HashMap::new(),
            requested_one_parts: RequestPool::new(
                Duration::from_millis(CHUNK_REQUEST_RETRY_MS),
                Duration::from_millis(CHUNK_REQUEST_RETRY_MAX_MS),
            ),
            requested_chunks: RequestPool::new(
                Duration::from_millis(CHUNK_REQUEST_RETRY_MS),
                Duration::from_millis(CHUNK_REQUEST_RETRY_MAX_MS),
            ),
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

    fn request_chunk_one_parts(
        &mut self,
        height: BlockIndex,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        chunk_hash: &ChunkHash,
    ) -> Result<(), Error> {
        let tracking_shards = self.get_tracking_shards(&parent_hash);
        let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(&parent_hash)?;
        let mut requested_one_part = false;
        if let Some(me) = self.me.as_ref() {
            let encoded_chunk = self.encoded_chunks.get(chunk_hash);
            for part_id in 0..self.runtime_adapter.num_total_parts(&parent_hash) {
                if let Some(encoded_chunk) = encoded_chunk {
                    if encoded_chunk.content.parts[part_id].is_some() {
                        continue;
                    }
                }
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
                false,
            ) {
                assert!(requested_one_part)
            };
        }

        if !requested_one_part {
            let mut rng = rand::thread_rng();

            let part_id =
                rng.gen::<u64>() % (self.runtime_adapter.num_total_parts(&parent_hash) as u64);
            self.request_one_part(
                &epoch_id,
                height,
                shard_id,
                part_id,
                chunk_hash.clone(),
                tracking_shards.clone(),
            )?;
        }
        Ok(())
    }

    fn request_chunk_parts(
        &mut self,
        height: BlockIndex,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        chunk_hash: &ChunkHash,
    ) -> Result<(), Error> {
        let encoded_chunk = self
            .encoded_chunks
            .get(chunk_hash)
            .expect("request_chunk_parts only should be called if encoded_chunk is present");
        for part_id in 0..self.runtime_adapter.num_total_parts(&parent_hash) {
            // If we already have the part, don't request it again.
            if encoded_chunk.content.parts[part_id].is_some() {
                continue;
            }
            let part_id = part_id as u64;
            let to_whom = self.runtime_adapter.get_part_owner(&parent_hash, part_id)?;
            let to_whom = if Some(&to_whom) == self.me.as_ref() {
                // If missing own part, request it from the chunk producer
                let ret = self.runtime_adapter.get_chunk_producer(
                    &self.runtime_adapter.get_epoch_id_from_prev_block(parent_hash)?,
                    height,
                    shard_id,
                )?;
                ret
            } else {
                to_whom
            };
            assert_ne!(Some(&to_whom), self.me.as_ref());
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
        Ok(())
    }

    fn get_tracking_shards(&self, parent_hash: &CryptoHash) -> HashSet<ShardId> {
        (0..self.runtime_adapter.num_shards())
            .filter(|chunk_shard_id| {
                self.cares_about_shard_this_or_next_epoch(
                    self.me.as_ref(),
                    &parent_hash,
                    *chunk_shard_id,
                    true,
                )
            })
            .collect::<HashSet<_>>()
    }

    pub fn request_chunks(
        &mut self,
        chunks_to_request: Vec<ShardChunkHeader>,
        delay_requests: bool,
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

            let need_to_request_one_parts = match self.encoded_chunks.get(&chunk_hash) {
                Some(encoded_chunk) => !self.has_all_one_parts(&parent_hash, encoded_chunk)?,
                None => true,
            };

            // TODO: remove !delay_requests, should be done as part of #1434
            let need_to_request_parts = !delay_requests
                && self.encoded_chunks.contains_key(&chunk_hash)
                && self.cares_about_shard_this_or_next_epoch(
                    self.me.as_ref(),
                    &parent_hash,
                    shard_id,
                    true,
                );

            if need_to_request_one_parts {
                if self.requested_one_parts.contains_key(&chunk_hash) {
                    continue;
                }
                self.requested_one_parts.insert(
                    chunk_hash.clone(),
                    ChunkRequestInfo {
                        height,
                        parent_hash,
                        shard_id,
                        last_requested: Instant::now(),
                        added: Instant::now(),
                    },
                );
                if !delay_requests {
                    self.request_chunk_one_parts(height, &parent_hash, shard_id, &chunk_hash)?;
                }
            }

            if need_to_request_parts {
                assert!(!delay_requests);
                if self.requested_chunks.contains_key(&chunk_hash) {
                    continue;
                }
                self.requested_chunks.insert(
                    chunk_hash.clone(),
                    ChunkRequestInfo {
                        height,
                        parent_hash,
                        shard_id,
                        last_requested: Instant::now(),
                        added: Instant::now(),
                    },
                );

                self.request_chunk_parts(height, &parent_hash, shard_id, &chunk_hash)?;
            }
        }
        Ok(())
    }

    /// Resends chunk requests if haven't received it within expected time.
    pub fn resend_chunk_requests(&mut self) -> Result<(), Error> {
        // Process chunk one part requests.
        let requests = self.requested_one_parts.fetch();
        for (chunk_hash, chunk_request) in requests {
            match self.request_chunk_one_parts(
                chunk_request.height,
                &chunk_request.parent_hash,
                chunk_request.shard_id,
                &chunk_hash,
            ) {
                Ok(()) => {}
                Err(err) => {
                    error!(target: "client", "Error during requesting chunk one part: {}", err);
                }
            }
        }

        // Process chunk part requests.
        let requests = self.requested_chunks.fetch();
        for (chunk_hash, chunk_request) in requests {
            match self.request_chunk_parts(
                chunk_request.height,
                &chunk_request.parent_hash,
                chunk_request.shard_id,
                &chunk_hash,
            ) {
                Ok(()) => {}
                Err(err) => {
                    error!(target: "client", "Error during requesting chunk parts: {}", err);
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
        if !served {
            debug!(
                "part request for {}:{}, I'm {:?}, served: {}",
                request.chunk_hash.0, request.part_id, self.me, served
            ); // RDX
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

    pub fn receipts_recipient_filter(
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
                    ShardProof {
                        from_shard_id,
                        to_shard_id,
                        proof: proofs[to_shard_id as usize].clone(),
                    },
                ))
            }
        }
        one_part_receipt_proofs
    }

    pub fn process_chunk_one_part_request(
        &mut self,
        request: ChunkOnePartRequestMsg,
        peer_id: PeerId,
        chain_store: &mut ChainStore,
    ) -> Result<(), Error> {
        debug!(target:"chunks", "Received one part request for {:?}, I'm {:?}", request.chunk_hash.0, self.me);
        if let Some(encoded_chunk) = self.encoded_chunks.get(&request.chunk_hash) {
            if request.part_id as usize >= encoded_chunk.content.parts.len() {
                return Err(Error::InvalidChunkPartId);
            }

            // TODO: should have a ref to ChainStore instead, and use the cache
            if let Ok(chunk) = chain_store.get_chunk(&request.chunk_hash) {
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
                debug!(target: "chunks",
                    "Responding to one part request for {:?}, I'm {:?}",
                    request.chunk_hash.0, self.me
                );
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

    /// Add a part to current encoded chunk stored in memory. It's present only if One Part was present and signed correctly.
    fn add_part_to_encoded_chunk(&mut self, part: ChunkPartMsg) -> Result<(), Error> {
        if let Some(chunk) = self.encoded_chunks.get_mut(&part.chunk_hash) {
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

                    Ok(())
                } else {
                    Err(Error::KnownPart)
                }
            } else {
                Err(Error::InvalidChunkPartId)
            }
        } else {
            // We haven't received one part yet, so it's an unknown chunk.
            Err(Error::UnknownChunk)
        }
    }

    pub fn persist_chunk_if_complete(
        &mut self,
        chunk_hash: &ChunkHash,
        prev_block_hash: &CryptoHash,
        chain_store: &mut ChainStore,
    ) -> Result<Option<CryptoHash>, Error> {
        let chunk = self.encoded_chunks.get_mut(chunk_hash).unwrap();
        match ShardsManager::check_chunk_complete(
            self.runtime_adapter.num_data_parts(&prev_block_hash),
            self.runtime_adapter.num_total_parts(&prev_block_hash),
            chunk,
        ) {
            ChunkStatus::Complete(merkle_paths) => {
                let chunk = self
                    .encoded_chunks
                    .get(&chunk_hash)
                    .map(std::clone::Clone::clone)
                    .expect("Present if add_part returns Ok");
                self.process_encoded_chunk(chunk, merkle_paths, chain_store)
            }
            ChunkStatus::Incomplete => Ok(None),
            ChunkStatus::Invalid => {
                let chunk =
                    self.encoded_chunks.get(&chunk_hash).expect("Present if add_part returns Ok");
                for i in
                    0..self.runtime_adapter.num_total_parts(&chunk.header.inner.prev_block_hash)
                {
                    self.merkle_paths.remove(&(chunk_hash.clone(), i as u64));
                }
                self.encoded_chunks.remove(&chunk_hash);
                Err(Error::InvalidChunk)
            }
        }
    }

    /// Returns the hash of the enclosing block if a chunk part was not known previously, and the chunk is complete after receiving it
    /// Once it receives the last part necessary to reconstruct, the chunk gets reconstructed and fills in all the remaining parts,
    ///     thus once the remaining parts arrive, they do not trigger returning the hash again.
    pub fn process_chunk_part(
        &mut self,
        part: ChunkPartMsg,
        chain_store: &mut ChainStore,
    ) -> Result<Option<CryptoHash>, Error> {
        let part_id = part.part_id;
        let chunk_hash = part.chunk_hash.clone();
        if !self.requested_chunks.contains_key(&chunk_hash) {
            // Received chunk that wasn't requested.
            return Ok(None);
        }
        match self.add_part_to_encoded_chunk(part) {
            Ok(()) => {
                let prev_block_hash = self
                    .encoded_chunks
                    .get_mut(&chunk_hash)
                    .expect("Successfully added part")
                    .header
                    .inner
                    .prev_block_hash;
                self.persist_chunk_if_complete(&chunk_hash, &prev_block_hash, chain_store)
            }
            Err(Error::KnownPart) => Ok(None),
            Err(Error::UnknownChunk) => {
                debug!(target: "shards", "Received part {} for unknown chunk {:?}, declining", part_id, chunk_hash);
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    pub fn process_chunk_one_part(
        &mut self,
        one_part: ChunkOnePart,
        chain_store: &mut ChainStore,
    ) -> Result<ProcessChunkOnePartResult, Error> {
        let chunk_hash = one_part.chunk_hash.clone();
        let prev_block_hash = one_part.header.inner.prev_block_hash;

        if let Some(ec) = self.encoded_chunks.get(&one_part.chunk_hash) {
            if ec.content.parts[one_part.part_id as usize].is_some() {
                return Ok(ProcessChunkOnePartResult::Known);
            }
        }

        match self.runtime_adapter.get_epoch_id_from_prev_block(&prev_block_hash) {
            Ok(_) => {}
            Err(err) => {
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
            // This is not slashable behvior, because we can't prove that merkle path is the one that validator signed.
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
                    || shard_id != (one_part.receipt_proofs[proof_index].1).to_shard_id
                    || !verify_path(
                        one_part.header.inner.outgoing_receipts_root,
                        &(one_part.receipt_proofs[proof_index].1).proof,
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

        let total_parts = self.runtime_adapter.num_total_parts(&prev_block_hash);
        self.encoded_chunks.entry(one_part.chunk_hash.clone()).or_insert_with(|| {
            EncodedShardChunk::from_header(one_part.header.clone(), total_parts)
        });

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

        self.merkle_paths
            .insert((one_part.chunk_hash.clone(), one_part.part_id), one_part.merkle_path.clone());

        self.encoded_chunks.get_mut(&one_part.chunk_hash).unwrap().content.parts
            [one_part.part_id as usize] = Some(one_part.part.clone());

        // Fills in all the rest of the parts if there is enough parts,
        // so if it's complete, have_all_one_parts will be true after this.
        self.persist_chunk_if_complete(&one_part.chunk_hash, &prev_block_hash, chain_store)?;

        let have_all_one_parts = self.has_all_one_parts(
            &prev_block_hash,
            self.encoded_chunks.get(&one_part.chunk_hash).unwrap(),
        )?;

        if have_all_one_parts {
            let mut store_update = chain_store.store_update();
            store_update.save_chunk_one_part(&chunk_hash, one_part.clone());
            store_update.commit()?;

            self.requested_one_parts.remove(&one_part.chunk_hash);
        }

        // If we do not follow this shard, having the one parts is sufficient to include the chunk in the block
        if have_all_one_parts
            && !self.cares_about_shard_this_or_next_epoch(
                self.me.as_ref(),
                &prev_block_hash,
                one_part.shard_id,
                true,
            )
        {
            self.block_hash_to_chunk_headers
                .entry(one_part.header.inner.prev_block_hash)
                .or_insert_with(|| vec![])
                .push((one_part.shard_id, one_part.header.clone()));
        }

        Ok(if have_all_one_parts {
            ProcessChunkOnePartResult::HaveAllOneParts
        } else {
            ProcessChunkOnePartResult::NeedMoreOneParts
        })
    }

    fn has_all_one_parts(
        &self,
        prev_block_hash: &CryptoHash,
        encoded_chunk: &EncodedShardChunk,
    ) -> Result<bool, Error> {
        for part_id in 0..self.runtime_adapter.num_total_parts(&prev_block_hash) {
            if encoded_chunk.content.parts[part_id].is_none() {
                if Some(self.runtime_adapter.get_part_owner(&prev_block_hash, part_id as u64)?)
                    == self.me
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    pub fn create_encoded_shard_chunk(
        &mut self,
        prev_block_hash: CryptoHash,
        prev_state_root: StateRoot,
        outcome_root: CryptoHash,
        height: u64,
        shard_id: ShardId,
        gas_used: Gas,
        gas_limit: Gas,
        rent_paid: Balance,
        validator_reward: Balance,
        balance_burnt: Balance,
        validator_proposals: Vec<ValidatorStake>,
        transactions: &Vec<SignedTransaction>,
        outgoing_receipts: &Vec<Receipt>,
        outgoing_receipts_root: CryptoHash,
        tx_root: CryptoHash,
        signer: &dyn Signer,
    ) -> Result<(EncodedShardChunk, Vec<MerklePath>), Error> {
        let total_parts = self.runtime_adapter.num_total_parts(&prev_block_hash);
        let data_parts = self.runtime_adapter.num_data_parts(&prev_block_hash);
        EncodedShardChunk::new(
            prev_block_hash,
            prev_state_root,
            outcome_root,
            height,
            shard_id,
            total_parts,
            data_parts,
            gas_used,
            gas_limit,
            rent_paid,
            validator_reward,
            balance_burnt,
            tx_root,
            validator_proposals,
            transactions,
            outgoing_receipts,
            outgoing_receipts_root,
            signer,
        )
        .map_err(|err| err.into())
    }

    pub fn process_encoded_chunk(
        &mut self,
        chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        chain_store: &mut ChainStore,
    ) -> Result<Option<CryptoHash>, Error> {
        let chunk_hash = chunk.header.chunk_hash();
        assert!(self.encoded_chunks.contains_key(&chunk_hash));
        let cares_about_shard = self.cares_about_shard_this_or_next_epoch(
            self.me.as_ref(),
            &chunk.header.inner.prev_block_hash,
            chunk.header.inner.shard_id,
            true,
        );
        let mut store_update = chain_store.store_update();
        if let Ok(shard_chunk) = chunk
            .decode_chunk(self.runtime_adapter.num_data_parts(&chunk.header.inner.prev_block_hash))
            .map_err(|err| Error::from(err))
            .and_then(|shard_chunk| {
                if !validate_chunk_proofs(&shard_chunk, &*self.runtime_adapter)? {
                    return Err(Error::InvalidChunk);
                }
                Ok(shard_chunk)
            })
        {
            debug!(target: "chunks", "Reconstructed and decoded chunk {}, encoded length was {}, num txs: {}, I'm {:?}", chunk_hash.0, chunk.header.inner.encoded_length, shard_chunk.transactions.len(), self.me);

            // Decoded a valid chunk, store it in the permanent store ...
            store_update.save_chunk(&chunk_hash, shard_chunk);
            store_update.commit()?;
            // ... and include into the block if we are the producer
            if cares_about_shard {
                self.block_hash_to_chunk_headers
                    .entry(chunk.header.inner.prev_block_hash)
                    .or_insert_with(|| vec![])
                    .push((chunk.header.inner.shard_id, chunk.header.clone()));
            }

            for (part_id, merkle_path) in merkle_paths.iter().enumerate() {
                let part_id = part_id as u64;
                self.merkle_paths.insert((chunk_hash.clone(), part_id), merkle_path.clone());
            }
            self.requested_chunks.remove(&chunk_hash);

            return Ok(Some(chunk.header.inner.prev_block_hash));
        } else {
            // Can't decode chunk or has invalid proofs, ignore it
            error!(target: "chunks", "Reconstructed, but failed to decoded chunk {}, I'm {:?}", chunk_hash.0, self.me);
            for i in 0..self.runtime_adapter.num_total_parts(&chunk.header.inner.prev_block_hash) {
                self.merkle_paths.remove(&(chunk_hash.clone(), i as u64));
            }
            store_update.save_invalid_chunk(chunk);
            store_update.commit()?;
            self.encoded_chunks.remove(&chunk_hash);
            self.requested_chunks.remove(&chunk_hash);
            return Err(Error::InvalidChunk);
        }
    }

    pub fn distribute_encoded_chunk(
        &mut self,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        outgoing_receipts: Vec<Receipt>,
        chain_store: &mut ChainStore,
    ) -> Result<(), Error> {
        // TODO: if the number of validators exceeds the number of parts, this logic must be changed
        let prev_block_hash = encoded_chunk.header.inner.prev_block_hash;
        let chunk_hash = encoded_chunk.chunk_hash();
        let shard_id = encoded_chunk.header.inner.shard_id;
        let outgoing_receipts_hashes =
            self.runtime_adapter.build_receipts_hashes(&outgoing_receipts).unwrap();
        let (outgoing_receipts_root, outgoing_receipts_proofs) =
            merklize(&outgoing_receipts_hashes);
        assert_eq!(encoded_chunk.header.inner.outgoing_receipts_root, outgoing_receipts_root);

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
                &outgoing_receipts,
                &outgoing_receipts_proofs,
            );
            let one_part = encoded_chunk.create_chunk_one_part(
                part_ord,
                one_part_receipt_proofs,
                merkle_paths[part_ord as usize].clone(),
            );

            // 1/2 This is a weird way to introduce the chunk to the producer's storage
            if Some(&to_whom) == self.me.as_ref() {
                self.process_chunk_one_part(one_part.clone(), chain_store)?;
            } else {
                self.network_adapter.send(NetworkRequests::ChunkOnePartMessage {
                    account_id: to_whom.clone(),
                    header_and_part: one_part,
                });
            }
        }

        // Save merkle paths.
        for (part_id, merkle_path) in merkle_paths.iter().enumerate() {
            let part_id = part_id as u64;
            self.merkle_paths.insert((chunk_hash.clone(), part_id), merkle_path.clone());
        }

        // Save this chunk into encoded_chunks.
        self.encoded_chunks.insert(chunk_hash.clone(), encoded_chunk.clone());

        // Process encoded chunk to add to the store.
        let num_parts = encoded_chunk.content.parts.len();
        self.process_encoded_chunk(
            encoded_chunk,
            (0..num_parts)
                .map(|part_id| {
                    self.merkle_paths.get(&(chunk_hash.clone(), part_id as u64)).unwrap().clone()
                })
                .collect(),
            chain_store,
        )?;

        Ok(())
    }
}
