extern crate log;

use actix::Recipient;
use cached::Cached;
use cached::SizedCache;
use log::{debug, error};
use near_chain::{RuntimeAdapter, ValidTransaction};
use near_network::types::{ChunkPartMsg, ChunkPartRequestMsg, PeerId};
use near_network::NetworkRequests;
use near_pool::{Error, TransactionPool};
use near_primitives::crypto::signer::EDSigner;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{verify_path, MerklePath};
use near_primitives::serialize::{Decode, Encode};
use near_primitives::sharding::{
    ChunkHash, ChunkOnePart, EncodedShardChunk, ShardChunk, ShardChunkHeader,
};
use near_primitives::transaction::{ReceiptTransaction, SignedTransaction};
use near_primitives::types::{AccountId, ShardId};
use near_store::{Store, COL_CHUNKS, COL_CHUNK_ONE_PARTS};
use rand::Rng;
use reed_solomon_erasure::option_shards_into_shards;
use std::collections::hash_map::Entry;
use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;

const MAX_CHUNK_REQUESTS_TO_KEEP_PER_SHARD: usize = 128;
const ORPHANED_ONE_PART_CACHE_SIZE: usize = 1024;

#[derive(PartialEq, Eq)]
pub enum ChunkStatus {
    Complete(Vec<MerklePath>),
    Incomplete,
    Invalid,
}

pub struct ShardsManager {
    me: Option<AccountId>,

    tx_pools: HashMap<ShardId, TransactionPool>,
    state_roots: HashMap<ShardId, CryptoHash>,

    runtime_adapter: Arc<dyn RuntimeAdapter>,
    peer_mgr: Recipient<NetworkRequests>,
    store: Arc<Store>,

    encoded_chunks: HashMap<ChunkHash, EncodedShardChunk>,
    merkle_paths: HashMap<(ChunkHash, u64), MerklePath>,
    block_hash_to_chunk_headers: HashMap<CryptoHash, Vec<(ShardId, ShardChunkHeader)>>,

    orphaned_one_parts: SizedCache<(CryptoHash, ShardId, u64), ChunkOnePart>,

    requested_one_parts: HashSet<ChunkHash>,
    requested_chunks: HashSet<ChunkHash>,

    requests_fifo: VecDeque<(ShardId, ChunkHash, PeerId, u64)>,
    requests: HashMap<(ShardId, ChunkHash, u64), HashSet<(PeerId)>>,
}

impl ShardsManager {
    pub fn new(
        me: Option<AccountId>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        peer_mgr: Recipient<NetworkRequests>,
        store: Arc<Store>,
    ) -> Self {
        Self {
            me,
            tx_pools: HashMap::new(),
            state_roots: HashMap::new(),
            runtime_adapter,
            peer_mgr,
            store,
            encoded_chunks: HashMap::new(),
            merkle_paths: HashMap::new(),
            block_hash_to_chunk_headers: HashMap::new(),
            orphaned_one_parts: SizedCache::with_size(ORPHANED_ONE_PART_CACHE_SIZE),
            requested_one_parts: HashSet::new(),
            requested_chunks: HashSet::new(),
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
            tx_pool.prepare_transactions(expected_weight)
        } else {
            Ok(vec![])
        }
    }
    pub fn cares_about_shard_this_or_next_epoch(
        &self,
        account_id: &AccountId,
        parent_hash: CryptoHash,
        shard_id: ShardId,
    ) -> bool {
        return self.runtime_adapter.cares_about_shard(account_id, parent_hash, shard_id)
            || self.runtime_adapter.will_care_about_shard(account_id, parent_hash, shard_id);
    }
    pub fn request_chunks(&mut self, chunks_to_request: Vec<ShardChunkHeader>) {
        for chunk_header in chunks_to_request {
            let ShardChunkHeader {
                shard_id,
                prev_block_hash: parent_hash,
                height_created: height,
                ..
            } = chunk_header;
            let chunk_hash = chunk_header.chunk_hash();

            if !self.encoded_chunks.contains_key(&chunk_hash) {
                if self.requested_one_parts.contains(&chunk_hash) {
                    continue;
                }
                self.requested_one_parts.insert(chunk_hash.clone());

                let mut requested_one_part = false;
                for part_id in 0..self.runtime_adapter.num_total_parts(parent_hash) {
                    let part_id = part_id as u64;
                    if self.runtime_adapter.get_part_owner(parent_hash, part_id).unwrap()
                        == self.me.clone().unwrap()
                    {
                        requested_one_part = true;
                        error!(
                            "MOO requesting single part for chunk {:?}: {} from {}, I'm {:?}",
                            chunk_hash.clone(),
                            part_id,
                            self.runtime_adapter
                                .get_chunk_proposer(parent_hash, height, shard_id)
                                .unwrap(),
                            self.me.clone().unwrap()
                        );
                        let _ = self.peer_mgr.do_send(NetworkRequests::ChunkOnePartRequest {
                            account_id: self
                                .runtime_adapter
                                .get_chunk_proposer(parent_hash, height, shard_id)
                                .unwrap(),
                            part_request: ChunkPartRequestMsg {
                                shard_id,
                                chunk_hash: chunk_hash.clone(),
                                height,
                                part_id,
                            },
                        });
                    }
                }

                if match self.me.clone() {
                    None => false,
                    Some(me) => self.runtime_adapter.cares_about_shard(&me, parent_hash, shard_id),
                } {
                    assert!(requested_one_part)
                };

                if !requested_one_part {
                    let mut rng = rand::thread_rng();

                    let part_id = rng.gen::<u64>()
                        % (self.runtime_adapter.num_total_parts(parent_hash) as u64);
                    error!(
                        "MOO requesting RANDOM single part for chunk {:?}: {} from {}, I'm {:?}",
                        chunk_hash.clone(),
                        part_id,
                        self.runtime_adapter
                            .get_chunk_proposer(parent_hash, height, shard_id)
                            .unwrap(),
                        self.me.clone().unwrap()
                    );
                    let _ = self.peer_mgr.do_send(NetworkRequests::ChunkOnePartRequest {
                        account_id: self
                            .runtime_adapter
                            .get_chunk_proposer(parent_hash, height, shard_id)
                            .unwrap(),
                        part_request: ChunkPartRequestMsg {
                            shard_id,
                            chunk_hash: chunk_hash.clone(),
                            height,
                            part_id,
                        },
                    });
                }
            } else {
                if self.requested_chunks.contains(&chunk_hash) {
                    continue;
                }
                self.requested_chunks.insert(chunk_hash.clone());

                error!(
                    "MOO requesting all parts for chunk {:?}, I'm {:?}",
                    chunk_hash.clone(),
                    self.me.clone().unwrap()
                );

                for part_id in 0..self.runtime_adapter.num_total_parts(parent_hash) {
                    let part_id = part_id as u64;
                    let _ = self.peer_mgr.do_send(NetworkRequests::ChunkPartRequest {
                        account_id: self
                            .runtime_adapter
                            .get_part_owner(parent_hash, part_id)
                            .unwrap(),
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
    pub fn prepare_chunks(
        &mut self,
        prev_block_hash: CryptoHash,
    ) -> Vec<(ShardId, ShardChunkHeader)> {
        self.block_hash_to_chunk_headers.remove(&prev_block_hash).unwrap_or(vec![])
    }
    pub fn insert_transaction(&mut self, shard_id: ShardId, tx: ValidTransaction) {
        let _ = self
            .tx_pools
            .entry(shard_id)
            .or_insert_with(|| TransactionPool::new())
            .insert_transaction(tx);
    }
    pub fn get_state_root(&self, shard_id: ShardId) -> Option<CryptoHash> {
        self.state_roots.get(&shard_id).map(|x| *x)
    }
    pub fn set_state_root(&mut self, shard_id: ShardId, root: CryptoHash) {
        self.state_roots.insert(shard_id, root);
    }
    pub fn remove_transactions(
        &mut self,
        shard_id: ShardId,
        transactions: &Vec<SignedTransaction>,
    ) {
        self.tx_pools.get_mut(&shard_id).map(|pool| pool.remove_transactions(transactions));
    }
    pub fn reintroduce_transactions(
        &mut self,
        shard_id: ShardId,
        transactions: &Vec<SignedTransaction>,
    ) {
        self.tx_pools.get_mut(&shard_id).map(|pool| pool.reintroduce_transactions(transactions));
    }
    pub fn process_chunk_part_request(
        &mut self,
        request: ChunkPartRequestMsg,
        peer_id: PeerId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut served = false;
        if let Some(chunk) = self.encoded_chunks.get(&request.chunk_hash) {
            if request.part_id as usize >= chunk.content.parts.len() {
                return Err("Failed to process chunk part request: part_id is too big".into());
            }

            if let Some(part) = &chunk.content.parts[request.part_id as usize] {
                served = true;
                let _ = self.peer_mgr.do_send(NetworkRequests::ChunkPart {
                    peer_id,
                    part: ChunkPartMsg {
                        shard_id: request.shard_id,
                        chunk_hash: request.chunk_hash.clone(),
                        part_id: request.part_id,
                        part: part.clone(),
                        // Part should never exist in the chunk content if the merkle path for it is
                        //    not in merkle_paths, so `unwrap` here
                        merkle_path: self
                            .merkle_paths
                            .get(&(request.chunk_hash.clone(), request.part_id))
                            .unwrap()
                            .clone(),
                    },
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
                .or_insert(HashSet::default())
                .insert(peer_id)
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
    pub fn process_chunk_one_part_request(
        &mut self,
        request: ChunkPartRequestMsg,
        account_id: AccountId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_ne!(Some(account_id.clone()), self.me);
        if let Some(encoded_chunk) = self.encoded_chunks.get(&request.chunk_hash) {
            if request.part_id as usize >= encoded_chunk.content.parts.len() {
                return Err("Failed to process chunk one part request: part_id is too big".into());
            }

            // TODO: should have a ref to ChainStore instead, and use the cache
            if let Some(chunk) =
                self.store.get_ser::<ShardChunk>(COL_CHUNKS, request.chunk_hash.as_ref())?
            {
                error!(
                    "MOO responding with single part for chunk {:?}: {} to {}, I'm {:?}",
                    request.chunk_hash.clone(),
                    request.part_id,
                    account_id.clone(),
                    self.me.clone().unwrap()
                );
                let _ = self.peer_mgr.do_send(NetworkRequests::ChunkOnePart {
                    account_id: account_id.clone(),
                    header_and_part: self.create_chunk_one_part(
                        request.chunk_hash.clone(),
                        encoded_chunk,
                        request.part_id,
                        chunk
                            .receipts
                            .iter()
                            .filter(|&receipt| {
                                self.cares_about_shard_this_or_next_epoch(
                                    &account_id,
                                    chunk.header.prev_block_hash,
                                    self.runtime_adapter.account_id_to_shard_id(&receipt.receiver),
                                )
                            })
                            .cloned()
                            .collect(),
                    ),
                });
            } else {
                assert!(false); // TODO XXX MOO remove
            }
        } else {
            assert!(false); // TODO XXX MOO remove
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
            if merkle_root == chunk.header.encoded_merkle_root {
                ChunkStatus::Complete(paths)
            } else {
                ChunkStatus::Invalid
            }
        } else {
            ChunkStatus::Incomplete
        }
    }
    // Returns the height of the enclosing block if a chunk part was not known previously, and the chunk is complete after receiving it
    // Once it receives the last part necessary to reconstruct, the chunk gets reconstructed and fills in all the remaining parts,
    //     thus once the remaining parts arrive, they do not trigger returning true again.
    pub fn process_chunk_part(
        &mut self,
        part: ChunkPartMsg,
    ) -> Result<Option<u64>, Box<dyn std::error::Error>> {
        let cares_about_shard = if let Some(chunk) = self.encoded_chunks.get(&part.chunk_hash) {
            let prev_block_hash = chunk.header.prev_block_hash;
            let shard_id = part.shard_id;
            self.me.clone().map_or_else(
                || false,
                |me| self.cares_about_shard_this_or_next_epoch(&me, prev_block_hash, shard_id),
            )
        } else {
            false
        };

        if let Some(chunk) = self.encoded_chunks.get_mut(&part.chunk_hash) {
            let prev_block_hash = chunk.header.prev_block_hash;
            if chunk.header.shard_id != part.shard_id {
                return Err(
                    "Invalid part message: shard_id doesn't match the header of the chunk".into()
                );
            }
            if (part.part_id as usize) < chunk.content.parts.len() {
                if chunk.content.parts[part.part_id as usize].is_none() {
                    // We have the chunk but haven't seen the part, so actually need to process it
                    // First validate the merkle proof
                    if !verify_path(chunk.header.encoded_merkle_root, &part.merkle_path, &part.part)
                    {
                        return Err("Invalid merkle proof".into());
                    }

                    chunk.content.parts[part.part_id as usize] = Some(part.part);
                    self.merkle_paths
                        .insert((part.chunk_hash.clone(), part.part_id), part.merkle_path);

                    match ShardsManager::check_chunk_complete(
                        self.runtime_adapter.num_data_parts(prev_block_hash),
                        self.runtime_adapter.num_total_parts(prev_block_hash),
                        chunk,
                    ) {
                        ChunkStatus::Complete(merkle_paths) => {
                            let mut store_update = self.store.store_update();
                            if let Ok(shard_chunk) = Self::decode_chunk(
                                self.runtime_adapter.num_data_parts(prev_block_hash),
                                chunk,
                            ) {
                                debug!(target: "chunks", "Reconstructed and decoded chunk {}, encoded length was {}, num txs: {}, I'm {:?}", chunk.header.chunk_hash().0, chunk.header.encoded_length, shard_chunk.transactions.len(), self.me);
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
                                        .entry(chunk.header.prev_block_hash)
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

                                return Ok(Some(chunk.header.height_created));
                            } else {
                                error!(target: "chunks", "Reconstructed but failed to decoded chunk {}", chunk.header.chunk_hash().0);
                                // Can't decode chunk, ignore it
                                for i in 0..self
                                    .runtime_adapter
                                    .num_total_parts(chunk.header.prev_block_hash)
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
                                .num_total_parts(chunk.header.prev_block_hash)
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
            error!("MOO {:?} fucked up", self.me.clone().unwrap());
        }
        Ok(None)
    }
    // Returns true if the chunk_one_part was not previously known
    pub fn process_chunk_one_part(
        &mut self,
        one_part: ChunkOnePart,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let chunk_hash = one_part.chunk_hash.clone();
        let prev_block_hash = one_part.header.prev_block_hash;

        match self.runtime_adapter.get_part_owner(prev_block_hash, one_part.part_id) {
            Ok(owner) => {
                match self.me.clone() {
                    None => {
                        // ChunkOnePartMsg should never be sent to nodes that are not block producers
                        assert!(false); // TODO XXX MOO remove
                        return Ok(false);
                    }
                    Some(me) => {
                        if self.runtime_adapter.cares_about_shard(
                            &me,
                            prev_block_hash,
                            one_part.shard_id,
                        ) {
                            if me != owner {
                                // ChunkOnePartMsg should only be sent to the authority that corresponds to the part_id
                                // unless the authority doesn't validate the shard presently, in which case it requests
                                // random onepart in `request_chunks`, so should also accept any onepart
                                assert!(false); // TODO XXX MOO remove
                                return Ok(false);
                            }
                        }
                    }
                }
                // Fall through, normal case
            }
            Err(err) => {
                error!(
                    "MOO orphaning one part {:?}, prev_block: {:?}, me: {:?}",
                    one_part.chunk_hash, one_part.header.prev_block_hash, self.me
                );
                self.orphaned_one_parts
                    .cache_set((prev_block_hash, one_part.shard_id, one_part.part_id), one_part);
                return Err(err);
            }
        }

        if !self.runtime_adapter.verify_chunk_header_signature(&one_part.header) {
            assert!(false); // TODO XXX MOO remove
            return Err("Incorrect chunk signature when processing ChunkOnePart".into());
        }

        if one_part.shard_id != one_part.header.shard_id {
            assert!(false); // TODO XXX MOO remove
            return Err("Invalid one part: shard_id doesn't match the header".into());
        }

        if !verify_path(one_part.header.encoded_merkle_root, &one_part.merkle_path, &one_part.part)
        {
            // TODO: this is a slashable behavior
            assert!(false); // TODO XXX MOO remove
            return Err("Invalid merkle proof".into());
        }

        if let Some(send_to) =
            self.requests.remove(&(one_part.shard_id, chunk_hash.clone(), one_part.part_id))
        {
            for whom in send_to {
                let _ = self.peer_mgr.do_send(NetworkRequests::ChunkPart {
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
                    self.runtime_adapter.num_total_parts(prev_block_hash),
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
        if self.me.as_ref().map_or_else(
            || false,
            |me| !self.cares_about_shard_this_or_next_epoch(me, prev_block_hash, one_part.shard_id),
        ) {
            self.block_hash_to_chunk_headers
                .entry(one_part.header.prev_block_hash)
                .or_insert_with(|| vec![])
                .push((one_part.shard_id, one_part.header.clone()));
        }
        store_update.commit()?;

        self.encoded_chunks
            .get_mut(&one_part.chunk_hash)
            .map(|x| x.content.parts[one_part.part_id as usize] = Some(one_part.part));

        Ok(ret)
    }

    pub fn process_orphaned_one_parts(&mut self, block_hash: CryptoHash) -> bool {
        let mut ret = false;
        error!("MOO processing orphaned one parts after receiving {:?}", block_hash);
        for shard_id in 0..self.runtime_adapter.num_shards() {
            let shard_id = shard_id as ShardId;
            for part_id in 0..self.runtime_adapter.num_total_parts(block_hash) {
                let part_id = part_id as u64;
                if let Some(a) =
                    self.orphaned_one_parts.cache_remove(&(block_hash, shard_id, part_id))
                {
                    let shard_id = a.shard_id;
                    let chunk_header = a.header.clone();
                    error!(
                        "MOO processing orphaned one part {:?}",
                        chunk_header.chunk_hash().clone()
                    );
                    match self.process_chunk_one_part(a) {
                        Ok(cur) => {
                            ret |= cur;

                            if self.me.is_some()
                                && self.cares_about_shard_this_or_next_epoch(
                                    self.me.as_ref().unwrap(),
                                    block_hash,
                                    shard_id,
                                )
                            {
                                self.request_chunks(vec![chunk_header]);
                            }
                        }
                        Err(_) => {
                            assert!(false); /* ignore error */
                            // MOO XXX TODO remove assert
                        }
                    }
                }
            }
        }
        ret
    }

    pub fn decode_chunk(
        data_parts: usize,
        encoded_chunk: &EncodedShardChunk,
    ) -> Result<ShardChunk, io::Error> {
        let encoded_data =
            option_shards_into_shards(encoded_chunk.content.parts[0..data_parts].to_vec())
                .iter()
                .map(|boxed| boxed.iter())
                .flatten()
                .cloned()
                .collect::<Vec<u8>>()[0..encoded_chunk.header.encoded_length as usize]
                .to_vec();

        let (transactions, receipts): (Vec<SignedTransaction>, Vec<ReceiptTransaction>) =
            Decode::decode(&encoded_data)?;

        Ok(ShardChunk {
            chunk_hash: encoded_chunk.chunk_hash(),
            header: encoded_chunk.header.clone(),
            transactions,
            receipts,
        })
    }

    pub fn create_encoded_shard_chunk(
        &mut self,
        prev_block_hash: CryptoHash,
        prev_state_root: CryptoHash,
        height: u64,
        shard_id: ShardId,
        transactions: &Vec<SignedTransaction>,
        receipts: &Vec<ReceiptTransaction>,
        signer: Arc<dyn EDSigner>,
    ) -> Result<EncodedShardChunk, io::Error> {
        let mut bytes = Encode::encode(&(transactions, receipts))?;
        let total_parts = self.runtime_adapter.num_total_parts(prev_block_hash);
        let data_parts = self.runtime_adapter.num_data_parts(prev_block_hash);
        let parity_parts = total_parts - data_parts;

        let mut parts = vec![];
        let encoded_length = bytes.len();

        if bytes.len() % data_parts != 0 {
            bytes.extend((bytes.len() % data_parts..data_parts).map(|_| 0));
        }
        let shard_length = (encoded_length + data_parts - 1) / data_parts;
        assert_eq!(bytes.len(), shard_length * data_parts);

        for i in 0..data_parts {
            parts.push(Some(
                bytes[i * shard_length..(i + 1) * shard_length].to_vec().into_boxed_slice()
                    as Box<[u8]>,
            ));
        }
        for _i in data_parts..total_parts {
            parts.push(None);
        }

        let (new_chunk, merkle_paths) = EncodedShardChunk::from_parts_and_metadata(
            prev_block_hash,
            prev_state_root,
            height,
            shard_id,
            encoded_length as u64,
            parts,
            data_parts,
            parity_parts,
            signer,
        );

        for (part_id, merkle_path) in merkle_paths.iter().enumerate() {
            let part_id = part_id as u64;
            self.merkle_paths.insert((new_chunk.chunk_hash(), part_id), merkle_path.clone());
        }

        Ok(new_chunk)
    }

    pub fn create_chunk_one_part(
        &self,
        chunk_hash: ChunkHash,
        encoded_chunk: &EncodedShardChunk,
        part_id: u64,
        receipts: Vec<ReceiptTransaction>,
    ) -> ChunkOnePart {
        ChunkOnePart {
            shard_id: encoded_chunk.header.shard_id,
            chunk_hash: chunk_hash.clone(),
            header: encoded_chunk.header.clone(),
            part_id,
            part: encoded_chunk.content.parts[part_id as usize].clone().unwrap(),
            receipts,
            // It should be impossible to have a part but not the merkle path
            merkle_path: self.merkle_paths.get(&(chunk_hash.clone(), part_id)).unwrap().clone(),
        }
    }

    pub fn distribute_encoded_chunk(
        &mut self,
        encoded_chunk: EncodedShardChunk,
        receipts: Vec<ReceiptTransaction>,
    ) {
        // TODO: if the number of validators exceeds the number of parts, this logic must be changed
        let prev_block_hash = encoded_chunk.header.prev_block_hash;
        let mut processed_one_part = false;
        let chunk_hash = encoded_chunk.chunk_hash();
        let shard_id = encoded_chunk.header.shard_id;
        for part_ord in 0..self.runtime_adapter.num_total_parts(prev_block_hash) {
            let part_ord = part_ord as u64;
            let to_whom = self.runtime_adapter.get_part_owner(prev_block_hash, part_ord).unwrap();
            let one_part = self.create_chunk_one_part(
                chunk_hash.clone(),
                &encoded_chunk,
                part_ord,
                receipts
                    .iter()
                    .filter(|&receipt| {
                        self.cares_about_shard_this_or_next_epoch(
                            &to_whom,
                            prev_block_hash,
                            self.runtime_adapter.account_id_to_shard_id(&receipt.receiver),
                        )
                    })
                    .cloned()
                    .collect(),
            );

            // 1/2 This is a weird way to introduce the chunk to the producer's storage
            if !processed_one_part && Some(&to_whom) == self.me.as_ref() {
                processed_one_part = true;
                self.process_chunk_one_part(one_part.clone()).unwrap();
            }

            let _ = self.peer_mgr.do_send(NetworkRequests::ChunkOnePart {
                account_id: to_whom.clone(),
                header_and_part: one_part,
            });
        }

        // 2/2 This is a weird way to introduce the chunk to the producer's storage
        for (part_id, part) in encoded_chunk.content.parts.iter().enumerate() {
            let part_id = part_id as u64;
            self.process_chunk_part(ChunkPartMsg {
                shard_id,
                chunk_hash: chunk_hash.clone(),
                part_id,
                part: part.clone().unwrap(),
                // It should be impossible to have a part but not the merkle path
                merkle_path: self.merkle_paths.get(&(chunk_hash.clone(), part_id)).unwrap().clone(),
            })
            .unwrap();
        }
    }
}
