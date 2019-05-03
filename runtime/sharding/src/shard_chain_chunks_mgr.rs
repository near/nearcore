use crate::messages::{ChunkHeaderAndPartMsg, ChunkHeaderMsg, ChunkPartMsg};
use crate::orchestrator::BaseOrchestrator;
use primitives::hash::CryptoHash;
use primitives::sharding::EncodedShardChunk;
use primitives::types::AuthorityId;
use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};

const MAX_SHARD_CHUNKS_TO_KEEP_PER_SHARD: usize = 128;

#[derive(Clone, Copy)]
enum ChunkStatus {
    PrepareToFetch,
    FetchingHeader,
    FetchingBody,
    FailedToFetch,
    Present,
}

pub enum ShardChainChunkFetchError {
    Unknown,
    NotReady,
    Failed,
}

#[derive(Default)]
pub struct ShardChainChunksManager {
    chunks: HashMap<CryptoHash, EncodedShardChunk>,
    statuses: HashMap<CryptoHash, ChunkStatus>,

    fifo: VecDeque<CryptoHash>,
    in_progress: HashSet<CryptoHash>,
}

impl ShardChainChunksManager {
    pub fn request_fetch(&mut self, hash: CryptoHash) {
        if self.statuses.contains_key(&hash) {
            return;
        }

        let existing_chunk = self.chunks.get(&hash);

        match existing_chunk {
            None => self.statuses.insert(hash, ChunkStatus::PrepareToFetch),
            Some(_) => self.statuses.insert(hash, ChunkStatus::FetchingHeader),
        };

        while self.fifo.len() + 1 > MAX_SHARD_CHUNKS_TO_KEEP_PER_SHARD {
            let hash_to_remove = self.fifo.pop_front().unwrap();

            self.chunks.remove(&hash_to_remove);
            self.statuses.remove(&hash_to_remove);
            self.in_progress.remove(&hash_to_remove);
        }

        self.fifo.push_back(hash);
        self.in_progress.insert(hash);
    }

    pub fn insert_chunk(&mut self, hash: CryptoHash, chunk: EncodedShardChunk) {
        self.chunks.insert(hash, chunk);
        self.statuses.insert(hash, ChunkStatus::Present);
        self.fifo.push_back(hash);
        self.in_progress.remove(&hash);
    }

    pub fn get_encoded_chunk(
        &self,
        hash: CryptoHash,
    ) -> Result<&EncodedShardChunk, ShardChainChunkFetchError> {
        match self.statuses.get(&hash) {
            None => Err(ShardChainChunkFetchError::Unknown),
            Some(ChunkStatus::FailedToFetch) => {
                assert!(self.in_progress.get(&hash).is_none());
                Err(ShardChainChunkFetchError::Failed)
            }
            Some(ChunkStatus::Present) => {
                let ret = self.chunks.get(&hash).unwrap();
                assert!(self.in_progress.get(&hash).is_none());

                return Ok(&ret);
            }
            _ => Err(ShardChainChunkFetchError::NotReady),
        }
    }
}

pub fn shard_chain_chunks_exchange_worker(
    mgr: Arc<RwLock<ShardChainChunksManager>>,
    orchestrator: Arc<RwLock<impl BaseOrchestrator>>,

    chunk_header_request_rx: Receiver<(AuthorityId, CryptoHash)>,
    chunk_part_request_rx: Receiver<(AuthorityId, CryptoHash, u64)>,

    chunk_header_tx: Sender<(AuthorityId, ChunkHeaderMsg)>,
    chunk_part_tx: Sender<(AuthorityId, ChunkPartMsg)>,
    chunk_header_and_part_tx: Sender<(AuthorityId, ChunkHeaderAndPartMsg)>,

    chunk_publishing_rx: Receiver<EncodedShardChunk>,

    terminated: Arc<RwLock<bool>>,
) {
    loop {
        {
            if *terminated.read().unwrap() {
                break;
            }
        }

        let mut work_done = false;

        for (whom, hash) in chunk_header_request_rx.try_iter() {
            let mgr = mgr.read().unwrap();
            if let Some(x) = mgr.chunks.get(&hash) {
                let _ = chunk_header_tx
                    .send((whom, ChunkHeaderMsg { chunk_hash: hash, header: x.header.clone() }));
                work_done = true;
            }
        }

        for (whom, hash, part_id) in chunk_part_request_rx.try_iter() {
            let mgr = mgr.read().unwrap();
            if let Some(x) = mgr.chunks.get(&hash) {
                if (part_id as usize) < x.content.parts.len()
                    && x.content.parts[part_id as usize].is_some()
                {
                    let _ = chunk_part_tx.send((
                        whom,
                        ChunkPartMsg {
                            chunk_hash: hash,
                            part_id,
                            part: x.content.parts[part_id as usize].as_ref().unwrap().clone(),
                        },
                    ));
                    work_done = true;
                }
            }
        }

        for chunk in chunk_publishing_rx.try_iter() {
            let mut mgr = mgr.write().unwrap();
            let orchestrator = orchestrator.read().unwrap();

            let hash = chunk.chunk_hash();
            for part_id in 0..orchestrator.get_total_chunk_parts_num() {
                let authority_id = orchestrator.get_authority_id_for_part(part_id);
                let _ = chunk_header_and_part_tx.send((
                    authority_id,
                    ChunkHeaderAndPartMsg {
                        chunk_hash: hash,
                        part_id: part_id as u64,
                        header: chunk.header.clone(),
                        part: chunk.content.parts[part_id].as_ref().unwrap().clone(),
                    },
                ));
            }

            mgr.insert_chunk(hash, chunk);
            work_done = true;
        }

        if !work_done {
            std::thread::yield_now();
        }
    }
}

pub fn shard_chain_chunks_mgr_worker(
    mgr: Arc<RwLock<ShardChainChunksManager>>,
    orchestrator: Arc<RwLock<impl BaseOrchestrator>>,

    chunk_header_request_tx: Sender<(CryptoHash)>,
    chunk_part_request_tx: Sender<(CryptoHash, u64)>,

    chunk_header_rx: Receiver<ChunkHeaderMsg>,
    chunk_part_rx: Receiver<ChunkPartMsg>,
    chunk_header_and_part_rx: Receiver<ChunkHeaderAndPartMsg>,

    terminated: Arc<RwLock<bool>>,
) {
    loop {
        {
            if *terminated.read().unwrap() {
                break;
            }
        }

        let mut work_done = false;

        {
            let mut mgr = mgr.write().unwrap();

            let ShardChainChunksManager { statuses, in_progress, chunks, .. } = &mut *mgr;
            let mut in_progress_to_remove = vec![];

            for msg in chunk_header_rx.try_iter() {
                chunks.entry(msg.chunk_hash).or_insert(EncodedShardChunk::from_header(
                    msg.header,
                    orchestrator.read().unwrap().get_total_chunk_parts_num(),
                ));
                work_done = true;
            }

            for msg in chunk_part_rx.try_iter() {
                chunks
                    .get_mut(&msg.chunk_hash)
                    .map(|x| x.content.parts[msg.part_id as usize] = Some(msg.part));
                work_done = true;
            }

            for msg in chunk_header_and_part_rx.try_iter() {
                chunks.entry(msg.chunk_hash).or_insert(EncodedShardChunk::from_header(
                    msg.header.clone(),
                    orchestrator.read().unwrap().get_total_chunk_parts_num(),
                ));
                chunks
                    .get_mut(&msg.chunk_hash)
                    .map(|x| x.content.parts[msg.part_id as usize] = Some(msg.part));
                work_done = true;
            }

            for hash in in_progress.iter() {
                let value = statuses.get_mut(hash).unwrap();

                let orchestrator = orchestrator.read().unwrap();

                let data_parts = orchestrator.get_data_chunk_parts_num();
                let parity_parts = orchestrator.get_total_chunk_parts_num() - data_parts;

                match *value {
                    ChunkStatus::PrepareToFetch => {
                        // The header might have been received in the meantime
                        match chunks.get(hash) {
                            None => {
                                let _ = chunk_header_request_tx.send(*hash);
                            }
                            _ => {}
                        };
                        *value = ChunkStatus::FetchingHeader;
                    }
                    ChunkStatus::FetchingHeader => {
                        if chunks.get(hash).is_some() {
                            for i in 0..orchestrator.get_total_chunk_parts_num() {
                                let _ = chunk_part_request_tx.send((*hash, i as u64));
                            }
                            *value = ChunkStatus::FetchingBody;
                        }
                    }
                    ChunkStatus::FetchingBody => {
                        let chunk = chunks.get_mut(hash).unwrap();
                        if chunk.content.num_fetched_parts() >= data_parts {
                            chunk.content.reconstruct(data_parts, parity_parts);
                            in_progress_to_remove.push(*hash);

                            let (merkle_root, _) = chunk.content.get_merkle_hash_and_paths();
                            if merkle_root == chunk.header.encoded_merkle_root {
                                *value = ChunkStatus::Present;
                            } else {
                                chunks.remove(hash);
                                *value = ChunkStatus::FailedToFetch;
                            }
                        }
                    }
                    ChunkStatus::Present => {
                        unreachable!();
                    }
                    ChunkStatus::FailedToFetch => {
                        unreachable!();
                    }
                }
            }

            for hash in in_progress_to_remove {
                in_progress.remove(&hash);
                work_done = true;
            }
        }

        if !work_done {
            std::thread::yield_now();
        }
    }
}
