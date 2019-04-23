use crate::messages::{ChunkHeaderMsg, ChunkPartMsg};
use crate::orchestrator::BaseOrchestrator;
use primitives::hash::CryptoHash;
use primitives::sharding::EncodedShardChunk;
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
        if self.chunks.contains_key(&hash) || self.statuses.contains_key(&hash) {
            return;
        }

        while self.fifo.len() + 1 > MAX_SHARD_CHUNKS_TO_KEEP_PER_SHARD {
            let hash_to_remove = self.fifo.pop_front().unwrap();

            self.chunks.remove(&hash_to_remove);
            self.statuses.remove(&hash_to_remove);
            self.in_progress.remove(&hash_to_remove);
        }

        self.fifo.push_back(hash);
        self.in_progress.insert(hash);
        self.statuses.insert(hash, ChunkStatus::PrepareToFetch);
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

pub fn shard_chain_chunks_mgr_worker(
    mgr: Arc<RwLock<ShardChainChunksManager>>,
    orchestrator: Arc<RwLock<impl BaseOrchestrator>>,

    chunk_header_request_tx: Sender<(CryptoHash)>,
    chunk_part_request_tx: Sender<(CryptoHash, u64)>,

    chunk_header_rx: Receiver<ChunkHeaderMsg>,
    chunk_part_rx: Receiver<ChunkPartMsg>,

    terminated: Arc<RwLock<bool>>,
) {
    loop {
        {
            if *terminated.read().unwrap() {
                break;
            }
        }

        let mut mgr = mgr.write().unwrap();

        let ShardChainChunksManager { statuses, in_progress, chunks, .. } = &mut *mgr;
        let mut in_progress_to_remove = vec![];

        for msg in &chunk_header_rx {
            chunks.insert(msg.chunk_hash, EncodedShardChunk::from_header(msg.header));
        }

        for msg in &chunk_part_rx {
            chunks.get_mut(&msg.chunk_hash).unwrap().content.parts[msg.part_id] = Some(msg.part);
        }

        for hash in in_progress.iter() {
            let value = statuses.get_mut(hash).unwrap();

            let orchestrator = orchestrator.write().unwrap();

            let data_parts = orchestrator.get_data_chunk_parts_num();
            let parity_parts = orchestrator.get_total_chunk_parts_num() - data_parts;

            match *value {
                ChunkStatus::PrepareToFetch => {
                    assert!(chunks.get(hash).is_none());
                    let _ = chunk_header_request_tx.send(*hash);
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
        }
    }
}
