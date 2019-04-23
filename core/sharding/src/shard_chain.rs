use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};

use primitives::hash::{hash, CryptoHash};
use primitives::sharding::EncodedShardChunk;
use primitives::types::AuthorityId;

use super::orchestrator::BaseOrchestrator;
use crate::shard_chain_chunks_mgr::{ShardChainChunkFetchError, ShardChainChunksManager};
use reed_solomon_erasure::Shard;

pub fn shard_chain_worker(
    me: AuthorityId,
    shard_id: u64,
    orchestrator: Arc<RwLock<impl BaseOrchestrator>>,

    finalized_chunk_rx: Receiver<(CryptoHash, u64, bool)>,
    last_chunk_hash_bridge: Arc<RwLock<Option<(CryptoHash, u64)>>>,
    chunk_producer_bridge: Arc<RwLock<Option<EncodedShardChunk>>>,
    chunk_publishing_tx: Sender<EncodedShardChunk>,

    terminated: Arc<RwLock<bool>>,
) {
    let shard_genesis_hash: CryptoHash = hash(&[1u8]);

    let mut last_finalized_chunk_hash: CryptoHash = shard_genesis_hash;
    let mut last_height: u64 = 0;
    let mut is_syncing: bool = true;

    let mut current_chunk_hash: Option<CryptoHash> = None;
    let mut next_chunk: Option<EncodedShardChunk> = None;

    loop {
        {
            if *terminated.read().unwrap() {
                break;
            }
        }

        if let Ok((new_finalized_chunk_hash, new_height, new_is_syncing)) =
            finalized_chunk_rx.try_recv()
        {
            last_finalized_chunk_hash = new_finalized_chunk_hash;
            last_height = new_height;
            is_syncing = new_is_syncing;

            let mut matches_next_prev = false;
            if let Some(next_chunk) = &next_chunk {
                if next_chunk.header.prev_block_hash == new_finalized_chunk_hash {
                    matches_next_prev = true;
                }
            }

            if matches_next_prev {
                current_chunk_hash = Some(next_chunk.as_ref().unwrap().chunk_hash());
                let _ = chunk_publishing_tx.send(next_chunk.unwrap());
                *last_chunk_hash_bridge.write().unwrap() =
                    Some((current_chunk_hash.unwrap(), last_height + 1));
            } else {
                current_chunk_hash = None;
                *last_chunk_hash_bridge.write().unwrap() =
                    Some((last_finalized_chunk_hash, last_height));
            }

            next_chunk = None;
        }

        if is_syncing {
            std::thread::yield_now();
            continue;
        }

        let orchestrator = &*orchestrator.read().unwrap();
        if orchestrator.is_shard_chunk_producer(me, shard_id, last_height + 1)
            && current_chunk_hash.is_none()
        {
            let last_created_chunk = &mut *chunk_producer_bridge.write().unwrap();
            if let Some(x) = last_created_chunk {
                if x.header.prev_block_hash == last_finalized_chunk_hash {
                    let current_chunk = last_created_chunk.take();
                    current_chunk_hash = Some(current_chunk.as_ref().unwrap().chunk_hash());
                    let _ = chunk_publishing_tx.send(current_chunk.unwrap());
                    *last_chunk_hash_bridge.write().unwrap() =
                        Some((current_chunk_hash.unwrap(), last_height + 1));
                }
            }
        }

        if orchestrator.is_shard_chunk_producer(me, shard_id, last_height + 2)
            && current_chunk_hash.is_some()
            && next_chunk.is_none()
        {
            let last_created_chunk = &mut *chunk_producer_bridge.write().unwrap();
            if let Some(x) = last_created_chunk {
                if x.header.prev_block_hash == current_chunk_hash.unwrap() {
                    next_chunk = last_created_chunk.take();
                    *last_chunk_hash_bridge.write().unwrap() = None;
                }
            }
        }
    }
}

pub fn shard_chain_block_producer(
    me: AuthorityId,
    shard_id: u64,
    orchestrator: Arc<RwLock<impl BaseOrchestrator>>,

    chunks_mgr: Arc<RwLock<ShardChainChunksManager>>,

    last_chunk_hash_bridge: Arc<RwLock<Option<(CryptoHash, u64)>>>,
    chunk_producer_bridge: Arc<RwLock<Option<EncodedShardChunk>>>,

    terminated: Arc<RwLock<bool>>,
) {
    let shard_genesis_hash: CryptoHash = hash(&[1u8]);

    loop {
        {
            if *terminated.read().unwrap() {
                break;
            }
        }

        let last_chunk_hash_and_height = { *last_chunk_hash_bridge.read().unwrap() };

        if last_chunk_hash_and_height.is_none() {
            std::thread::yield_now();
            continue;
        }

        let (last_chunk_hash, last_height) = last_chunk_hash_and_height.unwrap();

        if last_chunk_hash == shard_genesis_hash {
            let orchestrator = &*orchestrator.write().unwrap();
            if orchestrator.is_shard_chunk_producer(me, shard_id, last_height + 1) {
                println!("YAY!");
                let new_chunk = produce_chunk(orchestrator, last_chunk_hash, last_height);

                *chunk_producer_bridge.write().unwrap() = Some(new_chunk);

                {
                    // only overwrite the value of last_chunk_hash_bridge if it didn't change
                    let mut bridge_local = last_chunk_hash_bridge.write().unwrap();
                    if let Some((current_hash, _)) = *bridge_local {
                        if current_hash == last_chunk_hash {
                            *bridge_local = None;
                        }
                    }
                }
            }

            continue;
        }

        let mut need_to_request = false;

        match chunks_mgr.read().unwrap().get_encoded_chunk(last_chunk_hash) {
            Ok(_) => {
                let orchestrator = &*orchestrator.write().unwrap();
                // TODO: some proper block production / previous blocks fetching needs to be happening here
                if orchestrator.is_shard_chunk_producer(me, shard_id, last_height + 1) {
                    let new_chunk = produce_chunk(orchestrator, last_chunk_hash, last_height);

                    *chunk_producer_bridge.write().unwrap() = Some(new_chunk);

                    {
                        // only overwrite the value of last_chunk_hash_bridge if it didn't change
                        let mut bridge_local = last_chunk_hash_bridge.write().unwrap();
                        if let Some((current_hash, _)) = *bridge_local {
                            if current_hash == last_chunk_hash {
                                *bridge_local = None;
                            }
                        }
                    }
                }
            }
            Err(err) => match err {
                ShardChainChunkFetchError::Unknown => {
                    need_to_request = true;
                }
                ShardChainChunkFetchError::NotReady => {
                    std::thread::yield_now();
                    continue;
                }
                ShardChainChunkFetchError::Failed => {
                    println!("Got here FAILURE!");
                    *last_chunk_hash_bridge.write().unwrap() = None;
                }
            },
        }

        if need_to_request {
            chunks_mgr.write().unwrap().request_fetch(last_chunk_hash)
        }
    }
}

fn produce_chunk(
    orchestrator: &impl BaseOrchestrator,
    last_chunk_hash: CryptoHash,
    last_height: u64,
) -> EncodedShardChunk {
    let mut parts: Vec<Option<Shard>> = vec![];
    let data_parts_num = orchestrator.get_data_chunk_parts_num();
    let parity_parts_num = orchestrator.get_total_chunk_parts_num() - data_parts_num;
    for i in 0..data_parts_num {
        parts.push(Some(Box::new([(i % 256) as u8; 1024]) as Box<[u8]>));
    }
    for _i in 0..parity_parts_num {
        parts.push(None);
    }
    let new_chunk: EncodedShardChunk = EncodedShardChunk::from_parts_and_metadata(
        last_chunk_hash,
        last_height + 1,
        parts,
        data_parts_num,
        parity_parts_num,
    );
    new_chunk
}

#[cfg(test)]
mod tests {
    use crate::messages::{ChunkHeaderMsg, ChunkPartMsg};
    use crate::orchestrator::BaseOrchestrator;
    use crate::shard_chain::{shard_chain_block_producer, shard_chain_worker};
    use crate::shard_chain_chunks_mgr::{shard_chain_chunks_mgr_worker, ShardChainChunksManager};
    use primitives::hash::{hash, CryptoHash};
    use primitives::sharding::EncodedShardChunk;
    use primitives::types::AuthorityId;
    use std::collections::HashMap;
    use std::sync::mpsc::Receiver;
    use std::sync::mpsc::{channel, Sender};
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::thread::JoinHandle;
    use std::time::Duration;

    struct TestOrchestrator {
        n_producers: u64,
    }

    impl TestOrchestrator {
        pub fn new(n_producers: u64) -> Self {
            Self { n_producers }
        }
    }

    impl BaseOrchestrator for TestOrchestrator {
        fn is_shard_chunk_producer(
            &self,
            authority_id: AuthorityId,
            _shard_id: u64,
            height: u64,
        ) -> bool {
            height % self.n_producers == (authority_id as u64)
        }
        fn is_block_producer(&self, _authority_id: AuthorityId, _height: u64) -> bool {
            false
        }

        fn get_total_chunk_parts_num(&self) -> usize {
            30
        }
        fn get_data_chunk_parts_num(&self) -> usize {
            10
        }
    }

    fn wait_for_n<T>(receiver: &Receiver<T>, num: usize, timeout_ms: u64) -> Vec<T> {
        let mut ret = vec![];
        for _i in 0..num {
            ret.push(
                receiver
                    .recv_timeout(Duration::from_millis(timeout_ms))
                    .expect("wait_for_n gave up on waiting"),
            );
        }
        return ret;
    }

    enum WhaleCunksTransmittingMode {
        NoBroadcast, // the chunks are not broadcasted, and no chunks manager worker spawned
        Broadcast(
            Sender<CryptoHash>,
            Sender<(CryptoHash, u64)>,
            Receiver<ChunkHeaderMsg>,
            Receiver<ChunkPartMsg>,
        ), // the worker spawned
    }

    fn spawn_whale(
        me: AuthorityId,
        shard_id: u64,
        transmitting_mode: WhaleCunksTransmittingMode,
        orchestrator: &Arc<RwLock<impl BaseOrchestrator + 'static>>,
        finalized_chunk_rx: Receiver<(CryptoHash, u64, bool)>,
        chunk_publishing_tx: Sender<EncodedShardChunk>,
        terminated: &Arc<RwLock<bool>>,
        chunks_mgr: &Arc<RwLock<ShardChainChunksManager>>,
    ) -> (JoinHandle<()>, JoinHandle<()>, JoinHandle<()>) {
        let orchestrator1 = orchestrator.clone();
        let last_chunk_hash_bridge = Arc::new(RwLock::new(None));
        let chunk_producer_bridge = Arc::new(RwLock::new(None));
        let last_chunk_hash_bridge1 = last_chunk_hash_bridge.clone();
        let chunk_producer_bridge1 = chunk_producer_bridge.clone();
        let terminated1 = terminated.clone();
        let t1 = thread::spawn(move || {
            shard_chain_worker(
                me,
                shard_id,
                orchestrator1,
                finalized_chunk_rx,
                last_chunk_hash_bridge1,
                chunk_producer_bridge1,
                chunk_publishing_tx,
                terminated1,
            )
        });
        let orchestrator2 = orchestrator.clone();
        let last_chunk_hash_bridge2 = last_chunk_hash_bridge.clone();
        let chunk_producer_bridge2 = chunk_producer_bridge.clone();
        let chunks_mgr2 = chunks_mgr.clone();
        let terminated2 = terminated.clone();
        let t2 = thread::spawn(move || {
            shard_chain_block_producer(
                me,
                shard_id,
                orchestrator2,
                chunks_mgr2,
                last_chunk_hash_bridge2,
                chunk_producer_bridge2,
                terminated2,
            )
        });

        let t3 = match transmitting_mode {
            WhaleCunksTransmittingMode::NoBroadcast => thread::spawn(move || {}),
            WhaleCunksTransmittingMode::Broadcast(
                chunk_header_request_tx,
                chunk_part_request_tx,
                chunk_header_rx,
                chunk_part_rx,
            ) => {
                let chunks_mgr3 = chunks_mgr.clone();
                let orchestrator3 = orchestrator.clone();
                let terminated3 = terminated.clone();

                thread::spawn(move || {
                    shard_chain_chunks_mgr_worker(
                        chunks_mgr3,
                        orchestrator3,
                        chunk_header_request_tx,
                        chunk_part_request_tx,
                        chunk_header_rx,
                        chunk_part_rx,
                        terminated3,
                    )
                })
            }
        };

        (t1, t2, t3)
    }

    #[test]
    fn test_single_worker() {
        let (finalized_chunk_tx, finalized_chunk_rx) = channel();
        let (chunk_publishing_tx, chunk_publishing_rx) = channel();

        let terminated = Arc::new(RwLock::new(false));
        let chunks_mgr = Arc::new(RwLock::new(ShardChainChunksManager::default()));
        let orchestrator = Arc::new(RwLock::new(TestOrchestrator::new(1)));

        let genesis_hash = hash(&[1u8]);

        let (t1, t2, t3) = spawn_whale(
            0,
            7,
            WhaleCunksTransmittingMode::NoBroadcast,
            &orchestrator,
            finalized_chunk_rx,
            chunk_publishing_tx,
            &terminated,
            &chunks_mgr,
        );

        finalized_chunk_tx.send((genesis_hash, 0, false)).unwrap();

        let mut blocks = wait_for_n(&chunk_publishing_rx, 1, 1000);
        assert_eq!(blocks[0].header.prev_block_hash, genesis_hash);

        let first_block_hash = blocks[0].chunk_hash();
        chunks_mgr.write().unwrap().insert_chunk(first_block_hash, blocks.pop().unwrap());
        finalized_chunk_tx.send((first_block_hash, 1, false)).unwrap();

        let blocks = wait_for_n(&chunk_publishing_rx, 1, 1000);
        assert_eq!(blocks[0].header.prev_block_hash, first_block_hash);

        *terminated.write().unwrap() = true;

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();
    }

    fn test_multiple_workers_common(num_malicious: usize) {
        let terminated = Arc::new(RwLock::new(false));
        let chunks_mgr = Arc::new(RwLock::new(ShardChainChunksManager::default()));
        let orchestrator = Arc::new(RwLock::new(TestOrchestrator::new(1)));

        let mut chunk_publishing_rxs = vec![];
        let mut finalized_chunk_txs = vec![];

        let mut chunk_header_request_rxs = vec![];
        let mut chunk_part_request_rxs = vec![];
        let mut chunk_header_txs = vec![];
        let mut chunk_part_txs = vec![];

        let genesis_hash = hash(&[1u8]);

        let mut join_handles = vec![];

        for me in 0..(4 - num_malicious) {
            let (finalized_chunk_tx, finalized_chunk_rx) = channel();
            let (chunk_publishing_tx, chunk_publishing_rx) = channel();

            let (chunk_header_request_tx, chunk_header_request_rx) = channel();
            let (chunk_part_request_tx, chunk_part_request_rx) = channel();
            let (chunk_header_tx, chunk_header_rx) = channel();
            let (chunk_part_tx, chunk_part_rx) = channel();

            let (t1, t2, t3) = spawn_whale(
                me,
                7,
                WhaleCunksTransmittingMode::Broadcast(
                    chunk_header_request_tx,
                    chunk_part_request_tx,
                    chunk_header_rx,
                    chunk_part_rx,
                ),
                &orchestrator,
                finalized_chunk_rx,
                chunk_publishing_tx,
                &terminated,
                &chunks_mgr,
            );

            finalized_chunk_tx.send((genesis_hash, 0, false)).unwrap();

            join_handles.push(t1);
            join_handles.push(t2);
            join_handles.push(t3);

            chunk_publishing_rxs.push(chunk_publishing_rx);
            finalized_chunk_txs.push(finalized_chunk_tx);

            chunk_header_request_rxs.push(chunk_header_request_rx);
            chunk_part_request_rxs.push(chunk_part_request_rx);
            chunk_header_txs.push(chunk_header_tx);
            chunk_part_txs.push(chunk_part_tx);
        }

        let mut chunks = Arc::new(RwLock::new(HashMap::new()));

        let terminated2 = terminated.clone();
        let chunks2 = chunks.clone();

        let broadcast_thread = thread::spawn(move || {
            let mut header_requests = vec![];
            let mut part_requests = vec![];
            loop {
                if *terminated2.read().unwrap() {
                    break;
                }

                for (chunk_header_request_rx, chunk_header_tx) in
                    chunk_header_request_rxs.iter().zip(chunk_header_txs.iter())
                {
                    for hash in chunk_header_request_rx {
                        header_requests.push((chunk_header_tx, hash));
                    }
                }

                for (chunk_part_request_rx, chunk_part_tx) in
                    chunk_part_request_rxs.iter().zip(chunk_part_txs.iter())
                {
                    for (hash, part_id) in chunk_part_request_rx {
                        part_requests.push((chunk_part_tx, hash, part_id));
                    }
                }
            }
        });

        for chunk_publishing_rx in chunk_publishing_rxs.iter() {
            println!("!!!");
            let mut published_chunks = wait_for_n(chunk_publishing_rx, 1, 1000);
            let chunk_hash = published_chunks[0].chunk_hash();
            chunks.write().unwrap().insert(chunk_hash, published_chunks.pop());
        }

        *terminated.write().unwrap() = true;

        for handle in join_handles {
            handle.join().unwrap();
        }
        broadcast_thread.join();
    }

    #[test]
    fn test_multiple_workers() {
        test_multiple_workers_common(0);
    }
}
