use near_async::messaging::{CanSend, LateBoundSender};
use near_async::test_loop::TestLoopV2;
use near_async::test_loop::data::TestLoopData;
use near_async::test_loop::sender::TestLoopSender;
use near_async::time::Duration;
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::shards_manager_actor::ShardsManagerActor;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives_core::types::BlockHeight;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

use super::state::{SharedState, TestData};

pub struct TestLoopEnv {
    pub test_loop: TestLoopV2,
    pub node_datas: Vec<TestData>,
    pub shared_state: SharedState,
}

impl TestLoopEnv {
    /// Reach block with height `genesis_height + 3`. Check that it can be done
    /// within 5 seconds. Ensure that all clients have block
    /// `genesis_height + 2` and it has all chunks.
    /// Needed because for smaller heights blocks may not get all chunks and/or
    /// approvals.
    pub fn warmup(self) -> Self {
        let Self { mut test_loop, node_datas: datas, shared_state } = self;

        // This may happen if you're calling warmup twice or have set skip_warmup in builder.
        assert!(shared_state.warmup_pending.load(Ordering::Relaxed), "warmup already done");
        shared_state.warmup_pending.store(false, Ordering::Relaxed);

        let client_handle = datas[0].client_sender.actor_handle();
        let genesis_height = test_loop.data.get(&client_handle).client.chain.genesis().height();
        test_loop.run_until(
            |test_loop_data| {
                let client_actor = test_loop_data.get(&client_handle);
                client_actor.client.chain.head().unwrap().height == genesis_height + 4
            },
            Duration::seconds(5),
        );
        for idx in 0..datas.len() {
            let client_handle = datas[idx].client_sender.actor_handle();
            let event = move |test_loop_data: &mut TestLoopData| {
                let client_actor = test_loop_data.get(&client_handle);
                let block =
                    client_actor.client.chain.get_block_by_height(genesis_height + 3).unwrap();
                let num_shards = block.header().chunk_mask().len();
                assert_eq!(block.header().chunk_mask(), vec![true; num_shards]);
            };
            test_loop.send_adhoc_event("assertions".to_owned(), Box::new(event));
        }
        test_loop.run_instant();
        Self { test_loop, node_datas: datas, shared_state }
    }

    pub fn kill_node(&mut self, identifier: &str) {
        self.test_loop.remove_events_with_identifier(identifier);
    }

    /// Used to finish off remaining events that are still in the loop. This can be necessary if the
    /// destructor of some components wait for certain condition to become true. Otherwise, the
    /// destructors may end up waiting forever. This also helps avoid a panic when destructing
    /// TestLoop itself, as it asserts that all events have been handled.
    ///
    /// Returns the test loop data dir, if the caller wishes to reuse it for another test loop.
    pub fn shutdown_and_drain_remaining_events(mut self, timeout: Duration) -> TempDir {
        // State sync dumper is not an Actor, handle stopping separately.
        for node_data in self.node_datas {
            self.test_loop.data.get_mut(&node_data.state_sync_dumper_handle).stop();
        }

        self.test_loop.shutdown_and_drain_remaining_events(timeout);
        self.shared_state.tempdir
    }
}

/// Stores all chunks ever observed on chain. Determines if a chunk can be
/// dropped within a test loop.
///
/// Needed to intercept network messages storing chunk hash only, while
/// interception requires more detailed information like shard id.
#[derive(Default)]
pub struct TestLoopChunksStorage {
    /// Mapping from chunk hashes to headers.
    storage: HashMap<ChunkHash, ShardChunkHeader>,
    /// Minimal chunk height ever observed.
    min_chunk_height: Option<BlockHeight>,
}

impl TestLoopChunksStorage {
    pub fn insert(&mut self, chunk_header: ShardChunkHeader) {
        let chunk_height = chunk_header.height_created();
        self.min_chunk_height = Some(
            self.min_chunk_height
                .map_or(chunk_height, |current_height| current_height.min(chunk_height)),
        );
        self.storage.insert(chunk_header.chunk_hash(), chunk_header);
    }

    pub fn get(&self, chunk_hash: &ChunkHash) -> Option<&ShardChunkHeader> {
        self.storage.get(chunk_hash)
    }

    /// If chunk height is too low, don't drop chunk, allow the chain to warm
    /// up.
    pub fn can_drop_chunk(&self, chunk_header: &ShardChunkHeader) -> bool {
        self.min_chunk_height
            .is_some_and(|min_height| chunk_header.height_created() >= min_height + 3)
    }
}

/// Custom implementation of `Sender` for messages from `Client` to
/// `ShardsManagerActor` that allows to intercept all messages indicating
/// any chunk production and storing all chunks.
pub struct ClientToShardsManagerSender {
    pub sender: Arc<LateBoundSender<TestLoopSender<ShardsManagerActor>>>,
    /// Storage of chunks shared between all test loop nodes.
    pub chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
}

impl CanSend<ShardsManagerRequestFromClient> for ClientToShardsManagerSender {
    fn send(&self, message: ShardsManagerRequestFromClient) {
        // `DistributeEncodedChunk` indicates that a certain chunk was produced.
        if let ShardsManagerRequestFromClient::DistributeEncodedChunk { partial_chunk, .. } =
            &message
        {
            let mut chunks_storage = self.chunks_storage.lock().unwrap();
            chunks_storage.insert(partial_chunk.cloned_header());
        }
        // After maybe storing the chunk, send the message as usual.
        self.sender.send(message);
    }
}
