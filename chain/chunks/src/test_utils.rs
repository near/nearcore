use near_async::messaging::CanSend;
use near_chain::types::{EpochManagerAdapter, Tip};
use near_chain::{Chain, ChainStore};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::test_utils::setup_epoch_manager_with_block_and_chunk_producers;
use near_epoch_manager::EpochManagerHandle;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::test_utils::MockPeerManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{self, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{
    EncodedShardChunk, PartialEncodedChunk, PartialEncodedChunkPart, PartialEncodedChunkV2,
    ReedSolomonWrapper, ShardChunkHeader,
};
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::MerkleHash;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::test_utils::create_test_store;
use near_store::Store;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};

use crate::adapter::ShardsManagerRequestFromClient;
use crate::client::ShardsManagerResponse;
use crate::ShardsManager;

/// Deprecated. Use `MockChainForShardsManager`.
pub struct ChunkTestFixture {
    pub store: Store,
    pub epoch_manager: EpochManagerHandle,
    pub shard_tracker: ShardTracker,
    pub mock_network: Arc<MockPeerManagerAdapter>,
    pub mock_client_adapter: Arc<MockClientAdapterForShardsManager>,
    pub chain_store: ChainStore,
    pub all_part_ords: Vec<u64>,
    pub mock_part_ords: Vec<u64>,
    pub mock_merkle_paths: Vec<MerklePath>,
    pub mock_outgoing_receipts: Vec<Receipt>,
    pub mock_encoded_chunk: EncodedShardChunk,
    pub mock_chunk_part_owner: AccountId,
    pub mock_shard_tracker: AccountId,
    pub mock_chunk_header: ShardChunkHeader,
    pub mock_chunk_parts: Vec<PartialEncodedChunkPart>,
    pub mock_chain_head: Tip,
    pub rs: ReedSolomonWrapper,
}

impl Default for ChunkTestFixture {
    fn default() -> Self {
        Self::new(false, 3, 6, 6, true)
    }
}

impl ChunkTestFixture {
    pub fn new(
        orphan_chunk: bool,
        num_shards: u64,
        num_block_producers: usize,
        num_chunk_only_producers: usize,
        track_all_shards: bool,
    ) -> Self {
        if num_shards > num_block_producers as u64 {
            panic!("Invalid setup: there must be at least as many block producers as shards");
        }
        let store = create_test_store();
        let epoch_manager = setup_epoch_manager_with_block_and_chunk_producers(
            store.clone(),
            (0..num_block_producers).map(|i| format!("test_bp_{}", i).parse().unwrap()).collect(),
            (0..num_chunk_only_producers)
                .map(|i| format!("test_cp_{}", i).parse().unwrap())
                .collect(),
            num_shards,
            2,
        );
        let epoch_manager = epoch_manager.into_handle();
        let shard_tracker = ShardTracker::new(
            if track_all_shards { TrackedConfig::AllShards } else { TrackedConfig::new_empty() },
            Arc::new(epoch_manager.clone()),
        );
        let mock_network = Arc::new(MockPeerManagerAdapter::default());
        let mock_client_adapter = Arc::new(MockClientAdapterForShardsManager::default());

        let data_parts = epoch_manager.num_data_parts();
        let parity_parts = epoch_manager.num_total_parts() - data_parts;
        let mut rs = ReedSolomonWrapper::new(data_parts, parity_parts);
        let mock_ancestor_hash = CryptoHash::default();
        // generate a random block hash for the block at height 1
        let (mock_parent_hash, mock_height) =
            if orphan_chunk { (CryptoHash::hash_bytes(&[]), 2) } else { (mock_ancestor_hash, 1) };
        // setting this to 2 instead of 0 so that when chunk producers
        let mock_shard_id: ShardId = 0;
        let mock_epoch_id =
            epoch_manager.get_epoch_id_from_prev_block(&mock_ancestor_hash).unwrap();
        let mock_chunk_producer =
            epoch_manager.get_chunk_producer(&mock_epoch_id, mock_height, mock_shard_id).unwrap();
        let signer = create_test_signer(mock_chunk_producer.as_str());
        let validators: Vec<_> = epoch_manager
            .get_epoch_block_producers_ordered(&EpochId::default(), &CryptoHash::default())
            .unwrap()
            .into_iter()
            .map(|v| v.0.account_id().clone())
            .collect();
        let mock_shard_tracker = validators
            .iter()
            .find(|v| {
                if v == &&mock_chunk_producer {
                    false
                } else {
                    let tracks_shard = shard_tracker.care_about_shard(
                        Some(*v),
                        &mock_ancestor_hash,
                        mock_shard_id,
                        false,
                    ) || shard_tracker.will_care_about_shard(
                        Some(*v),
                        &mock_ancestor_hash,
                        mock_shard_id,
                        false,
                    );
                    tracks_shard
                }
            })
            .cloned()
            .unwrap();
        let mock_chunk_part_owner = validators
            .into_iter()
            .find(|v| v != &mock_chunk_producer && v != &mock_shard_tracker)
            .unwrap();

        let receipts = Vec::new();
        let shard_layout = epoch_manager.get_shard_layout(&EpochId::default()).unwrap();
        let receipts_hashes = Chain::build_receipts_hashes(&receipts, &shard_layout);
        let (receipts_root, _) = merkle::merklize(&receipts_hashes);
        let (mock_chunk, mock_merkle_paths) = ShardsManager::create_encoded_shard_chunk(
            mock_parent_hash,
            Default::default(),
            Default::default(),
            mock_height,
            mock_shard_id,
            0,
            1000,
            0,
            Vec::new(),
            Vec::new(),
            &receipts,
            receipts_root,
            MerkleHash::default(),
            &signer,
            &mut rs,
            PROTOCOL_VERSION,
        )
        .unwrap();

        let all_part_ords: Vec<u64> =
            (0..mock_chunk.content().parts.len()).map(|p| p as u64).collect();
        let mock_part_ords = all_part_ords
            .iter()
            .copied()
            .filter(|p| {
                epoch_manager.get_part_owner(&mock_epoch_id, *p).unwrap() == mock_chunk_part_owner
            })
            .collect();
        let encoded_chunk = mock_chunk.create_partial_encoded_chunk(
            all_part_ords.clone(),
            Vec::new(),
            &mock_merkle_paths,
        );
        let chain_store = ChainStore::new(store.clone(), 0, true);

        ChunkTestFixture {
            store,
            epoch_manager,
            shard_tracker,
            mock_network,
            mock_client_adapter,
            chain_store,
            all_part_ords,
            mock_part_ords,
            mock_encoded_chunk: mock_chunk,
            mock_merkle_paths,
            mock_outgoing_receipts: receipts,
            mock_chunk_part_owner,
            mock_shard_tracker,
            mock_chunk_header: encoded_chunk.cloned_header(),
            mock_chunk_parts: encoded_chunk.parts().to_vec(),
            mock_chain_head: Tip {
                height: 0,
                last_block_hash: CryptoHash::default(),
                prev_block_hash: CryptoHash::default(),
                epoch_id: EpochId::default(),
                next_epoch_id: EpochId::default(),
            },
            rs,
        }
    }

    pub fn make_partial_encoded_chunk(&self, part_ords: &[u64]) -> PartialEncodedChunk {
        let parts = part_ords
            .iter()
            .copied()
            .flat_map(|ord| self.mock_chunk_parts.iter().find(|part| part.part_ord == ord))
            .cloned()
            .collect();
        PartialEncodedChunk::V2(PartialEncodedChunkV2 {
            header: self.mock_chunk_header.clone(),
            parts,
            receipts: Vec::new(),
        })
    }

    pub fn count_chunk_completion_messages(&self) -> usize {
        let mut chunks_completed = 0;
        while let Some(message) = self.mock_client_adapter.pop() {
            if let ShardsManagerResponse::ChunkCompleted { .. } = message {
                chunks_completed += 1;
            }
        }
        chunks_completed
    }

    pub fn count_chunk_ready_for_inclusion_messages(&self) -> usize {
        let mut chunks_ready = 0;
        while let Some(message) = self.mock_client_adapter.pop() {
            if let ShardsManagerResponse::ChunkHeaderReadyForInclusion { .. } = message {
                chunks_ready += 1;
            }
        }
        chunks_ready
    }
}

/// Gets the Tip from the block hash and the EpochManager.
pub fn tip(epoch_manager: &dyn EpochManagerAdapter, last_block_hash: CryptoHash) -> Tip {
    let block_info = epoch_manager.get_block_info(&last_block_hash).unwrap();
    let epoch_id = epoch_manager.get_epoch_id(&last_block_hash).unwrap();
    let next_epoch_id = epoch_manager.get_next_epoch_id(&last_block_hash).unwrap();
    Tip {
        height: block_info.height(),
        last_block_hash,
        prev_block_hash: *block_info.prev_hash(),
        epoch_id,
        next_epoch_id,
    }
}

/// Returns the tip representing the genesis block for testing.
pub fn default_tip() -> Tip {
    Tip {
        height: 0,
        last_block_hash: CryptoHash::default(),
        prev_block_hash: CryptoHash::default(),
        epoch_id: EpochId::default(),
        next_epoch_id: EpochId::default(),
    }
}

// Mocked `PeerManager` adapter, has a queue of `PeerManagerMessageRequest` messages.
#[derive(Default)]
pub struct MockClientAdapterForShardsManager {
    pub requests: Arc<RwLock<VecDeque<ShardsManagerResponse>>>,
}

impl CanSend<ShardsManagerResponse> for MockClientAdapterForShardsManager {
    fn send(&self, msg: ShardsManagerResponse) {
        self.requests.write().unwrap().push_back(msg);
    }
}

impl MockClientAdapterForShardsManager {
    pub fn pop(&self) -> Option<ShardsManagerResponse> {
        self.requests.write().unwrap().pop_front()
    }
    pub fn pop_most_recent(&self) -> Option<ShardsManagerResponse> {
        self.requests.write().unwrap().pop_back()
    }
}

// Allows ShardsManagerActor-like behavior, except without having to spawn an actor,
// and without having to manually route ShardsManagerRequest messages. This only works
// for single-threaded (synchronous) tests. The ShardsManager is immediately called
// upon receiving a ShardsManagerRequest message.
#[derive(Clone)]
pub struct SynchronousShardsManagerAdapter {
    // Need a mutex here even though we only support single-threaded tests, because
    // MsgRecipient requires Sync.
    pub shards_manager: Arc<Mutex<ShardsManager>>,
}

impl CanSend<ShardsManagerRequestFromClient> for SynchronousShardsManagerAdapter {
    fn send(&self, msg: ShardsManagerRequestFromClient) {
        let mut shards_manager = self.shards_manager.lock().unwrap();
        shards_manager.handle_client_request(msg);
    }
}

impl CanSend<ShardsManagerRequestFromNetwork> for SynchronousShardsManagerAdapter {
    fn send(&self, msg: ShardsManagerRequestFromNetwork) {
        let mut shards_manager = self.shards_manager.lock().unwrap();
        shards_manager.handle_network_request(msg);
    }
}

impl SynchronousShardsManagerAdapter {
    pub fn new(shards_manager: ShardsManager) -> Self {
        Self { shards_manager: Arc::new(Mutex::new(shards_manager)) }
    }
}
