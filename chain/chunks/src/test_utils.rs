use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use actix::MailboxError;
use futures::future::BoxFuture;
use futures::FutureExt;
use near_network::types::MsgRecipient;
use near_primitives::receipt::Receipt;
use near_primitives::test_utils::create_test_signer;
use near_primitives::time::Clock;

use near_chain::test_utils::{KeyValueRuntime, ValidatorSchedule};
use near_chain::types::{EpochManagerAdapter, RuntimeAdapter, Tip};
use near_chain::{Chain, ChainStore};
use near_network::test_utils::MockPeerManagerAdapter;
use near_o11y::WithSpanContext;
use near_primitives::block::BlockHeader;
use near_primitives::hash::{self, CryptoHash};
use near_primitives::merkle::{self, MerklePath};
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunk, PartialEncodedChunk, PartialEncodedChunkPart,
    PartialEncodedChunkV2, ReedSolomonWrapper, ShardChunkHeader,
};
use near_primitives::types::NumShards;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::types::{BlockHeight, MerkleHash};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::Store;

use crate::client::ShardsManagerResponse;
use crate::{
    Seal, SealsManager, ShardsManager, ACCEPTING_SEAL_PERIOD_MS, PAST_SEAL_HEIGHT_HORIZON,
};

pub struct SealsManagerTestFixture {
    pub mock_chunk_producer: AccountId,
    mock_me: Option<AccountId>,
    pub mock_shard_id: ShardId,
    pub mock_chunk_hash: ChunkHash,
    pub mock_parent_hash: CryptoHash,
    pub mock_height: BlockHeight,
    pub mock_distant_block_hash: CryptoHash,
    pub mock_distant_chunk_hash: ChunkHash,
    mock_runtime: Arc<KeyValueRuntime>,
}

impl Default for SealsManagerTestFixture {
    fn default() -> Self {
        let store = near_store::test_utils::create_test_store();
        let mock_runtime = default_runtime();

        let mock_parent_hash = CryptoHash::default();
        let mock_height: BlockHeight = 1;
        let mock_shard_id: ShardId = 0;
        let mock_epoch_id = mock_runtime.get_epoch_id_from_prev_block(&mock_parent_hash).unwrap();
        let mock_chunk_producer =
            mock_runtime.get_chunk_producer(&mock_epoch_id, mock_height, mock_shard_id).unwrap();

        let mock_me: Option<AccountId> = Some("me".parse().unwrap());
        let mock_chunk_hash = ChunkHash(CryptoHash::default());
        let mock_distant_chunk_hash = ChunkHash(hash::hash(b"some_chunk"));

        // Header for some distant block (with a very large height). The only field that
        // is important is the height because this influences the GC cut-off. The hash
        // in the header is used to check that the `SealsManager` seals cache is properly
        // cleared.
        let mock_distant_block_header = BlockHeader::genesis(
            PROTOCOL_VERSION,
            mock_height + PAST_SEAL_HEIGHT_HORIZON + 1,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Clock::utc(),
            Default::default(),
            Default::default(),
            Default::default(),
        );
        let mock_distant_block_hash = *mock_distant_block_header.hash();
        Self::store_block_header(store, mock_distant_block_header);

        Self {
            mock_chunk_producer,
            mock_me,
            mock_shard_id,
            mock_chunk_hash,
            mock_parent_hash,
            mock_height,
            mock_distant_block_hash,
            mock_distant_chunk_hash,
            mock_runtime: Arc::new(mock_runtime),
        }
    }
}

impl SealsManagerTestFixture {
    fn store_block_header(store: Store, header: BlockHeader) {
        let mut chain_store = ChainStore::new(store, header.height(), true);
        let mut update = chain_store.store_update();
        update.save_block_header(header).unwrap();
        update.commit().unwrap();
    }

    pub fn create_seals_manager(&self) -> SealsManager {
        SealsManager::new(self.mock_me.clone(), self.mock_runtime.clone())
    }

    pub fn create_seal(&self, seals_manager: &mut SealsManager) {
        seals_manager
            .get_seal(
                &self.mock_chunk_hash,
                &self.mock_parent_hash,
                self.mock_height,
                self.mock_shard_id,
            )
            .unwrap();
    }

    pub fn create_expired_seal(
        &self,
        seals_manager: &mut SealsManager,
        chunk_hash: &ChunkHash,
        parent_hash: &CryptoHash,
        height: BlockHeight,
    ) {
        let seal =
            seals_manager.get_seal(chunk_hash, parent_hash, height, self.mock_shard_id).unwrap();
        let demur = match seal {
            Seal::Active(demur) => demur,
            Seal::Past => panic!("Active demur expected"),
        };

        let d = chrono::Duration::milliseconds(2 * ACCEPTING_SEAL_PERIOD_MS);
        demur.sent = demur.sent - d;
    }
}

pub struct ChunkTestFixture {
    pub mock_runtime: Arc<KeyValueRuntime>,
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
        Self::new(false)
    }
}

impl ChunkTestFixture {
    pub fn new(orphan_chunk: bool) -> Self {
        // 12 validators, 3 shards, 4 validators per shard
        Self::new_with_runtime(orphan_chunk, Arc::new(default_runtime()))
    }

    pub fn new_with_all_shards_tracking() -> Self {
        let mut runtime = default_runtime();
        runtime.set_tracks_all_shards(true);
        Self::new_with_runtime(false, Arc::new(runtime))
    }

    // Create a ChunkTestFixture to test chunk only producers
    pub fn new_with_chunk_only_producers() -> Self {
        let store = near_store::test_utils::create_test_store();
        // 3 block producer. 1 block producer + 2 chunk only producer per shard
        // This setup ensures that the chunk producer
        let vs = make_validators(6, 2, 3);
        let mock_runtime =
            Arc::new(KeyValueRuntime::new_with_validators_and_no_gc(store.clone(), vs, 5, false));
        Self::new_with_runtime(false, mock_runtime)
    }

    pub fn new_with_runtime(orphan_chunk: bool, mock_runtime: Arc<KeyValueRuntime>) -> Self {
        let mock_network = Arc::new(MockPeerManagerAdapter::default());
        let mock_client_adapter = Arc::new(MockClientAdapterForShardsManager::default());

        let data_parts = mock_runtime.num_data_parts();
        let parity_parts = mock_runtime.num_total_parts() - data_parts;
        let mut rs = ReedSolomonWrapper::new(data_parts, parity_parts);
        let mock_ancestor_hash = CryptoHash::default();
        // generate a random block hash for the block at height 1
        let (mock_parent_hash, mock_height) =
            if orphan_chunk { (CryptoHash::hash_bytes(&[]), 2) } else { (mock_ancestor_hash, 1) };
        // setting this to 2 instead of 0 so that when chunk producers
        let mock_shard_id: ShardId = 0;
        let mock_epoch_id = mock_runtime.get_epoch_id_from_prev_block(&mock_ancestor_hash).unwrap();
        let mock_chunk_producer =
            mock_runtime.get_chunk_producer(&mock_epoch_id, mock_height, mock_shard_id).unwrap();
        let signer = create_test_signer(mock_chunk_producer.as_str());
        let validators: Vec<_> = mock_runtime
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
                    let tracks_shard = mock_runtime.cares_about_shard(
                        Some(*v),
                        &mock_ancestor_hash,
                        mock_shard_id,
                        false,
                    ) || mock_runtime.will_care_about_shard(
                        Some(*v),
                        &mock_ancestor_hash,
                        mock_shard_id,
                        false,
                    );
                    tracks_shard
                }
            })
            .cloned()
            .unwrap()
            .clone();
        let mock_chunk_part_owner = validators
            .into_iter()
            .find(|v| v != &mock_chunk_producer && v != &mock_shard_tracker)
            .unwrap();

        let receipts = Vec::new();
        let shard_layout = mock_runtime.get_shard_layout(&EpochId::default()).unwrap();
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
                mock_runtime.get_part_owner(&mock_epoch_id, *p).unwrap() == mock_chunk_part_owner
            })
            .collect();
        let encoded_chunk = mock_chunk.create_partial_encoded_chunk(
            all_part_ords.clone(),
            Vec::new(),
            &mock_merkle_paths,
        );
        let chain_store = ChainStore::new(mock_runtime.store().clone(), 0, true);

        ChunkTestFixture {
            mock_runtime,
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

/// `num_bp` is number of block producers
/// `num_cp` is number of chunk producers per shard
fn make_validators(
    num_bp: usize,
    num_cp_per_shard: usize,
    num_shards: NumShards,
) -> ValidatorSchedule {
    let n = num_bp + num_cp_per_shard * num_shards as usize;
    if n > 26 {
        panic!("I can't make that many validators!");
    }

    let mut accounts: Vec<_> =
        ('a'..='z').take(n).map(|c| AccountId::try_from(format!("test_{}", c)).unwrap()).collect();

    let bp = vec![accounts.drain(..num_bp).collect()];
    let cp = vec![(0..num_shards).map(|_| accounts.drain(..num_cp_per_shard).collect()).collect()];
    ValidatorSchedule::new()
        .num_shards(3)
        .block_producers_per_epoch(bp)
        .validator_groups(3)
        .chunk_only_producers_per_epoch_per_shard(cp)
}

// 12 validators, 3 shards, 4 validators per shard
fn default_runtime() -> KeyValueRuntime {
    let store = near_store::test_utils::create_test_store();
    // 12 validators, 3 shards, 4 validators per shard
    let vs = make_validators(12, 0, 3);
    KeyValueRuntime::new_with_validators(store.clone(), vs, 5)
}
// Mocked `PeerManager` adapter, has a queue of `PeerManagerMessageRequest` messages.
#[derive(Default)]
pub struct MockClientAdapterForShardsManager {
    pub requests: Arc<RwLock<VecDeque<ShardsManagerResponse>>>,
}

impl MsgRecipient<WithSpanContext<ShardsManagerResponse>> for MockClientAdapterForShardsManager {
    fn send(
        &self,
        msg: WithSpanContext<ShardsManagerResponse>,
    ) -> BoxFuture<'static, Result<(), MailboxError>> {
        self.do_send(msg);
        futures::future::ok(()).boxed()
    }

    fn do_send(&self, msg: WithSpanContext<ShardsManagerResponse>) {
        self.requests.write().unwrap().push_back(msg.msg);
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
