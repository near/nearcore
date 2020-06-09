use std::sync::Arc;

use chrono::Utc;

use near_chain::test_utils::KeyValueRuntime;
use near_chain::types::RuntimeAdapter;
use near_chain::ChainStore;
use near_crypto::KeyType;
use near_network::test_utils::MockNetworkAdapter;
use near_primitives::block::BlockHeader;
use near_primitives::hash::{self, CryptoHash};
use near_primitives::merkle;
use near_primitives::sharding::{
    ChunkHash, PartialEncodedChunk, PartialEncodedChunkPart, ReedSolomonWrapper, ShardChunkHeader,
};
use near_primitives::types::{AccountId, ShardId};
use near_primitives::types::{BlockHeight, MerkleHash};
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::Store;

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
        // 12 validators, 3 shards => 4 validators per shard
        let validators = make_validators(12);
        let mock_runtime =
            KeyValueRuntime::new_with_validators(Arc::clone(&store), validators, 1, 3, 5);

        let mock_parent_hash = CryptoHash::default();
        let mock_height: BlockHeight = 1;
        let mock_shard_id: ShardId = 0;
        let mock_epoch_id = mock_runtime.get_epoch_id_from_prev_block(&mock_parent_hash).unwrap();
        let mock_chunk_producer =
            mock_runtime.get_chunk_producer(&mock_epoch_id, mock_height, mock_shard_id).unwrap();

        let mock_me: Option<AccountId> = Some("me".to_string());
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
            Utc::now(),
            Default::default(),
            Default::default(),
            Default::default(),
        );
        let mock_distant_block_hash = mock_distant_block_header.hash().clone();
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
    fn store_block_header(store: Arc<Store>, header: BlockHeader) {
        let mut chain_store = ChainStore::new(store, header.height());
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

pub struct ChunkForwardingTestFixture {
    pub mock_runtime: Arc<KeyValueRuntime>,
    pub mock_network: Arc<MockNetworkAdapter>,
    pub chain_store: ChainStore,
    pub mock_part_ords: Vec<u64>,
    pub mock_chunk_part_owner: AccountId,
    pub mock_shard_tracker: AccountId,
    pub mock_chunk_header: ShardChunkHeader,
    pub mock_chunk_parts: Vec<PartialEncodedChunkPart>,
    pub rs: ReedSolomonWrapper,
}

impl Default for ChunkForwardingTestFixture {
    fn default() -> Self {
        let store = near_store::test_utils::create_test_store();
        // 12 validators, 3 shards => 4 validators per shard
        let validators = make_validators(12);
        let mock_runtime = Arc::new(KeyValueRuntime::new_with_validators(
            Arc::clone(&store),
            validators.clone(),
            1,
            3,
            5,
        ));
        let mock_network = Arc::new(MockNetworkAdapter::default());

        let data_parts = mock_runtime.num_data_parts();
        let parity_parts = mock_runtime.num_total_parts() - data_parts;
        let mut rs = ReedSolomonWrapper::new(data_parts, parity_parts);
        let mock_parent_hash = CryptoHash::default();
        let mock_height: BlockHeight = 1;
        let mock_shard_id: ShardId = 0;
        let mock_epoch_id = mock_runtime.get_epoch_id_from_prev_block(&mock_parent_hash).unwrap();
        let mock_chunk_producer =
            mock_runtime.get_chunk_producer(&mock_epoch_id, mock_height, mock_shard_id).unwrap();
        let signer = InMemoryValidatorSigner::from_seed(
            &mock_chunk_producer,
            KeyType::ED25519,
            &mock_chunk_producer,
        );
        let mock_shard_tracker = validators
            .iter()
            .flatten()
            .find(|v| {
                if *v == &mock_chunk_producer {
                    false
                } else {
                    let tracks_shard = mock_runtime.cares_about_shard(
                        Some(*v),
                        &mock_parent_hash,
                        mock_shard_id,
                        false,
                    ) || mock_runtime.will_care_about_shard(
                        Some(*v),
                        &mock_parent_hash,
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
            .flatten()
            .find(|v| v != &mock_chunk_producer && v != &mock_shard_tracker)
            .unwrap();

        let mut producer_shard_manager = ShardsManager::new(
            Some(mock_chunk_producer.clone()),
            mock_runtime.clone(),
            mock_network.clone(),
        );
        let receipts = Vec::new();
        let receipts_hashes = mock_runtime.build_receipts_hashes(&receipts);
        let (receipts_root, _) = merkle::merklize(&receipts_hashes);
        let (mock_chunk, mock_merkles) = producer_shard_manager
            .create_encoded_shard_chunk(
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
            )
            .unwrap();

        let all_part_ords: Vec<u64> =
            (0..mock_chunk.content.parts.len()).map(|p| p as u64).collect();
        let mock_part_ords = all_part_ords
            .iter()
            .copied()
            .filter(|p| {
                mock_runtime.get_part_owner(&mock_parent_hash, *p).unwrap() == mock_chunk_part_owner
            })
            .collect();
        let encoded_chunk = mock_chunk.create_partial_encoded_chunk(
            all_part_ords.clone(),
            Vec::new(),
            &mock_merkles,
        );
        let chain_store = ChainStore::new(store, 0);

        ChunkForwardingTestFixture {
            mock_runtime,
            mock_network,
            chain_store,
            mock_part_ords,
            mock_chunk_part_owner,
            mock_shard_tracker,
            mock_chunk_header: encoded_chunk.header,
            mock_chunk_parts: encoded_chunk.parts,
            rs,
        }
    }
}

impl ChunkForwardingTestFixture {
    pub fn make_partial_encoded_chunk(&self, part_ords: &[u64]) -> PartialEncodedChunk {
        let parts = part_ords
            .iter()
            .copied()
            .flat_map(|ord| self.mock_chunk_parts.iter().find(|part| part.part_ord == ord))
            .cloned()
            .collect();
        PartialEncodedChunk { header: self.mock_chunk_header.clone(), parts, receipts: Vec::new() }
    }
}

fn make_validators(n: usize) -> Vec<Vec<AccountId>> {
    if n > 26 {
        panic!("I can't make that many validators!");
    }

    let letters = (b'A'..=b'Z')
        .take(n)
        .map(|c| {
            let mut s = String::with_capacity(1);
            s.push(c as char);
            s
        })
        .collect();

    vec![letters]
}
