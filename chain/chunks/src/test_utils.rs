use crate::{Seal, SealsManager, ACCEPTING_SEAL_PERIOD_MS, PAST_SEAL_HEIGHT_HORIZON};
use chrono::Utc;
use near_chain::test_utils::KeyValueRuntime;
use near_chain::types::RuntimeAdapter;
use near_chain::ChainStore;
use near_primitives::block::BlockHeader;
use near_primitives::hash::{self, CryptoHash};
use near_primitives::sharding::ChunkHash;
use near_primitives::types::BlockHeight;
use near_primitives::types::{AccountId, ShardId};
use near_store::Store;
use std::sync::Arc;

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
        let letters = (b'A'..=b'Z')
            .take(12)
            .map(|c| {
                let mut s = String::with_capacity(1);
                s.push(c as char);
                s
            })
            .collect();
        let validators = vec![letters];
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
        let mock_distant_block_hash = mock_distant_block_header.hash.clone();
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
        let mut chain_store = ChainStore::new(store, header.inner_lite.height);
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
