use crate::types::RuntimeAdapter;
use borsh::BorshDeserialize;
use enum_map::Enum;
use near_async::time::{Clock, Duration, Instant};
use near_chain_configs::GenesisConfig;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::borsh;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::epoch_sync::EpochSyncProof;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::get_block_shard_uid_rev;
use near_primitives::sharding::{ChunkHash, PartialEncodedChunk, ShardChunk, StateSyncInfo};
use near_primitives::state_sync::{ShardStateSyncResponseHeader, StateHeaderKey, StatePartKey};
use near_primitives::transaction::ExecutionOutcomeWithProof;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, BlockHeight, EpochId};
use near_primitives::utils::{get_block_shard_id_rev, get_outcome_id_block_hash_rev};
use near_store::db::refcount;
use near_store::{DBCol, Store, TrieChanges};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use strum::IntoEnumIterator;
use tracing::warn;
use validate::StoreValidatorError;

mod validate;

pub struct StoreValidatorCache {
    head: BlockHeight,
    header_head: BlockHeight,
    tail: BlockHeight,
    chunk_tail: BlockHeight,
    block_heights_less_tail: Vec<CryptoHash>,

    tx_refcount: HashMap<CryptoHash, u64>,
    receipt_refcount: HashMap<CryptoHash, u64>,
    block_refcount: HashMap<CryptoHash, u64>,
    genesis_blocks: Vec<CryptoHash>,
}

impl StoreValidatorCache {
    fn new() -> Self {
        Self {
            head: 0,
            header_head: 0,
            tail: 0,
            chunk_tail: 0,
            block_heights_less_tail: vec![],
            tx_refcount: HashMap::new(),
            receipt_refcount: HashMap::new(),
            block_refcount: HashMap::new(),
            genesis_blocks: vec![],
        }
    }
}

#[derive(Debug)]
pub struct ErrorMessage {
    pub col: String,
    pub key: String,
    pub err: StoreValidatorError,
}

pub struct StoreValidator {
    me: Option<AccountId>,
    config: GenesisConfig,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    store: Store,
    inner: StoreValidatorCache,
    timeout: Option<i64>,
    start_time: Instant,
    pub is_archival: bool,
    // If present, the node was bootstrapped with epoch sync, and this block height
    // represents the first block of the target epoch that we epoch synced to.
    epoch_sync_boundary: Option<BlockHeight>,

    pub errors: Vec<ErrorMessage>,
    tests: u64,
}

impl StoreValidator {
    pub fn new(
        me: Option<AccountId>,
        config: GenesisConfig,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        runtime: Arc<dyn RuntimeAdapter>,
        store: Store,
        is_archival: bool,
    ) -> Self {
        let epoch_sync_boundary = store
            .get_ser::<EpochSyncProof>(DBCol::EpochSyncProof, &[])
            .expect("Store IO error when getting EpochSyncProof")
            .map(|epoch_sync_proof| {
                epoch_sync_proof.into_v1().current_epoch.first_block_header_in_epoch.height()
            });
        StoreValidator {
            me,
            config,
            epoch_manager,
            shard_tracker,
            runtime,
            store: store,
            inner: StoreValidatorCache::new(),
            timeout: None,
            start_time: Clock::real().now(),
            is_archival,
            epoch_sync_boundary,
            errors: vec![],
            tests: 0,
        }
    }
    pub fn set_timeout(&mut self, timeout: i64) {
        self.timeout = Some(timeout)
    }
    pub fn is_failed(&self) -> bool {
        self.tests == 0 || !self.errors.is_empty()
    }
    pub fn num_failed(&self) -> u64 {
        self.errors.len() as u64
    }
    pub fn tests_done(&self) -> u64 {
        self.tests
    }
    fn process_error<K: std::fmt::Debug>(&mut self, err: StoreValidatorError, key: K, col: DBCol) {
        self.errors.push(ErrorMessage { key: format!("{key:?}"), col: col.to_string(), err })
    }
    fn validate_col(&mut self, col: DBCol) -> Result<(), StoreValidatorError> {
        for item in self.store.clone().iter_raw_bytes(col) {
            let (key, value) = item?;
            let key_ref = key.as_ref();
            let value_ref = value.as_ref();
            match col {
                DBCol::BlockHeader => {
                    let block_hash = CryptoHash::try_from(key_ref)?;
                    let header = BlockHeader::try_from_slice(value_ref)?;
                    // Block Header Hash is valid
                    self.check(&validate::block_header_hash_validity, &block_hash, &header, col);
                    // Block Header Height is valid
                    self.check(&validate::block_header_height_validity, &block_hash, &header, col);
                    // Block Header can be indexed by Height
                    self.check(&validate::header_hash_indexed_by_height, &block_hash, &header, col);
                }
                DBCol::Block => {
                    let block_hash = CryptoHash::try_from(key_ref)?;
                    let block = Block::try_from_slice(value_ref)?;
                    // Block Hash is valid
                    self.check(&validate::block_hash_validity, &block_hash, &block, col);
                    // Block Height is valid
                    self.check(&validate::block_height_validity, &block_hash, &block, col);
                    // Block can be indexed by its Height
                    self.check(&validate::block_indexed_by_height, &block_hash, &block, col);
                    // Block Header for current Block exists
                    self.check(&validate::block_header_exists, &block_hash, &block, col);
                    // Chunks for current Block exist
                    self.check(&validate::block_chunks_exist, &block_hash, &block, col);
                    // Chunks for current Block have Height Created not higher than Block Height
                    self.check(&validate::block_chunks_height_validity, &block_hash, &block, col);
                    // BlockInfo for current Block exists
                    self.check(&validate::block_info_exists, &block_hash, &block, col);
                    // EpochInfo for current Epoch id of Block exists
                    self.check(&validate::block_epoch_exists, &block_hash, &block, col);
                    // Increase Block Refcount
                    self.check(&validate::block_increment_refcount, &block_hash, &block, col);
                }
                DBCol::BlockHeight => {
                    let height = BlockHeight::try_from_slice(key_ref)?;
                    let hash = CryptoHash::try_from(value_ref)?;
                    // Block on the Canonical Chain is stored properly
                    self.check(&validate::canonical_header_validity, &height, &hash, col);
                    // If prev Block exists, it's also on the Canonical Chain and
                    // there are no Blocks in range (prev_height, height) on the Canonical Chain
                    self.check(&validate::canonical_prev_block_validity, &height, &hash, col);
                }
                DBCol::Chunks => {
                    let chunk_hash = ChunkHash::try_from_slice(key_ref)?;
                    let shard_chunk = ShardChunk::try_from_slice(value_ref)?;
                    // Chunk Hash is valid
                    self.check(&validate::chunk_hash_validity, &chunk_hash, &shard_chunk, col);
                    // Chunk Height Created is not lower than Chunk Tail
                    self.check(&validate::chunk_tail_validity, &chunk_hash, &shard_chunk, col);
                    // ShardChunk can be indexed by Height
                    self.check(
                        &validate::chunk_indexed_by_height_created,
                        &chunk_hash,
                        &shard_chunk,
                        col,
                    );
                    // Check that all Txs in Chunk exist
                    self.check(&validate::chunk_tx_exists, &chunk_hash, &shard_chunk, col);
                }
                DBCol::ChunkExtra => {
                    let (block_hash, shard_uid) = get_block_shard_uid_rev(key_ref)?;
                    let chunk_extra = ChunkExtra::try_from_slice(value_ref)?;
                    self.check(
                        &validate::chunk_extra_block_exists,
                        &(block_hash, shard_uid),
                        &chunk_extra,
                        col,
                    );
                }
                DBCol::TrieChanges => {
                    let (block_hash, shard_uid) = get_block_shard_uid_rev(key_ref)?;
                    let trie_changes = TrieChanges::try_from_slice(value_ref)?;
                    // ShardChunk should exist for current TrieChanges
                    self.check(
                        &validate::trie_changes_chunk_extra_exists,
                        &(block_hash, shard_uid),
                        &trie_changes,
                        col,
                    );
                }
                DBCol::ChunkHashesByHeight => {
                    let height = BlockHeight::try_from_slice(key_ref)?;
                    let chunk_hashes = HashSet::<ChunkHash>::try_from_slice(value_ref)?;
                    // ShardChunk which can be indexed by Height exists
                    self.check(&validate::chunk_of_height_exists, &height, &chunk_hashes, col);
                }
                DBCol::HeaderHashesByHeight => {
                    let height = BlockHeight::try_from_slice(key_ref)?;
                    let header_hashes = HashSet::<CryptoHash>::try_from_slice(value_ref)?;
                    // Headers which can be indexed by Height exists
                    self.check(
                        &validate::header_hash_of_height_exists,
                        &height,
                        &header_hashes,
                        col,
                    );
                }
                DBCol::OutcomeIds => {
                    let (block_hash, _) = get_block_shard_id_rev(key_ref)?;
                    let outcome_ids = Vec::<CryptoHash>::try_from_slice(value_ref)?;
                    // TransactionResultForBlock should exist for outcome ID and block hash
                    self.check(
                        &validate::outcome_by_outcome_id_exists,
                        &block_hash,
                        &outcome_ids,
                        col,
                    );
                    // Block which can be indexed by Outcome block_hash exists
                    self.check(&validate::outcome_id_block_exists, &block_hash, &outcome_ids, col);
                }
                DBCol::PartialChunks => {
                    let chunk_hash = ChunkHash::try_from_slice(key_ref)?;
                    let shard_chunk = PartialEncodedChunk::try_from_slice(value_ref)?;
                    // Receipts column contain exactly the receipts from PartialEncodedChunk.
                    self.check(
                        &validate::partial_chunk_receipts_exist_in_receipts,
                        &chunk_hash,
                        &shard_chunk,
                        col,
                    );
                }
                DBCol::TransactionResultForBlock => {
                    let (outcome_id, block_hash) = get_outcome_id_block_hash_rev(key_ref)?;
                    let outcome = <ExecutionOutcomeWithProof>::try_from_slice(value_ref)?;
                    // Outcome is reachable in ColOutcomesByBlockHash
                    self.check(
                        &validate::outcome_indexed_by_block_hash,
                        &(outcome_id, block_hash),
                        &outcome,
                        col,
                    );
                }
                DBCol::StateDlInfos => {
                    let block_hash = CryptoHash::try_from(key_ref)?;
                    let state_sync_info = StateSyncInfo::try_from_slice(value_ref)?;
                    // StateSyncInfo is valid
                    self.check(
                        &validate::state_sync_info_valid,
                        &block_hash,
                        &state_sync_info,
                        col,
                    );
                    // Block which can be indexed by StateSyncInfo exists
                    self.check(
                        &validate::state_sync_info_block_exists,
                        &block_hash,
                        &state_sync_info,
                        col,
                    );
                }
                DBCol::BlockInfo => {
                    let block_hash = CryptoHash::try_from(key_ref)?;
                    let block_info = BlockInfo::try_from_slice(value_ref)?;
                    // Block which can be indexed by BlockInfo exists
                    self.check(
                        &validate::block_info_block_header_exists,
                        &block_hash,
                        &block_info,
                        col,
                    );
                }
                DBCol::EpochInfo => {
                    if key_ref != AGGREGATOR_KEY {
                        let epoch_id = EpochId::try_from_slice(key_ref)?;
                        let epoch_info = EpochInfo::try_from_slice(value_ref)?;
                        // Epoch should exist
                        self.check(&validate::epoch_validity, &epoch_id, &epoch_info, col);
                    }
                }
                DBCol::Transactions => {
                    let (_value, rc) = refcount::decode_value_with_rc(value_ref);
                    let tx_hash = CryptoHash::try_from(key_ref)?;
                    self.check(&validate::tx_refcount, &tx_hash, &(rc as u64), col);
                }
                DBCol::Receipts => {
                    let (_value, rc) = refcount::decode_value_with_rc(value_ref);
                    let receipt_id = CryptoHash::try_from(key_ref)?;
                    self.check(&validate::receipt_refcount, &receipt_id, &(rc as u64), col);
                }
                DBCol::BlockRefCount => {
                    let block_hash = CryptoHash::try_from(key_ref)?;
                    let refcount = u64::try_from_slice(value_ref)?;
                    self.check(&validate::block_refcount, &block_hash, &refcount, col);
                }
                DBCol::StateHeaders => {
                    let key = StateHeaderKey::try_from_slice(key_ref)?;
                    let header = ShardStateSyncResponseHeader::try_from_slice(value_ref)?;
                    self.check(&validate::state_header_block_exists, &key, &header, col);
                }
                DBCol::StateParts => {
                    let key = StatePartKey::try_from_slice(key_ref)?;
                    self.check(&validate::state_part_header_exists, &key, value_ref, col);
                }
                _ => {}
            }
            if let Some(timeout) = self.timeout {
                if self.start_time.elapsed() > Duration::milliseconds(timeout) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub fn validate(&mut self) {
        self.start_time = Clock::real().now();

        // Init checks
        // Check Head-Tail validity and fill cache with their values
        if let Err(e) = validate::head_tail_validity(self) {
            self.process_error(e, "HEAD / HEADER_HEAD / TAIL / CHUNK_TAIL", DBCol::BlockMisc)
        }

        // Main loop
        for col in DBCol::iter() {
            if let Err(e) = self.validate_col(col) {
                self.process_error(e, col.to_string(), col)
            }
            if let Some(timeout) = self.timeout {
                if self.start_time.elapsed() > Duration::milliseconds(timeout) {
                    warn!(target: "adversary", "Store validator hit timeout at {col} ({}/{})", col.into_usize(), DBCol::LENGTH);
                    return;
                }
            }
        }
        if let Some(timeout) = self.timeout {
            // We didn't complete all Column checks and cannot do final checks, returning here
            if self.start_time.elapsed() > Duration::milliseconds(timeout) {
                warn!(target: "adversary", "Store validator hit timeout before final checks");
                return;
            }
        }

        // Final checks
        // There is no more than one Block which Height is lower than Tail and not equal to Genesis
        if let Err(e) = validate::block_height_cmp_tail_final(self) {
            self.process_error(e, "TAIL", DBCol::BlockMisc)
        }
        // Check that all refs are counted
        if let Err(e) = validate::tx_refcount_final(self) {
            self.process_error(e, "TX_REFCOUNT", DBCol::Transactions)
        }
        if let Err(e) = validate::receipt_refcount_final(self) {
            self.process_error(e, "RECEIPT_REFCOUNT", DBCol::Receipts)
        }
        // Check that all Block Refcounts are counted
        if let Err(e) = validate::block_refcount_final(self) {
            self.process_error(e, "BLOCK_REFCOUNT", DBCol::BlockRefCount)
        }
    }

    fn check<K: std::fmt::Debug + ?Sized, V: ?Sized>(
        &mut self,
        f: &dyn Fn(&mut StoreValidator, &K, &V) -> Result<(), StoreValidatorError>,
        key: &K,
        value: &V,
        col: DBCol,
    ) {
        self.tests += 1;
        if let Err(e) = f(self, key, value) {
            self.process_error(e, key, col);
        }
    }
}

#[cfg(test)]
mod tests {
    use near_async::time::Clock;
    use near_chain_configs::{Genesis, MutableConfigValue};
    use near_epoch_manager::EpochManager;
    use near_store::genesis::initialize_genesis_state;
    use near_store::test_utils::create_test_store;

    use crate::rayon_spawner::RayonAsyncComputationSpawner;
    use crate::runtime::NightshadeRuntime;
    use crate::types::ChainConfig;
    use crate::{Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode};

    use super::*;
    use near_async::messaging::{IntoMultiSender, noop};

    fn init() -> (Chain, StoreValidator) {
        let store = create_test_store();
        let genesis = Genesis::test(vec!["test".parse().unwrap()], 1);
        let tempdir = tempfile::tempdir().unwrap();
        initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
        let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
        let runtime = NightshadeRuntime::test(
            tempdir.path(),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
        );
        let chain_genesis = ChainGenesis::new(&genesis.config);
        let chain = Chain::new(
            Clock::real(),
            epoch_manager.clone(),
            shard_tracker.clone(),
            runtime.clone(),
            &chain_genesis,
            DoomslugThresholdMode::NoApprovals,
            ChainConfig::test(),
            None,
            Arc::new(RayonAsyncComputationSpawner),
            MutableConfigValue::new(None, "validator_signer"),
            noop().into_multi_sender(),
        )
        .unwrap();
        (
            chain,
            StoreValidator::new(
                None,
                genesis.config,
                epoch_manager,
                shard_tracker,
                runtime,
                store,
                false,
            ),
        )
    }

    #[test]
    fn test_io_error() {
        let (chain, mut sv) = init();
        let mut store_update = chain.chain_store().store().store_update();
        assert!(sv.validate_col(DBCol::Block).is_ok());
        // Use `set_raw` to ruthlessly override block data with some garbage,
        // simulating IO error.
        store_update.set_raw_bytes(
            DBCol::Block,
            chain.get_block_by_height(0).unwrap().hash().as_ref(),
            &[123],
        );
        store_update.commit().unwrap();
        match sv.validate_col(DBCol::Block) {
            Err(StoreValidatorError::IOError(_)) => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn test_db_corruption() {
        let (chain, mut sv) = init();
        let mut store_update = chain.chain_store().store().store_update();
        assert!(sv.validate_col(DBCol::TrieChanges).is_ok());
        store_update.set_ser::<[u8]>(DBCol::TrieChanges, "567".as_ref(), &[123]).unwrap();
        store_update.commit().unwrap();
        match sv.validate_col(DBCol::TrieChanges) {
            Err(StoreValidatorError::DBCorruption(_)) => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn test_db_not_found() {
        let (chain, mut sv) = init();
        let block = chain.get_block_by_height(0).unwrap();
        assert!(validate::block_header_exists(&mut sv, block.hash(), &block).is_ok());
        match validate::block_header_exists(&mut sv, &CryptoHash::default(), &block) {
            Err(StoreValidatorError::DBNotFound { .. }) => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn test_discrepancy() {
        let (chain, mut sv) = init();
        let block_header = chain.get_block_header_by_height(0).unwrap();
        assert!(
            validate::block_header_hash_validity(&mut sv, block_header.hash(), &block_header)
                .is_ok()
        );
        match validate::block_header_hash_validity(&mut sv, &CryptoHash::default(), &block_header) {
            Err(StoreValidatorError::Discrepancy { .. }) => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn test_validation_failed() {
        let (_chain, mut sv) = init();
        assert!(validate::block_height_cmp_tail_final(&mut sv).is_ok());
        sv.inner.block_heights_less_tail.push(CryptoHash::default());
        assert!(validate::block_height_cmp_tail_final(&mut sv).is_ok());
        sv.inner.block_heights_less_tail.push(CryptoHash::default());
        match validate::block_height_cmp_tail_final(&mut sv) {
            Err(StoreValidatorError::ValidationFailed { .. }) => {}
            _ => assert!(false),
        }
    }
}
