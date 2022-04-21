use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};

use near_primitives::block::{Block, Tip};
use near_primitives::block_header::BlockHeader;
use near_primitives::epoch_manager::epoch_info::{EpochInfo, EpochInfoV1};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::receipt::{DelayedReceiptIndices, Receipt, ReceiptEnum};
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::{
    EncodedShardChunk, EncodedShardChunkV1, PartialEncodedChunk, PartialEncodedChunkV1, ShardChunk,
    ShardChunkV1,
};
use near_primitives::syncing::{ShardStateSyncResponseHeader, ShardStateSyncResponseHeaderV1};
use near_primitives::transaction::ExecutionOutcomeWithIdAndProof;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, Balance};
use near_primitives::utils::{
    create_receipt_id_from_transaction, get_block_shard_id, index_to_bytes,
};
use near_primitives::version::DbVersion;

use crate::db::{DBError, RocksDB, GENESIS_JSON_HASH_KEY, VERSION_KEY};
use crate::migrations::v6_to_v7::{
    col_state_refcount_8byte, migrate_col_transaction_refcount, migrate_receipts_refcount,
};
use crate::migrations::v8_to_v9::{
    recompute_col_rc, repair_col_receipt_id_to_shard_id, repair_col_transactions,
};
use crate::trie::{TrieCache, TrieCachingStorage};
use crate::DBCol::{
    ColBlockHeader, ColBlockHeight, ColBlockMerkleTree, ColBlockMisc, ColBlockOrdinal,
    ColStateParts,
};
use crate::{create_store, DBCol, Store, StoreUpdate, Trie, TrieUpdate, FINAL_HEAD_KEY, HEAD_KEY};
use std::path::Path;

pub mod v6_to_v7;
pub mod v8_to_v9;

pub fn get_store_version(path: &Path) -> Result<DbVersion, DBError> {
    RocksDB::get_version(path)
}

fn set_store_version_inner(store_update: &mut StoreUpdate, db_version: u32) {
    store_update.set(
        DBCol::ColDbVersion,
        VERSION_KEY,
        &serde_json::to_vec(&db_version).expect("Failed to serialize version"),
    );
}

pub fn set_store_version(store: &Store, db_version: u32) {
    let mut store_update = store.store_update();
    set_store_version_inner(&mut store_update, db_version);
    store_update.commit().expect("Failed to write version to database");
}

fn get_outcomes_by_block_hash(store: &Store, block_hash: &CryptoHash) -> HashSet<CryptoHash> {
    match store.get_ser(DBCol::ColOutcomeIds, block_hash.as_ref()) {
        Ok(Some(hash_set)) => hash_set,
        Ok(None) => HashSet::new(),
        Err(e) => panic!("Can't read DB, {:?}", e),
    }
}

pub fn fill_col_outcomes_by_hash(store: &Store) {
    let mut store_update = store.store_update();
    let outcomes: Vec<ExecutionOutcomeWithIdAndProof> = store
        .iter(DBCol::ColTransactionResult)
        .map(|key| {
            ExecutionOutcomeWithIdAndProof::try_from_slice(&key.1)
                .expect("BorshDeserialize should not fail")
        })
        .collect();
    let mut block_hash_to_outcomes: HashMap<CryptoHash, HashSet<CryptoHash>> = HashMap::new();
    for outcome in outcomes {
        match block_hash_to_outcomes.entry(outcome.block_hash) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().insert(*outcome.id());
            }
            Entry::Vacant(entry) => {
                let mut hash_set = get_outcomes_by_block_hash(store, &outcome.block_hash);
                hash_set.insert(*outcome.id());
                entry.insert(hash_set);
            }
        };
    }
    for (block_hash, hash_set) in block_hash_to_outcomes {
        store_update
            .set_ser(DBCol::ColOutcomeIds, block_hash.as_ref(), &hash_set)
            .expect("BorshSerialize should not fail");
    }
    store_update.commit().expect("Failed to migrate");
}

pub fn fill_col_transaction_refcount(store: &Store) {
    let mut store_update = store.store_update();
    let chunks: Vec<ShardChunkV1> = store
        .iter(DBCol::ColChunks)
        .map(|key| ShardChunkV1::try_from_slice(&key.1).expect("BorshDeserialize should not fail"))
        .collect();

    let mut tx_refcount: HashMap<CryptoHash, u64> = HashMap::new();
    for chunk in chunks {
        for tx in chunk.transactions {
            tx_refcount.entry(tx.get_hash()).and_modify(|x| *x += 1).or_insert(1);
        }
    }
    for (tx_hash, refcount) in tx_refcount {
        store_update
            .set_ser(DBCol::_ColTransactionRefCount, tx_hash.as_ref(), &refcount)
            .expect("BorshSerialize should not fail");
    }
    store_update.commit().expect("Failed to migrate");
}

fn recompute_block_ordinal(store: &Store) {
    let mut store_update = BatchedStoreUpdate::new(store, 10_000_000);
    for (_, value) in store.iter(ColBlockHeight) {
        let block_merkle_tree =
            store.get_ser::<PartialMerkleTree>(ColBlockMerkleTree, &value).unwrap().unwrap();
        let block_hash = CryptoHash::try_from_slice(&value).unwrap();
        store_update
            .set_ser(ColBlockOrdinal, &index_to_bytes(block_merkle_tree.size()), &block_hash)
            .unwrap();
    }
    store_update.finish().unwrap();
}

pub fn migrate_6_to_7(path: &Path) {
    let db = Arc::new(RocksDB::new_v6(path).expect("Failed to open the database"));
    let store = Store::new(db);
    let mut store_update = store.store_update();
    col_state_refcount_8byte(&store, &mut store_update);
    migrate_col_transaction_refcount(&store, &mut store_update);
    migrate_receipts_refcount(&store, &mut store_update);
    set_store_version_inner(&mut store_update, 7);
    store_update.commit().expect("Failed to migrate")
}

pub fn migrate_7_to_8(path: &Path) {
    let store = create_store(path);
    let mut store_update = store.store_update();
    for (key, _) in store.iter_without_rc_logic(ColStateParts) {
        store_update.delete(ColStateParts, &key);
    }
    set_store_version_inner(&mut store_update, 8);
    store_update.commit().expect("Fail to migrate from DB version 7 to DB version 8");
}

// No format change. Recompute ColTransactions and ColReceiptIdToShardId because they could be inconsistent.
pub fn migrate_8_to_9(path: &Path) {
    let store = create_store(path);
    repair_col_transactions(&store);
    repair_col_receipt_id_to_shard_id(&store);
    set_store_version(&store, 9);
}

pub fn migrate_10_to_11(path: &Path) {
    let store = create_store(path);
    let mut store_update = store.store_update();
    let head = store.get_ser::<Tip>(ColBlockMisc, HEAD_KEY).unwrap().expect("head must exist");
    let block_header = store
        .get_ser::<BlockHeader>(ColBlockHeader, head.last_block_hash.as_ref())
        .unwrap()
        .expect("head header must exist");
    let last_final_block_hash = if block_header.last_final_block() == &CryptoHash::default() {
        let mut cur_header = block_header;
        while cur_header.prev_hash() != &CryptoHash::default() {
            cur_header = store
                .get_ser::<BlockHeader>(ColBlockHeader, cur_header.prev_hash().as_ref())
                .unwrap()
                .unwrap()
        }
        *cur_header.hash()
    } else {
        *block_header.last_final_block()
    };
    let last_final_header = store
        .get_ser::<BlockHeader>(ColBlockHeader, last_final_block_hash.as_ref())
        .unwrap()
        .expect("last final block header must exist");
    let final_head = Tip::from_header(&last_final_header);
    store_update.set_ser(ColBlockMisc, FINAL_HEAD_KEY, &final_head).unwrap();
    store_update.commit().unwrap();
    set_store_version(&store, 11);
}

pub fn migrate_11_to_12(path: &Path) {
    let store = create_store(path);
    recompute_col_rc(
        &store,
        DBCol::ColReceipts,
        store
            .iter(DBCol::ColChunks)
            .map(|(_key, value)| {
                ShardChunkV1::try_from_slice(&value).expect("BorshDeserialize should not fail")
            })
            .flat_map(|chunk: ShardChunkV1| chunk.receipts)
            .map(|rx| (rx.receipt_id, rx.try_to_vec().unwrap())),
    );
    set_store_version(&store, 12);
}

pub struct BatchedStoreUpdate<'a> {
    batch_size_limit: usize,
    batch_size: usize,
    store: &'a Store,
    store_update: Option<StoreUpdate>,
}

impl<'a> BatchedStoreUpdate<'a> {
    pub fn new(store: &'a Store, batch_size_limit: usize) -> Self {
        Self { batch_size_limit, batch_size: 0, store, store_update: Some(store.store_update()) }
    }

    fn commit(&mut self) -> Result<(), std::io::Error> {
        let store_update = self.store_update.take().unwrap();
        store_update.commit()?;
        self.store_update = Some(self.store.store_update());
        self.batch_size = 0;
        Ok(())
    }

    pub fn set_ser<T: BorshSerialize>(
        &mut self,
        col: DBCol,
        key: &[u8],
        value: &T,
    ) -> Result<(), std::io::Error> {
        let value_bytes = value.try_to_vec()?;
        self.batch_size += key.as_ref().len() + value_bytes.len() + 8;
        self.store_update.as_mut().unwrap().set(col, key.as_ref(), &value_bytes);

        if self.batch_size > self.batch_size_limit {
            self.commit()?;
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<(), std::io::Error> {
        if self.batch_size > 0 {
            self.commit()?;
        }

        Ok(())
    }
}

fn map_col<T, U, F>(store: &Store, col: DBCol, f: F) -> Result<(), std::io::Error>
where
    T: BorshDeserialize,
    U: BorshSerialize,
    F: Fn(T) -> U,
{
    let keys: Vec<_> = store.iter(col).map(|(key, _)| key).collect();
    let mut store_update = BatchedStoreUpdate::new(store, 10_000_000);

    for key in keys {
        let value: T = store.get_ser(col, key.as_ref())?.unwrap();
        let new_value = f(value);
        store_update.set_ser(col, key.as_ref(), &new_value)?;
    }

    store_update.finish()?;

    Ok(())
}

#[allow(unused)]
fn map_col_from_key<U, F>(store: &Store, col: DBCol, f: F) -> Result<(), std::io::Error>
where
    U: BorshSerialize,
    F: Fn(&[u8]) -> U,
{
    let mut store_update = store.store_update();
    let batch_size_limit = 10_000_000;
    let mut batch_size = 0;
    for (key, _) in store.iter(col) {
        let new_value = f(&key);
        let new_bytes = new_value.try_to_vec()?;
        batch_size += key.as_ref().len() + new_bytes.len() + 8;
        store_update.set(col, key.as_ref(), &new_bytes);

        if batch_size > batch_size_limit {
            store_update.commit()?;
            store_update = store.store_update();
            batch_size = 0;
        }
    }

    if batch_size > 0 {
        store_update.commit()?;
    }

    Ok(())
}

/// Lift all chunks to the versioned structure
pub fn migrate_13_to_14(path: &Path) {
    let store = create_store(path);

    map_col(&store, DBCol::ColPartialChunks, |pec: PartialEncodedChunkV1| {
        PartialEncodedChunk::V1(pec)
    })
    .unwrap();
    map_col(&store, DBCol::ColInvalidChunks, |chunk: EncodedShardChunkV1| {
        EncodedShardChunk::V1(chunk)
    })
    .unwrap();
    map_col(&store, DBCol::ColChunks, ShardChunk::V1).unwrap();
    map_col(&store, DBCol::ColStateHeaders, |header: ShardStateSyncResponseHeaderV1| {
        ShardStateSyncResponseHeader::V1(header)
    })
    .unwrap();

    set_store_version(&store, 14);
}

/// Make execution outcome ids in `ColOutcomeIds` ordered by replaying the chunks.
pub fn migrate_14_to_15(path: &Path) {
    let store = create_store(path);
    let trie_store = Box::new(TrieCachingStorage::new(
        store.clone(),
        TrieCache::new(),
        ShardUId::single_shard(),
    ));
    let trie = Rc::new(Trie::new(trie_store, ShardUId::single_shard()));

    let mut store_update = store.store_update();
    let batch_size_limit = 10_000_000;
    let mut batch_size = 0;

    for (key, value) in store.iter_without_rc_logic(DBCol::ColOutcomeIds) {
        let block_hash = CryptoHash::try_from_slice(&key).unwrap();
        let block =
            store.get_ser::<Block>(DBCol::ColBlock, &key).unwrap().expect("block should exist");

        for chunk_header in
            block.chunks().iter().filter(|h| h.height_included() == block.header().height())
        {
            let execution_outcome_ids = <HashSet<CryptoHash>>::try_from_slice(&value).unwrap();

            let chunk = store
                .get_ser::<ShardChunk>(DBCol::ColChunks, chunk_header.chunk_hash().as_ref())
                .unwrap()
                .expect("chunk should exist");

            let epoch_info = store
                .get_ser::<EpochInfoV1>(DBCol::ColEpochInfo, block.header().epoch_id().as_ref())
                .unwrap()
                .expect("epoch id should exist");
            let protocol_version = epoch_info.protocol_version;

            let mut new_execution_outcome_ids = vec![];
            let mut local_receipt_ids = vec![];
            let mut local_receipt_congestion = false;

            // Step 0: execution outcomes of transactions
            for transaction in chunk.transactions() {
                let tx_hash = transaction.get_hash();
                // Transactions must all be executed since when chunk is produced, there is a gas
                // limit check.
                assert!(
                    execution_outcome_ids.contains(&tx_hash),
                    "transaction hash {} does not exist in block {}",
                    tx_hash,
                    block_hash
                );
                new_execution_outcome_ids.push(tx_hash);
                if transaction.transaction.signer_id == transaction.transaction.receiver_id {
                    let local_receipt_id = create_receipt_id_from_transaction(
                        protocol_version,
                        transaction,
                        block.header().prev_hash(),
                        block.header().hash(),
                    );
                    if execution_outcome_ids.contains(&local_receipt_id) {
                        local_receipt_ids.push(local_receipt_id);
                    } else {
                        local_receipt_congestion = true;
                    }
                }
            }

            // Step 1: local receipts
            new_execution_outcome_ids.extend(local_receipt_ids);

            let mut state_update = TrieUpdate::new(trie.clone(), chunk.prev_state_root());

            let mut process_receipt =
                |receipt: &Receipt, state_update: &mut TrieUpdate| match &receipt.receipt {
                    ReceiptEnum::Action(_) => {
                        if execution_outcome_ids.contains(&receipt.receipt_id) {
                            new_execution_outcome_ids.push(receipt.receipt_id);
                        }
                    }
                    ReceiptEnum::Data(data_receipt) => {
                        if let Ok(Some(bytes)) = state_update.get(&TrieKey::PostponedReceiptId {
                            receiver_id: receipt.receiver_id.clone(),
                            data_id: data_receipt.data_id,
                        }) {
                            let receipt_id = CryptoHash::try_from_slice(&bytes).unwrap();
                            let trie_key = TrieKey::PendingDataCount {
                                receiver_id: receipt.receiver_id.clone(),
                                receipt_id,
                            };
                            let pending_receipt_count =
                                u32::try_from_slice(&state_update.get(&trie_key).unwrap().unwrap())
                                    .unwrap();
                            if pending_receipt_count == 1
                                && execution_outcome_ids.contains(&receipt_id)
                            {
                                new_execution_outcome_ids.push(receipt_id);
                            }
                            state_update
                                .set(trie_key, (pending_receipt_count - 1).try_to_vec().unwrap())
                        }
                    }
                };

            // Step 2: delayed receipts
            if !local_receipt_congestion {
                let mut delayed_receipt_indices: DelayedReceiptIndices = state_update
                    .get(&TrieKey::DelayedReceiptIndices)
                    .map(|bytes| {
                        bytes
                            .map(|b| DelayedReceiptIndices::try_from_slice(&b).unwrap())
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();

                while delayed_receipt_indices.first_index
                    < delayed_receipt_indices.next_available_index
                {
                    let receipt: Receipt = state_update
                        .get(&TrieKey::DelayedReceipt {
                            index: delayed_receipt_indices.first_index,
                        })
                        .unwrap()
                        .map(|bytes| Receipt::try_from_slice(&bytes).unwrap())
                        .unwrap();
                    process_receipt(&receipt, &mut state_update);
                    delayed_receipt_indices.first_index += 1;
                }
            }

            // Step 3: receipts
            for receipt in chunk.receipts() {
                process_receipt(receipt, &mut state_update);
            }
            assert_eq!(
                new_execution_outcome_ids.len(),
                execution_outcome_ids.len(),
                "inconsistent number of outcomes detected while migrating block {}: {:?} vs. {:?}",
                block_hash,
                new_execution_outcome_ids,
                execution_outcome_ids
            );
            let value = new_execution_outcome_ids.try_to_vec().unwrap();
            store_update.set(
                DBCol::ColOutcomeIds,
                &get_block_shard_id(&block_hash, chunk_header.shard_id()),
                &value,
            );
            store_update.delete(DBCol::ColOutcomeIds, &key);
            batch_size += key.len() + value.len() + 40;
            if batch_size > batch_size_limit {
                store_update.commit().unwrap();
                store_update = store.store_update();
                batch_size = 0;
            }
        }
    }
    store_update.commit().unwrap();
    set_store_version(&store, 15);
}

pub fn migrate_17_to_18(path: &Path) {
    use near_primitives::challenge::SlashedValidator;
    use near_primitives::types::validator_stake::ValidatorStakeV1;
    use near_primitives::types::{BlockHeight, EpochId};
    use near_primitives::version::ProtocolVersion;

    // Migrate from OldBlockInfo to NewBlockInfo - add hash
    #[derive(BorshDeserialize)]
    struct OldBlockInfo {
        pub height: BlockHeight,
        pub last_finalized_height: BlockHeight,
        pub last_final_block_hash: CryptoHash,
        pub prev_hash: CryptoHash,
        pub epoch_first_block: CryptoHash,
        pub epoch_id: EpochId,
        pub proposals: Vec<ValidatorStakeV1>,
        pub validator_mask: Vec<bool>,
        pub latest_protocol_version: ProtocolVersion,
        pub slashed: Vec<SlashedValidator>,
        pub total_supply: Balance,
    }
    #[derive(BorshSerialize)]
    struct NewBlockInfo {
        pub hash: CryptoHash,
        pub height: BlockHeight,
        pub last_finalized_height: BlockHeight,
        pub last_final_block_hash: CryptoHash,
        pub prev_hash: CryptoHash,
        pub epoch_first_block: CryptoHash,
        pub epoch_id: EpochId,
        pub proposals: Vec<ValidatorStakeV1>,
        pub validator_mask: Vec<bool>,
        pub latest_protocol_version: ProtocolVersion,
        pub slashed: Vec<SlashedValidator>,
        pub total_supply: Balance,
    }
    let store = create_store(path);
    map_col_from_key(&store, DBCol::ColBlockInfo, |key| {
        let hash = CryptoHash::try_from(key).unwrap();
        let old_block_info =
            store.get_ser::<OldBlockInfo>(DBCol::ColBlockInfo, key).unwrap().unwrap();
        NewBlockInfo {
            hash,
            height: old_block_info.height,
            last_finalized_height: old_block_info.last_finalized_height,
            last_final_block_hash: old_block_info.last_final_block_hash,
            prev_hash: old_block_info.prev_hash,
            epoch_first_block: old_block_info.epoch_first_block,
            epoch_id: old_block_info.epoch_id,
            proposals: old_block_info.proposals,
            validator_mask: old_block_info.validator_mask,
            latest_protocol_version: old_block_info.latest_protocol_version,
            slashed: old_block_info.slashed,
            total_supply: old_block_info.total_supply,
        }
    })
    .unwrap();

    // Add ColHeaderHashesByHeight lazily
    //
    // KPR: traversing thru ColBlockHeader at Mainnet (20 mln Headers)
    // takes ~13 minutes on my laptop.
    // It's annoying to wait until migration finishes
    // as real impact is not too big as we don't GC Headers now.
    // I expect that after 5 Epochs ColHeaderHashesByHeight will be filled
    // properly and we never return to this migration again.

    set_store_version(&store, 18);
}

pub fn migrate_20_to_21(path: &Path) {
    let store = create_store(path);
    let mut store_update = store.store_update();
    store_update.delete(DBCol::ColBlockMisc, GENESIS_JSON_HASH_KEY);
    store_update.commit().unwrap();

    set_store_version(&store, 21);
}

pub fn migrate_21_to_22(path: &Path) {
    use near_primitives::epoch_manager::{BlockInfoV1, SlashState};
    use near_primitives::types::validator_stake::ValidatorStakeV1;
    use near_primitives::types::{BlockHeight, EpochId};
    use near_primitives::version::ProtocolVersion;
    #[derive(BorshDeserialize)]
    struct OldBlockInfo {
        pub hash: CryptoHash,
        pub height: BlockHeight,
        pub last_finalized_height: BlockHeight,
        pub last_final_block_hash: CryptoHash,
        pub prev_hash: CryptoHash,
        pub epoch_first_block: CryptoHash,
        pub epoch_id: EpochId,
        pub proposals: Vec<ValidatorStakeV1>,
        pub chunk_mask: Vec<bool>,
        pub latest_protocol_version: ProtocolVersion,
        pub slashed: HashMap<AccountId, SlashState>,
        pub total_supply: Balance,
    }
    let store = create_store(path);
    map_col_from_key(&store, DBCol::ColBlockInfo, |key| {
        let old_block_info =
            store.get_ser::<OldBlockInfo>(DBCol::ColBlockInfo, key).unwrap().unwrap();
        if key == &[0; 32] {
            // dummy value
            return BlockInfoV1 {
                hash: old_block_info.hash,
                height: old_block_info.height,
                last_finalized_height: old_block_info.last_finalized_height,
                last_final_block_hash: old_block_info.last_final_block_hash,
                prev_hash: old_block_info.prev_hash,
                epoch_first_block: old_block_info.epoch_first_block,
                epoch_id: old_block_info.epoch_id,
                proposals: old_block_info.proposals,
                chunk_mask: old_block_info.chunk_mask,
                latest_protocol_version: old_block_info.latest_protocol_version,
                slashed: old_block_info.slashed,
                total_supply: old_block_info.total_supply,
                timestamp_nanosec: 0,
            };
        }
        let block_header =
            store.get_ser::<BlockHeader>(DBCol::ColBlockHeader, key).unwrap().unwrap();
        BlockInfoV1 {
            hash: old_block_info.hash,
            height: old_block_info.height,
            last_finalized_height: old_block_info.last_finalized_height,
            last_final_block_hash: old_block_info.last_final_block_hash,
            prev_hash: old_block_info.prev_hash,
            epoch_first_block: old_block_info.epoch_first_block,
            epoch_id: old_block_info.epoch_id,
            proposals: old_block_info.proposals,
            chunk_mask: old_block_info.chunk_mask,
            latest_protocol_version: old_block_info.latest_protocol_version,
            slashed: old_block_info.slashed,
            total_supply: old_block_info.total_supply,
            timestamp_nanosec: block_header.raw_timestamp(),
        }
    })
    .unwrap();
    set_store_version(&store, 22);
}

pub fn migrate_25_to_26(path: &Path) {
    let store = create_store(path);
    let mut store_update = store.store_update();
    store_update.delete_all(DBCol::ColCachedContractCode);
    store_update.commit().unwrap();

    set_store_version(&store, 26);
}

pub fn migrate_26_to_27(path: &Path, is_archival: bool) {
    let store = create_store(path);
    if is_archival {
        recompute_block_ordinal(&store);
    }
    set_store_version(&store, 27);
}

pub fn migrate_28_to_29(path: &Path) {
    let store = create_store(path);
    let mut store_update = store.store_update();
    store_update.delete_all(DBCol::_ColNextBlockWithNewChunk);
    store_update.delete_all(DBCol::_ColLastBlockWithNewChunk);
    store_update.commit().unwrap();

    set_store_version(&store, 29);
}

pub fn migrate_29_to_30(path: &Path) {
    use near_primitives::epoch_manager::block_info::BlockInfo;
    use near_primitives::epoch_manager::epoch_info::EpochSummary;
    use near_primitives::epoch_manager::AGGREGATOR_KEY;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_primitives::types::validator_stake::ValidatorStakeV1;
    use near_primitives::types::{
        BlockChunkValidatorStats, EpochId, ProtocolVersion, ShardId, ValidatorId,
        ValidatorKickoutReason, ValidatorStats,
    };
    use std::collections::BTreeMap;

    let store = create_store(path);

    #[derive(BorshDeserialize)]
    pub struct OldEpochSummary {
        pub prev_epoch_last_block_hash: CryptoHash,
        pub all_proposals: Vec<ValidatorStakeV1>,
        pub validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
        pub validator_block_chunk_stats: HashMap<AccountId, BlockChunkValidatorStats>,
        pub next_version: ProtocolVersion,
    }

    #[derive(BorshDeserialize)]
    pub struct OldEpochInfoAggregator {
        pub block_tracker: HashMap<ValidatorId, ValidatorStats>,
        pub shard_tracker: HashMap<ShardId, HashMap<ValidatorId, ValidatorStats>>,
        pub version_tracker: HashMap<ValidatorId, ProtocolVersion>,
        pub all_proposals: BTreeMap<AccountId, ValidatorStakeV1>,
        pub epoch_id: EpochId,
        pub last_block_hash: CryptoHash,
    }
    #[derive(BorshSerialize)]
    pub struct NewEpochInfoAggregator {
        pub block_tracker: HashMap<ValidatorId, ValidatorStats>,
        pub shard_tracker: HashMap<ShardId, HashMap<ValidatorId, ValidatorStats>>,
        pub version_tracker: HashMap<ValidatorId, ProtocolVersion>,
        pub all_proposals: BTreeMap<AccountId, ValidatorStake>,
        pub epoch_id: EpochId,
        pub last_block_hash: CryptoHash,
    }

    map_col(&store, DBCol::ColChunkExtra, ChunkExtra::V1).unwrap();

    map_col(&store, DBCol::ColBlockInfo, BlockInfo::V1).unwrap();

    map_col(&store, DBCol::ColEpochValidatorInfo, |info: OldEpochSummary| EpochSummary {
        prev_epoch_last_block_hash: info.prev_epoch_last_block_hash,
        all_proposals: info.all_proposals.into_iter().map(ValidatorStake::V1).collect(),
        validator_kickout: info.validator_kickout,
        validator_block_chunk_stats: info.validator_block_chunk_stats,
        next_version: info.next_version,
    })
    .unwrap();

    // DBCol::ColEpochInfo has a special key which contains a different type than all other
    // values (EpochInfoAggregator), so we cannot use `map_col` on it. We need to handle
    // the AGGREGATOR_KEY differently from all others.
    let col = DBCol::ColEpochInfo;
    let keys: Vec<_> = store.iter(col).map(|(key, _)| key).collect();
    let mut store_update = BatchedStoreUpdate::new(&store, 10_000_000);
    for key in keys {
        if key.as_ref() == AGGREGATOR_KEY {
            let value: OldEpochInfoAggregator = store.get_ser(col, key.as_ref()).unwrap().unwrap();
            let new_value = NewEpochInfoAggregator {
                block_tracker: value.block_tracker,
                shard_tracker: value.shard_tracker,
                version_tracker: value.version_tracker,
                epoch_id: value.epoch_id,
                last_block_hash: value.last_block_hash,
                all_proposals: value
                    .all_proposals
                    .into_iter()
                    .map(|(account, stake)| (account, ValidatorStake::V1(stake)))
                    .collect(),
            };
            store_update.set_ser(col, key.as_ref(), &new_value).unwrap();
        } else {
            let value: EpochInfoV1 = store.get_ser(col, key.as_ref()).unwrap().unwrap();
            let new_value = EpochInfo::V1(value);
            store_update.set_ser(col, key.as_ref(), &new_value).unwrap();
        }
    }

    store_update.finish().unwrap();

    set_store_version(&store, 30);
}
