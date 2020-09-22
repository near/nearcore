use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};

use near_primitives::hash::{hash, CryptoHash};
use near_primitives::sharding::{
    EncodedShardChunk, PartialEncodedChunk, ReceiptList, ReceiptProof, ReedSolomonWrapper,
    ShardChunk, ShardProof,
};
use near_primitives::transaction::ExecutionOutcomeWithIdAndProof;
use near_primitives::version::DbVersion;

use crate::db::DBCol::{ColChunks, ColPartialChunks, ColStateParts};
use crate::db::{DBCol, RocksDB, VERSION_KEY};
use crate::migrations::v6_to_v7::{
    col_state_refcount_8byte, migrate_col_transaction_refcount, migrate_receipts_refcount,
};
use crate::migrations::v8_to_v9::{repair_col_receipt_id_to_shard_id, repair_col_transactions};
use crate::{create_store, Store, StoreUpdate};
use near_crypto::KeyType;
use near_primitives::merkle::merklize;
use near_primitives::validator_signer::InMemoryValidatorSigner;

pub mod v6_to_v7;
pub mod v8_to_v9;

pub fn get_store_version(path: &str) -> DbVersion {
    RocksDB::get_version(path).expect("Failed to open the database")
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
    match store.get_ser(DBCol::ColOutcomesByBlockHash, block_hash.as_ref()) {
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
                entry.get_mut().insert(outcome.id().clone());
            }
            Entry::Vacant(entry) => {
                let mut hash_set = get_outcomes_by_block_hash(store, &outcome.block_hash);
                hash_set.insert(outcome.id().clone());
                entry.insert(hash_set);
            }
        };
    }
    for (block_hash, hash_set) in block_hash_to_outcomes {
        store_update
            .set_ser(DBCol::ColOutcomesByBlockHash, block_hash.as_ref(), &hash_set)
            .expect("BorshSerialize should not fail");
    }
    store_update.commit().expect("Failed to migrate");
}

pub fn fill_col_transaction_refcount(store: &Store) {
    let mut store_update = store.store_update();
    let chunks: Vec<ShardChunk> = store
        .iter(DBCol::ColChunks)
        .map(|key| ShardChunk::try_from_slice(&key.1).expect("BorshDeserialize should not fail"))
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

pub fn migrate_6_to_7(path: &String) {
    let db = Arc::pin(RocksDB::new_v6(path).expect("Failed to open the database"));
    let store = Store::new(db);
    let mut store_update = store.store_update();
    col_state_refcount_8byte(&store, &mut store_update);
    migrate_col_transaction_refcount(&store, &mut store_update);
    migrate_receipts_refcount(&store, &mut store_update);
    set_store_version_inner(&mut store_update, 7);
    store_update.commit().expect("Failed to migrate")
}

pub fn migrate_7_to_8(path: &String) {
    let store = create_store(path);
    let mut store_update = store.store_update();
    for (key, _) in store.iter_without_rc_logic(ColStateParts) {
        store_update.delete(ColStateParts, &key);
    }
    set_store_version_inner(&mut store_update, 8);
    store_update.commit().expect("Fail to migrate from DB version 7 to DB version 8");
}

// No format change. Recompute ColTransactions and ColReceiptIdToShardId because they could be inconsistent.
pub fn migrate_8_to_9(path: &String) {
    let store = create_store(path);
    repair_col_transactions(&store);
    repair_col_receipt_id_to_shard_id(&store);
    set_store_version(&store, 9);
}

pub fn migrate_9_to_10(path: &String, is_archival: bool) {
    let store = create_store(path);
    if is_archival {
        // Hard code the number of parts there. These numbers are only used for this migration.
        let num_total_parts = 100;
        let num_data_parts = (num_total_parts - 1) / 3;
        let num_parity_parts = num_total_parts - num_data_parts;
        let mut rs = ReedSolomonWrapper::new(num_data_parts, num_parity_parts);
        let signer = InMemoryValidatorSigner::from_seed("test", KeyType::ED25519, "test");
        let mut store_update = store.store_update();
        let batch_size_limit = 10_000_000;
        let mut batch_size = 0;
        for (key, value) in store.iter_without_rc_logic(ColChunks) {
            if let Ok(Some(partial_chunk)) =
                store.get_ser::<PartialEncodedChunk>(ColPartialChunks, &key)
            {
                if partial_chunk.parts.len() == num_total_parts {
                    continue;
                }
            }
            batch_size += key.len() + value.len() + 8;
            let chunk: ShardChunk = BorshDeserialize::try_from_slice(&value)
                .expect("Borsh deserialization should not fail");
            let ShardChunk { chunk_hash, header, transactions, receipts } = chunk;
            let (mut encoded_chunk, merkle_paths) = EncodedShardChunk::new(
                header.inner.prev_block_hash,
                header.inner.prev_state_root,
                header.inner.outcome_root,
                header.inner.height_created,
                header.inner.shard_id,
                &mut rs,
                header.inner.gas_used,
                header.inner.gas_limit,
                header.inner.balance_burnt,
                header.inner.tx_root,
                header.inner.validator_proposals.clone(),
                transactions,
                &receipts,
                header.inner.outgoing_receipts_root,
                &signer,
            )
            .expect("create encoded chunk should not fail");
            encoded_chunk.header = header;
            let outgoing_receipt_hashes =
                vec![hash(&ReceiptList(0, &receipts).try_to_vec().unwrap())];
            let (_, outgoing_receipt_proof) = merklize(&outgoing_receipt_hashes);

            let partial_encoded_chunk = encoded_chunk.create_partial_encoded_chunk(
                (0..num_total_parts as u64).collect(),
                vec![ReceiptProof(
                    receipts,
                    ShardProof {
                        from_shard_id: 0,
                        to_shard_id: 0,
                        proof: outgoing_receipt_proof[0].clone(),
                    },
                )],
                &merkle_paths,
            );
            store_update
                .set_ser(ColPartialChunks, chunk_hash.as_ref(), &partial_encoded_chunk)
                .expect("storage update should not fail");
            if batch_size > batch_size_limit {
                store_update.commit().expect("storage update should not fail");
                store_update = store.store_update();
                batch_size = 0;
            }
        }
        store_update.commit().expect("storage update should not fail");
    }
    set_store_version(&store, 10);
}
