use std::collections::HashMap;

use borsh::ser::BorshSerialize;

use near_primitives::borsh::BorshDeserialize;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;

use crate::db::refcount::encode_value_with_rc;
use crate::{DBCol, Store, StoreUpdate};
use near_primitives::sharding::ShardChunk;
use std::convert::TryFrom;

// Refcount from i32 to i64
pub(crate) fn col_state_refcount_8byte(store: &Store, store_update: &mut StoreUpdate) {
    for (k, v) in store.iter_without_rc_logic(DBCol::ColState) {
        if v.len() < 4 {
            store_update.delete(DBCol::ColState, &k);
            continue;
        }
        let mut v = v.into_vec();
        v.extend_from_slice(&[0, 0, 0, 0]);
        store_update.set(DBCol::ColState, &k, &v);
    }
}

// Deprecate ColTransactionRefCount, move the info to ColTransactions
pub(crate) fn migrate_col_transaction_refcount(store: &Store, store_update: &mut StoreUpdate) {
    let transactions: Vec<SignedTransaction> = store
        .iter_without_rc_logic(DBCol::ColTransactions)
        .map(|(_key, value)| {
            SignedTransaction::try_from_slice(&value).expect("BorshDeserialize should not fail")
        })
        .collect();
    let tx_refcount: HashMap<CryptoHash, u64> = store
        .iter(DBCol::_ColTransactionRefCount)
        .map(|(key, value)| {
            (
                CryptoHash::try_from_slice(&key).expect("BorshDeserialize should not fail"),
                u64::try_from_slice(&value).expect("BorshDeserialize should not fail"),
            )
        })
        .collect();

    assert_eq!(transactions.len(), tx_refcount.len());

    for tx in transactions {
        let tx_hash = tx.get_hash();
        let bytes = tx.try_to_vec().expect("BorshSerialize should not fail");
        let rc = *tx_refcount.get(&tx_hash).expect("Inconsistent tx refcount data") as i64;
        assert!(rc > 0);
        store_update.set(
            DBCol::ColTransactions,
            tx_hash.as_ref(),
            &encode_value_with_rc(&bytes, rc),
        );
        store_update.delete(DBCol::_ColTransactionRefCount, tx_hash.as_ref());
    }
}

// Make ColReceiptIdToShardId refcounted
pub(crate) fn migrate_receipts_refcount(store: &Store, store_update: &mut StoreUpdate) {
    let receipt_id_to_shard_id: Vec<_> =
        store.iter_without_rc_logic(DBCol::ColReceiptIdToShardId).collect();

    let chunks: Vec<ShardChunk> = store
        .iter(DBCol::ColChunks)
        .map(|key| ShardChunk::try_from_slice(&key.1).expect("BorshDeserialize should not fail"))
        .collect();

    let mut rx_refcount: HashMap<CryptoHash, i64> = HashMap::new();
    for chunk in chunks {
        for rx in chunk.receipts {
            rx_refcount.entry(rx.receipt_id).and_modify(|x| *x += 1).or_insert(1);
        }
    }

    for (key, bytes) in receipt_id_to_shard_id {
        let receipt_id = CryptoHash::try_from(key.as_ref()).unwrap();
        if let Some(rc) = rx_refcount.remove(&receipt_id) {
            store_update.set(DBCol::ColReceiptIdToShardId, &key, &encode_value_with_rc(&bytes, rc));
        } else {
            store_update.delete(DBCol::ColReceiptIdToShardId, &key);
        }
    }
    assert!(rx_refcount.is_empty(), "Receipts in chunks but not in ColReceiptIdToShardId");
}
