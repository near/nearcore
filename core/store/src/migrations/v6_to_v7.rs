use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::Cursor;

use borsh::ser::BorshSerialize;
use byteorder::{LittleEndian, ReadBytesExt};

use near_primitives::block::Block;
use near_primitives::borsh::BorshDeserialize;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::ShardChunkV1;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, NumShards, ShardId};

use crate::db::refcount::encode_value_with_rc;
use crate::{DBCol, Store, StoreUpdate};

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

pub(crate) fn get_num_shards(store: &Store) -> NumShards {
    store
        .iter(DBCol::ColBlock)
        .map(|(_key, value)| {
            Block::try_from_slice(value.as_ref()).expect("BorshDeserialize should not fail")
        })
        .map(|block| block.chunks().len() as u64)
        .next()
        .unwrap_or(1)
}

pub(crate) fn account_id_to_shard_id_v6(account_id: &AccountId, num_shards: NumShards) -> ShardId {
    let mut cursor = Cursor::new(hash(&account_id.clone().into_bytes()).0);
    cursor.read_u64::<LittleEndian>().expect("Must not happened") % (num_shards)
}

// Make ColReceiptIdToShardId refcounted
pub(crate) fn migrate_receipts_refcount(store: &Store, store_update: &mut StoreUpdate) {
    let receipt_id_to_shard_id: HashMap<_, _> =
        store.iter_without_rc_logic(DBCol::ColReceiptIdToShardId).collect();

    let chunks: Vec<ShardChunkV1> = store
        .iter(DBCol::ColChunks)
        .map(|(_key, value)| {
            ShardChunkV1::try_from_slice(&value).expect("BorshDeserialize should not fail")
        })
        .collect();

    let mut receipts: HashMap<CryptoHash, (Receipt, i64)> = HashMap::new();
    for chunk in chunks {
        for rx in chunk.receipts {
            receipts.entry(rx.receipt_id).and_modify(|(_receipt, rc)| *rc += 1).or_insert((rx, 1));
        }
    }

    for (key, bytes) in receipt_id_to_shard_id {
        let receipt_id = CryptoHash::try_from(key.as_ref()).unwrap();
        if let Some((_receipt, rc)) = receipts.remove(&receipt_id) {
            store_update.set(DBCol::ColReceiptIdToShardId, &key, &encode_value_with_rc(&bytes, rc));
        } else {
            store_update.delete(DBCol::ColReceiptIdToShardId, &key);
        }
    }

    if !receipts.is_empty() {
        // It's possible that some receipts are in chunks, but not in ColReceiptIdToShardId.
        // We need to write records for them to maintain store invariant, because gc-ing the chunks is
        // will decrement rc for receipts.
        //
        let num_shards = get_num_shards(&store);
        for (receipt_id, (receipt, rc)) in receipts {
            let shard_id = account_id_to_shard_id_v6(&receipt.receiver_id, num_shards)
                .try_to_vec()
                .expect("BorshSerialize should not fail");
            store_update.set(
                DBCol::ColReceiptIdToShardId,
                receipt_id.as_ref(),
                &encode_value_with_rc(&shard_id, rc),
            );
        }
    }
}
