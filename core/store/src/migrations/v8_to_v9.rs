use borsh::{BorshDeserialize, BorshSerialize};

use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::ShardChunkV1;

use crate::migrations::v6_to_v7::{account_id_to_shard_id_v6, get_num_shards};
use crate::{DBCol, Store};

/// Clear all data in the column, insert keys and values from iterator.
/// Uses multiple writes.
pub(crate) fn recompute_col_rc<Iter>(store: &Store, column: DBCol, values: Iter)
where
    Iter: Iterator<Item = (CryptoHash, Vec<u8>)>,
{
    assert!(crate::db::IS_COL_RC[column as usize]);
    let mut batch_size = 0;
    let batch_size_limit = 250_000_000;

    let mut store_update = store.store_update();
    store_update.delete_all(column);
    store_update.commit().unwrap();

    let mut store_update = store.store_update();

    for (key, value) in values {
        store_update.update_refcount(column, key.as_ref(), &value, 1);
        batch_size += key.as_ref().len() + value.len() + 8;
        if batch_size > batch_size_limit {
            store_update
                .commit()
                .unwrap_or_else(|_| panic!("Failed during recomputing column {:?}", column));
            store_update = store.store_update();
            batch_size = 0;
        }
    }

    if batch_size > 0 {
        store_update
            .commit()
            .unwrap_or_else(|_| panic!("Failed during recomputing column {:?}", column));
    }
}

// Make ColTransactions match transactions in ColChunks
pub(crate) fn repair_col_transactions(store: &Store) {
    recompute_col_rc(
        store,
        DBCol::ColTransactions,
        store
            .iter(DBCol::ColChunks)
            .map(|(_key, value)| {
                ShardChunkV1::try_from_slice(&value).expect("BorshDeserialize should not fail")
            })
            .flat_map(|chunk: ShardChunkV1| chunk.transactions)
            .map(|tx| (tx.get_hash(), tx.try_to_vec().unwrap())),
    )
}

// Make ColReceiptIdToShardId match receipts in ColOutgoingReceipts
pub(crate) fn repair_col_receipt_id_to_shard_id(store: &Store) {
    let num_shards = get_num_shards(store);
    recompute_col_rc(
        store,
        DBCol::ColReceiptIdToShardId,
        store
            .iter(DBCol::ColOutgoingReceipts)
            .flat_map(|(_key, value)| {
                <Vec<Receipt>>::try_from_slice(&value).expect("BorshDeserialize should not fail")
            })
            .map(|receipt| {
                (
                    receipt.receipt_id,
                    account_id_to_shard_id_v6(&receipt.receiver_id, num_shards)
                        .try_to_vec()
                        .unwrap(),
                )
            }),
    )
}
