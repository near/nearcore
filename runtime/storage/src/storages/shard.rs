use super::{extend_with_cache, read_with_cache, write_with_cache, StorageResult};
use super::{BlockChainStorage, GenericStorage};
use super::{ChainId, KeyValueDB};
use super::{
    CACHE_SIZE, COL_RECEIPT_BLOCK, COL_STATE, COL_TRANSACTION_ADDRESSES, COL_TRANSACTION_RESULTS,
    COL_TX_NONCE,
};
use cached::SizedCache;
use primitives::chain::{ReceiptBlock, SignedShardBlock, SignedShardBlockHeader};
use primitives::hash::CryptoHash;
use primitives::transaction::{TransactionAddress, TransactionResult};
use primitives::types::{AccountId, BlockIndex, ShardId};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

/// Shard chain
pub struct ShardChainStorage {
    generic_storage: BlockChainStorage<SignedShardBlockHeader, SignedShardBlock>,
    // keyed by transaction hash
    transaction_results: SizedCache<Vec<u8>, TransactionResult>,
    // keyed by transaction hash
    transaction_addresses: SizedCache<Vec<u8>, TransactionAddress>,
    // keyed by block index
    receipts: SizedCache<Vec<u8>, HashMap<ShardId, ReceiptBlock>>,
    // Records the largest transaction nonce per account
    tx_nonce: SizedCache<Vec<u8>, u64>,
}

impl GenericStorage<SignedShardBlockHeader, SignedShardBlock> for ShardChainStorage {
    #[inline]
    fn blockchain_storage_mut(
        &mut self,
    ) -> &mut BlockChainStorage<SignedShardBlockHeader, SignedShardBlock> {
        &mut self.generic_storage
    }
}

impl ShardChainStorage {
    pub fn new(storage: Arc<KeyValueDB>, shard_id: u32) -> Self {
        Self {
            generic_storage: BlockChainStorage::new(storage, ChainId::ShardChain(shard_id)),
            transaction_results: SizedCache::with_size(CACHE_SIZE),
            transaction_addresses: SizedCache::with_size(CACHE_SIZE),
            receipts: SizedCache::with_size(CACHE_SIZE),
            tx_nonce: SizedCache::with_size(CACHE_SIZE),
        }
    }

    /// Records the transaction addresses and the transaction results of the processed block.
    pub fn extend_transaction_results_addresses(
        &mut self,
        block: &SignedShardBlock,
        tx_results: Vec<TransactionResult>,
    ) -> io::Result<()> {
        let transaction_keys: Vec<_> = block
            .body
            .transactions
            .iter()
            .map(|t| self.generic_storage.enc_hash(&t.get_hash()).to_vec())
            .collect();
        let transaction_address_updates: HashMap<Vec<u8>, TransactionAddress> = transaction_keys
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, key)| {
                (key, TransactionAddress { block_hash: block.hash, index: i, shard_id: None })
            })
            .collect();
        extend_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_TRANSACTION_ADDRESSES,
            &mut self.transaction_addresses,
            transaction_address_updates,
        )?;

        let receipt_keys: Vec<_> = block
            .body
            .receipts
            .iter()
            .flat_map(|b| {
                b.receipts.iter().zip(std::iter::repeat(b.shard_id)).enumerate().map(
                    |(i, (r, shard_id))| {
                        (shard_id, i, self.generic_storage.enc_hash(&r.nonce).to_vec())
                    },
                )
            })
            .collect();
        let receipt_address_updates: HashMap<Vec<u8>, TransactionAddress> = receipt_keys
            .iter()
            .cloned()
            .map(|(shard_id, index, key)| {
                (
                    key,
                    TransactionAddress { block_hash: block.hash, index, shard_id: Some(shard_id) },
                )
            })
            .collect();
        extend_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_TRANSACTION_ADDRESSES,
            &mut self.transaction_addresses,
            receipt_address_updates,
        )?;

        let updates: HashMap<Vec<u8>, TransactionResult> = receipt_keys
            .into_iter()
            .map(|(_, _, key)| key)
            .chain(transaction_keys.into_iter())
            .zip(tx_results.into_iter())
            .collect();
        extend_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_TRANSACTION_RESULTS,
            &mut self.transaction_results,
            updates,
        )
    }

    #[inline]
    /// Get transaction address of the computed transaction from its hash.
    pub fn transaction_address(&mut self, hash: &CryptoHash) -> StorageResult<&TransactionAddress> {
        read_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_TRANSACTION_ADDRESSES,
            &mut self.transaction_addresses,
            &self.generic_storage.enc_hash(hash),
        )
    }

    #[inline]
    /// Get transaction hash of the computed transaction from its hash.
    pub fn transaction_result(&mut self, hash: &CryptoHash) -> StorageResult<&TransactionResult> {
        read_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_TRANSACTION_RESULTS,
            &mut self.transaction_results,
            &self.generic_storage.enc_hash(hash),
        )
    }

    #[inline]
    /// Gets trie-specific state by its hash.
    pub fn get_state(&self, hash: &CryptoHash) -> StorageResult<Vec<u8>> {
        self.generic_storage
            .storage
            .get(Some(COL_STATE), &self.generic_storage.enc_hash(hash))
            .map(|a| a.map(|b| b.to_vec()))
    }

    /// Saves state updates in the db.
    pub fn apply_state_updates(
        &self,
        changes: HashMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> std::io::Result<()> {
        let mut db_transaction = self.generic_storage.storage.transaction();
        let col = Some(COL_STATE);
        for (key, value) in changes.into_iter() {
            match value {
                Some(arr) => db_transaction.put_vec(col, &self.generic_storage.enc_slice(&key), arr),
                None => db_transaction.delete(col, &self.generic_storage.enc_slice(&key)),
            }
        }
        self.generic_storage.storage.write(db_transaction)
    }

    #[inline]
    pub fn receipt_block(
        &mut self,
        index: BlockIndex,
        shard_id: ShardId,
    ) -> StorageResult<&ReceiptBlock> {
        read_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_RECEIPT_BLOCK,
            &mut self.receipts,
            &self.generic_storage.enc_index(index),
        )
        .map(|receipts| receipts.and_then(|r| r.get(&shard_id)))
    }

    pub fn extend_receipts(
        &mut self,
        index: BlockIndex,
        receipts: HashMap<ShardId, ReceiptBlock>,
    ) -> io::Result<()> {
        write_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_RECEIPT_BLOCK,
            &mut self.receipts,
            &self.generic_storage.enc_index(index),
            receipts,
        )
    }

    pub fn tx_nonce(&mut self, account_id: AccountId) -> StorageResult<&u64> {
        read_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_TX_NONCE,
            &mut self.tx_nonce,
            &self.generic_storage.enc_slice(&account_id.into_bytes()),
        )
    }

    pub fn extend_tx_nonce(&mut self, tx_nonces: HashMap<AccountId, u64>) -> io::Result<()> {
        let updates: HashMap<_, _> = tx_nonces
            .into_iter()
            .map(|(k, v)| (self.generic_storage.enc_slice(&k.into_bytes()).to_vec(), v))
            .collect();
        extend_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_TX_NONCE,
            &mut self.tx_nonce,
            updates,
        )
    }
}
