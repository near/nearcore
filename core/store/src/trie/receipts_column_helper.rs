use near_primitives::errors::StorageError;
use near_primitives::receipt::{Receipt, ShardBufferedReceiptIndices};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::ShardId;

use crate::{get, TrieAccess};

/// Read-only iterator over receipt queues stored in the state trie.
pub struct ReceiptIterator<'a> {
    trie_keys: Box<dyn Iterator<Item = TrieKey>>,
    trie: &'a dyn TrieAccess,
}

impl<'a> ReceiptIterator<'a> {
    pub fn delayed_receipts(trie: &'a dyn TrieAccess) -> Result<Self, StorageError> {
        let indices = crate::get_delayed_receipt_indices(trie)?;
        Ok(Self {
            trie_keys: Box::new(
                (indices.first_index..indices.next_available_index)
                    .map(move |index| TrieKey::DelayedReceipt { index }),
            ),
            trie,
        })
    }

    /// Iterates over all receipts in any receipt buffer.
    pub fn buffered_receipts(trie: &'a dyn TrieAccess) -> Result<Self, StorageError> {
        let all_indices = crate::get_buffered_receipt_indices(trie)?;
        let trie_keys_iter =
            all_indices.shard_buffer_indices.into_iter().flat_map(|(shard, indices)| {
                (indices.first_index..indices.next_available_index)
                    .map(move |index| TrieKey::BufferedReceipt { index, receiving_shard: shard })
            });
        Ok(Self { trie_keys: Box::new(trie_keys_iter), trie })
    }

    /// Iterates over receipts in a receipt buffer to a specific receiver shard.
    pub fn shard_buffered_receipts(
        trie: &'a dyn TrieAccess,
        receiving_shard: ShardId,
        indices: &ShardBufferedReceiptIndices,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            trie_keys: Box::new(
                (indices.first_index..indices.next_available_index)
                    .map(move |index| TrieKey::BufferedReceipt { index, receiving_shard }),
            ),
            trie,
        })
    }
}

impl<'a> Iterator for ReceiptIterator<'a> {
    type Item = Result<Receipt, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.trie_keys.next()?;
        let result = match get(self.trie, &key) {
            Err(e) => Err(e),
            Ok(None) => Err(StorageError::StorageInconsistentState(
                "Receipt referenced by index should be in the state".to_owned(),
            )),
            Ok(Some(receipt)) => Ok(receipt),
        };
        Some(result)
    }
}
