use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_store::KeyForStateChanges;

pub fn get_state_change_shard_id(
    row_key: &[u8],
    trie_key: &TrieKey,
    block_hash: &CryptoHash,
    epoch_id: &EpochId,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<ShardId, near_chain_primitives::error::Error> {
    tracing::info!(target: "get_state_change_shard_id", ?trie_key);
    if let Some(account_id) = get_account_id_from_trie_key(trie_key) {
        let shard_id = epoch_manager.account_id_to_shard_id(&account_id, epoch_id)?;
        Ok(shard_id)
    } else {
        let shard_uid =
            KeyForStateChanges::delayed_receipt_key_decode_shard_uid(row_key, block_hash, trie_key)
                .map_err(|err| near_chain_primitives::error::Error::Other(err.to_string()))?;
        Ok(shard_uid.shard_id as ShardId)
    }
}

fn get_account_id_from_trie_key(trie_key: &TrieKey) -> Option<AccountId> {
    match trie_key {
        TrieKey::Account { account_id, .. } => Some(account_id.clone()),
        TrieKey::ContractCode { account_id, .. } => Some(account_id.clone()),
        TrieKey::AccessKey { account_id, .. } => Some(account_id.clone()),
        TrieKey::ReceivedData { receiver_id, .. } => Some(receiver_id.clone()),
        TrieKey::PostponedReceiptId { receiver_id, .. } => Some(receiver_id.clone()),
        TrieKey::PendingDataCount { receiver_id, .. } => Some(receiver_id.clone()),
        TrieKey::PostponedReceipt { receiver_id, .. } => Some(receiver_id.clone()),
        TrieKey::DelayedReceiptIndices => None,
        TrieKey::DelayedReceipt { .. } => None,
        TrieKey::ContractData { account_id, .. } => Some(account_id.clone()),
    }
}
