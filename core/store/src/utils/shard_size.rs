use crate::trie::AccessOptions;
use crate::trie::iterator::DiskTrieIteratorInner;
use crate::trie::mem::iter::MemTrieIteratorInner;
use crate::trie::ops::interface::{GenericTrieInternalStorage, GenericTrieNode};
use crate::{NibbleSlice, Trie, TrieUpdate, get, set};
use near_primitives::errors::StorageError;
use near_primitives::shard_size_contribution::ShardSizeContribution;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::AccountId;
use std::str::FromStr;

/// Add the given shard size contribution to the total amount stored for
/// the given account ID and all its prefixes.
pub fn update_contribution(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
    contribution: ShardSizeContribution,
) -> Result<(), StorageError> {
    for i in 0..=account_id.len() {
        let prefix = &account_id.as_bytes()[..i];
        let trie_key = TrieKey::ShardSizeContribution { account_id_prefix: prefix.to_vec() };
        let mut account_contribution: ShardSizeContribution =
            get(state_update, &trie_key)?.unwrap_or_default();
        account_contribution += contribution;
        set(state_update, trie_key, &account_contribution);
    }
    Ok(())
}

/// Find an account ID that splits the shard into two possibly equal parts, according
/// to the shard size contribution.
/// Returns `None` if the trie is empty or an empty node is found.
pub fn find_split_account(trie: &Trie) -> Result<Option<AccountId>, StorageError> {
    // Find the split key
    let key = match trie.read_lock_memtries() {
        Some(memtries) => find_split_key(MemTrieIteratorInner::new(&*memtries, trie))?,
        None => find_split_key(DiskTrieIteratorInner::new(trie))?,
    };
    let Some(key) = key else {
        return Ok(None);
    };

    // Convert the key to account ID by finding the longest prefix that is a valid ID
    for i in 0..key.len() {
        let prefix = std::str::from_utf8(&key[..(key.len() - i)]).expect("trie key is non-ASCII");
        if let Ok(account_id) = AccountId::from_str(prefix) {
            return Ok(Some(account_id));
        }
    }
    Ok(None)
}

fn find_split_key<NodePtr, ValueRef, TrieStorage>(
    trie_storage: TrieStorage,
) -> Result<Option<Vec<u8>>, StorageError>
where
    NodePtr: Copy,
    TrieStorage: GenericTrieInternalStorage<NodePtr, ValueRef>,
{
    let mut key_nibbles = Vec::with_capacity(AccountId::MAX_LEN * 2); // x2 because nibbles are half-bytes
    let Some(mut current_node_ptr) = trie_storage.get_root() else {
        return Ok(None);
    };
    loop {
        match trie_storage.get_node(current_node_ptr, AccessOptions::DEFAULT)? {
            GenericTrieNode::Empty => return Ok(None),
            GenericTrieNode::Leaf { extension, .. } => {
                key_nibbles.extend(extension);
                return Ok(Some(NibbleSlice::nibbles_to_bytes(&key_nibbles)));
            }
            GenericTrieNode::Extension { extension, child } => {
                key_nibbles.extend(extension);
                current_node_ptr = child;
            }
            GenericTrieNode::Branch { children, value } => {
                let Some(parent_value_ref) = value else { return Ok(None) };
                let parent_contribution = get_value_contribution(&trie_storage, parent_value_ref)?;
                let Some((idx, child)) =
                    find_middle_child(&trie_storage, parent_contribution, *children)?
                else {
                    return Ok(Some(NibbleSlice::nibbles_to_bytes(&key_nibbles)));
                };
                key_nibbles.push(idx);
                current_node_ptr = child;
            }
        }
    }
}

fn find_middle_child<NodePtr, ValueRef, TrieStorage>(
    trie_storage: &TrieStorage,
    parent_contribution: ShardSizeContribution,
    children: [Option<NodePtr>; 16],
) -> Result<Option<(u8, NodePtr)>, StorageError>
where
    NodePtr: Copy,
    TrieStorage: GenericTrieInternalStorage<NodePtr, ValueRef>,
{
    let mut cumulative_contribution = ShardSizeContribution::default();
    for i in 0..children.len() {
        let Some(child_ptr) = children[i] else { continue };
        let Some(child_contribution) = get_node_contribution(trie_storage, child_ptr)? else {
            continue;
        };
        cumulative_contribution += child_contribution;
        if cumulative_contribution >= parent_contribution / 2 {
            return Ok(Some((i as u8, child_ptr)));
        }
    }
    Ok(None)
}

fn get_value_contribution<NodePtr, ValueRef, TrieStorage>(
    trie_storage: &TrieStorage,
    value_ref: ValueRef,
) -> Result<ShardSizeContribution, StorageError>
where
    TrieStorage: GenericTrieInternalStorage<NodePtr, ValueRef>,
{
    let raw_value = trie_storage.get_value(value_ref, AccessOptions::DEFAULT)?;
    borsh::from_slice(&raw_value).map_err(|_| {
        StorageError::StorageInconsistentState(
            "ShardSizeContribution deserialization failed".to_string(),
        )
    })
}

fn get_node_contribution<NodePtr, ValueRef, TrieStorage>(
    trie_storage: &TrieStorage,
    node_ptr: NodePtr,
) -> Result<Option<ShardSizeContribution>, StorageError>
where
    TrieStorage: GenericTrieInternalStorage<NodePtr, ValueRef>,
{
    match trie_storage.get_node(node_ptr, AccessOptions::DEFAULT)? {
        GenericTrieNode::Empty => Ok(None),
        GenericTrieNode::Leaf { value, .. } => {
            Ok(Some(get_value_contribution(trie_storage, value)?))
        }
        GenericTrieNode::Extension { child, .. } => get_node_contribution(trie_storage, child),
        GenericTrieNode::Branch { value, .. } => match value {
            Some(value) => Ok(Some(get_value_contribution(trie_storage, value)?)),
            None => Ok(None),
        },
    }
}
