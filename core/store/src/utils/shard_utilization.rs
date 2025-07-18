use crate::trie::AccessOptions;
use crate::trie::iterator::DiskTrieIteratorInner;
use crate::trie::mem::iter::MemTrieIteratorInner;
use crate::trie::ops::interface::{GenericTrieInternalStorage, GenericTrieNode};
use crate::{NibbleSlice, Trie, TrieUpdate, get, set};
use near_primitives::errors::StorageError;
use near_primitives::shard_utilization::ShardUtilization;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::AccountId;
use std::str::FromStr;

/// Add the given shard utilization to the total amount stored for
/// the given account ID and all its prefixes belonging to the same shard.
/// `shard_left_boundary` is inclusive (it belongs to the shard).
pub fn update_shard_utilization(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
    utilization: ShardUtilization,
    shard_left_boundary: Option<&AccountId>,
) -> Result<(), StorageError> {
    // FIXME: Trie nodes with uneven nibble count might have to be updated as well
    // FIXME: This implementation unnecessarily creates new nodes (extension are broken into branches)
    for prefix in account_prefixes_within_shard(account_id, shard_left_boundary) {
        let trie_key = TrieKey::ShardUtilization { account_id_prefix: prefix.to_vec() };
        let mut account_utilization: ShardUtilization =
            get(state_update, &trie_key)?.unwrap_or_default();
        account_utilization += utilization;
        set(state_update, trie_key, &account_utilization);
    }
    Ok(())
}

fn account_prefixes_within_shard<'a>(
    account_id: &'a AccountId,
    left_shard_boundary: Option<&'a AccountId>,
) -> impl Iterator<Item = &'a [u8]> {
    (0..account_id.len()).filter_map(move |i| {
        let prefix = &account_id.as_str()[..i];
        match left_shard_boundary {
            Some(boundary) if prefix < boundary => None,
            _ => Some(prefix.as_bytes()),
        }
    })
}

/// Find an account ID that splits the shard into two possibly equal parts, according
/// to the shard size utilization.
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
                // FIXME: key_nibbles might have an odd number of nibbles
                return Ok(Some(NibbleSlice::nibbles_to_bytes(&key_nibbles)));
            }
            GenericTrieNode::Extension { extension, child } => {
                key_nibbles.extend(extension);
                current_node_ptr = child;
            }
            GenericTrieNode::Branch { children, value } => {
                let Some(parent_value_ref) = value else { return Ok(None) };
                let parent_utilization = get_value_utilization(&trie_storage, parent_value_ref)?;
                let Some((idx, child)) =
                    find_middle_child(&trie_storage, parent_utilization, *children)?
                else {
                    // FIXME: key_nibbles might have an odd number of nibbles
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
    parent_utilization: ShardUtilization,
    children: [Option<NodePtr>; 16],
) -> Result<Option<(u8, NodePtr)>, StorageError>
where
    NodePtr: Copy,
    TrieStorage: GenericTrieInternalStorage<NodePtr, ValueRef>,
{
    // FIXME: How much shard utilization lies to the right and to the left of the parent
    //        needs to be taken into account as well.
    let mut cumulative_utilization = ShardUtilization::default();
    for i in 0..children.len() {
        let Some(child_ptr) = children[i] else { continue };
        let Some(child_utilization) = get_node_utilization(trie_storage, child_ptr)? else {
            continue;
        };
        cumulative_utilization += child_utilization;
        if cumulative_utilization >= parent_utilization / 2 {
            return Ok(Some((i as u8, child_ptr)));
        }
    }
    Ok(None)
}

fn get_value_utilization<NodePtr, ValueRef, TrieStorage>(
    trie_storage: &TrieStorage,
    value_ref: ValueRef,
) -> Result<ShardUtilization, StorageError>
where
    TrieStorage: GenericTrieInternalStorage<NodePtr, ValueRef>,
{
    let raw_value = trie_storage.get_value(value_ref, AccessOptions::DEFAULT)?;
    borsh::from_slice(&raw_value).map_err(|_| {
        StorageError::StorageInconsistentState(
            "ShardUtilization deserialization failed".to_string(),
        )
    })
}

fn get_node_utilization<NodePtr, ValueRef, TrieStorage>(
    trie_storage: &TrieStorage,
    node_ptr: NodePtr,
) -> Result<Option<ShardUtilization>, StorageError>
where
    TrieStorage: GenericTrieInternalStorage<NodePtr, ValueRef>,
{
    match trie_storage.get_node(node_ptr, AccessOptions::DEFAULT)? {
        GenericTrieNode::Empty => Ok(None),
        GenericTrieNode::Leaf { value, .. } => {
            Ok(Some(get_value_utilization(trie_storage, value)?))
        }
        GenericTrieNode::Extension { child, .. } => get_node_utilization(trie_storage, child),
        GenericTrieNode::Branch { value, .. } => match value {
            Some(value) => Ok(Some(get_value_utilization(trie_storage, value)?)),
            None => Ok(None),
        },
    }
}
