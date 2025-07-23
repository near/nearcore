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
    // FIXME: Key prefixes are full-bytes only, so trie nodes lying at odd depth (counting nibbles)
    //        will not be updated. This makes searching for boundary suboptimal.
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
    let Some(mut current_node_ptr) = trie_storage.get_root() else {
        return Ok(None);
    };
    let Some(total_utilization) = get_node_utilization(&trie_storage, current_node_ptr)? else {
        return Ok(None);
    };
    let mut left_utilization = ShardUtilization::default();
    let mut key_nibbles = Vec::with_capacity(AccountId::MAX_LEN * 2); // x2 because nibbles are half-bytes
    loop {
        match trie_storage.get_node(current_node_ptr, AccessOptions::DEFAULT)? {
            GenericTrieNode::Empty => return Ok(None),
            GenericTrieNode::Leaf { extension, .. } => {
                key_nibbles.extend(extension);
                return Ok(Some(nibbles_to_bytes(&key_nibbles)));
            }
            GenericTrieNode::Extension { extension, child } => {
                key_nibbles.extend(extension);
                current_node_ptr = child;
            }
            GenericTrieNode::Branch { children, .. } => {
                let Some((idx, child)) = find_middle_child(
                    &trie_storage,
                    total_utilization,
                    &mut left_utilization,
                    *children,
                )?
                else {
                    return Ok(Some(nibbles_to_bytes(&key_nibbles)));
                };
                key_nibbles.push(idx);
                current_node_ptr = child;
            }
        }
    }
}

/// Convert nibbles to bytes, truncating the last nibble if the slice length is odd.
fn nibbles_to_bytes(mut nibbles: &[u8]) -> Vec<u8> {
    if nibbles.len() % 2 != 0 {
        nibbles = &nibbles[..(nibbles.len() - 1)];
    }
    NibbleSlice::nibbles_to_bytes(&nibbles)
}

fn find_middle_child<NodePtr, ValueRef, TrieStorage>(
    trie_storage: &TrieStorage,
    total_utilization: ShardUtilization,
    left_utilization: &mut ShardUtilization,
    children: [Option<NodePtr>; 16],
) -> Result<Option<(u8, NodePtr)>, StorageError>
where
    NodePtr: Copy,
    TrieStorage: GenericTrieInternalStorage<NodePtr, ValueRef>,
{
    for i in 0..children.len() {
        let Some(child_ptr) = children[i] else { continue };
        let Some(child_utilization) = get_node_utilization(trie_storage, child_ptr)? else {
            continue;
        };
        *left_utilization += child_utilization;
        if *left_utilization > total_utilization / 2 {
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
        GenericTrieNode::Branch { value, children } => match value {
            Some(value) => Ok(Some(get_value_utilization(trie_storage, value)?)),
            // If a branch node doesn't represent any key (i.e. has odd nibble depth), it might not have a value
            // associated. Then we have to go one level down and sum the utilization for all children.
            None => get_total_children_utilization(trie_storage, *children),
        },
    }
}

fn get_total_children_utilization<NodePtr, ValueRef, TrieStorage>(
    trie_storage: &TrieStorage,
    children: [Option<NodePtr>; 16],
) -> Result<Option<ShardUtilization>, StorageError>
where
    TrieStorage: GenericTrieInternalStorage<NodePtr, ValueRef>,
{
    let mut total_utilization = None;
    for child in children {
        let Some(child) = child else { continue };
        let child_utilization = get_node_utilization(trie_storage, child)?;
        let Some(child_utilization) = child_utilization else { continue };
        match &mut total_utilization {
            None => total_utilization = Some(child_utilization),
            Some(total_utilization) => *total_utilization += child_utilization,
        }
    }
    Ok(total_utilization)
}

#[cfg(test)]
mod tests {
    use crate::shard_utilization::{find_split_account, update_shard_utilization};
    use crate::test_utils::TestTriesBuilder;
    use crate::trie::update::TrieUpdateResult;
    use crate::{Trie, TrieUpdate};
    use near_primitives::shard_utilization::ShardUtilization;
    use near_primitives::types::{AccountId, StateChangeCause};

    fn test_trie(
        account_utilization: impl IntoIterator<Item = (AccountId, ShardUtilization)>,
    ) -> Trie {
        // Create a new test trie
        let (shard_tries, layout) =
            TestTriesBuilder::new().with_in_memory_tries(true).with_flat_storage(true).build2();
        let shard_uid = layout.get_shard_uid(0).unwrap();
        let trie = shard_tries.get_trie_for_shard(shard_uid, Trie::EMPTY_ROOT);

        // Add shard utilization per account
        let mut trie_update = TrieUpdate::new(trie);
        for (account_id, utilization) in account_utilization.into_iter() {
            update_shard_utilization(&mut trie_update, &account_id, utilization, None).unwrap();
        }
        trie_update.commit(StateChangeCause::InitialState);
        let TrieUpdateResult { trie, trie_changes, .. } = trie_update.finalize().unwrap();
        let mut store_update = shard_tries.store_update();
        shard_tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit().unwrap();

        trie
    }

    #[test]
    fn empty_trie() {
        let trie = test_trie(None);
        let result = find_split_account(&trie).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn single_account() -> anyhow::Result<()> {
        let account_id: AccountId = "abcd".parse()?;
        let utilization = ShardUtilization::V1(1.into());
        let trie = test_trie(vec![(account_id.clone(), utilization)]);
        assert_ne!(*trie.get_root(), Trie::EMPTY_ROOT);
        let result = find_split_account(&trie)?;
        assert_eq!(result, Some(account_id));
        Ok(())
    }
}
