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
///
/// **NOTE: This is test-only**
pub fn update_shard_utilization(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
    utilization: ShardUtilization,
    shard_left_boundary: Option<&AccountId>,
) -> Result<(), StorageError> {
    // FIXME: This implementation unnecessarily creates new nodes (extension are broken into branches)
    //        It cannot be easily fixed, because `TrieUpdate` interface doesn't give any way to
    //        analyze the trie structure and skip bytes belonging to an extension.
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
    (0..=account_id.len()).filter_map(move |i| {
        let prefix = &account_id.as_str()[..i];
        match left_shard_boundary {
            Some(boundary) if prefix < boundary => None,
            _ => Some(prefix.as_bytes()),
        }
    })
}

pub fn total_utilization(trie: &Trie) -> Result<ShardUtilization, StorageError> {
    match trie.read_lock_memtries() {
        Some(memtries) => {
            let trie_storage = MemTrieIteratorInner::new(&*memtries, trie);
            let Some(root) = trie_storage.get_root() else {
                return Ok(Default::default());
            };
            Ok(get_node_utilization(&trie_storage, root)?.unwrap_or_default())
        }
        None => {
            let trie_storage = DiskTrieIteratorInner::new(trie);
            let Some(root) = trie_storage.get_root() else {
                return Ok(Default::default());
            };
            Ok(get_node_utilization(&trie_storage, root)?.unwrap_or_default())
        }
    }
}

// Two nibbles per byte, and the first two nibbles are discarded
const MIN_NIBBLES_VALID_KEY: usize = AccountId::MIN_LEN * 2 + 2;

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
    // This is done to mitigate cases where a key ending with a separator ('.', '_', '-') is found.
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
        let node = trie_storage.get_node(current_node_ptr, AccessOptions::DEFAULT)?;
        // The current node provides a proper split, no need to check children
        if (left_utilization >= total_utilization / 2)
            && (key_nibbles.len() >= MIN_NIBBLES_VALID_KEY)
        {
            break;
        }

        match node {
            GenericTrieNode::Empty => return Ok(None),
            GenericTrieNode::Leaf { extension, .. } => {
                append_extension_nibbles(&mut key_nibbles, &extension);
                break;
            }
            GenericTrieNode::Extension { extension, child } => {
                append_extension_nibbles(&mut key_nibbles, &extension);
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
                    break;
                };
                key_nibbles.push(idx);
                current_node_ptr = child;
            }
        }
    }
    Ok(Some(nibbles_to_bytes(&key_nibbles)))
}

/// Append nibbles encoded in extension to the given vector
#[inline]
fn append_extension_nibbles(nibbles: &mut Vec<u8>, extension: &[u8]) {
    let (nibble_slice, _) = NibbleSlice::from_encoded(extension);
    nibbles.extend(nibble_slice.iter());
}

/// Convert nibbles to bytes, truncating the last nibble if the slice length is odd.
/// The first two nibbles are skipped (because they contain a prefix).
#[inline]
fn nibbles_to_bytes(nibbles: &[u8]) -> Vec<u8> {
    nibbles.chunks_exact(2).skip(1).map(|pair| (pair[0] << 4) | pair[1]).collect()
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
        if (*left_utilization + child_utilization) > total_utilization / 2 {
            return Ok(Some((i as u8, child_ptr)));
        }
        *left_utilization += child_utilization;
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
    use super::*;
    use crate::test_utils::TestTriesBuilder;
    use crate::trie::update::TrieUpdateResult;
    use crate::{Trie, TrieUpdate};
    use itertools::Itertools;
    use near_primitives::shard_utilization::ShardUtilization;
    use near_primitives::types::{AccountId, StateChangeCause};
    use rand::SeedableRng;
    use rand::distributions::Distribution;
    use rand::distributions::Uniform;
    use rand::rngs::StdRng;

    fn test_trie_inner(
        account_utilization: impl IntoIterator<Item = (AccountId, ShardUtilization)>,
        shard_left_boundary: Option<&AccountId>,
    ) -> Trie {
        // Create a new test trie
        let (shard_tries, layout) =
            TestTriesBuilder::new().with_in_memory_tries(true).with_flat_storage(true).build2();
        let shard_uid = layout.get_shard_uid(0).unwrap();
        let trie = shard_tries.get_trie_for_shard(shard_uid, Trie::EMPTY_ROOT);

        // Add shard utilization per account
        let mut trie_update = TrieUpdate::new(trie);
        for (account_id, utilization) in account_utilization {
            update_shard_utilization(
                &mut trie_update,
                &account_id,
                utilization,
                shard_left_boundary,
            )
            .unwrap();
        }
        trie_update.commit(StateChangeCause::InitialState);
        let TrieUpdateResult { trie_changes, .. } = trie_update.finalize().unwrap();
        let mut store_update = shard_tries.store_update();
        let new_root = shard_tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        shard_tries.apply_memtrie_changes(&trie_changes, shard_uid, 0);
        store_update.commit().unwrap();

        shard_tries.get_trie_for_shard(shard_uid, new_root)
    }

    fn test_trie(
        account_utilization: impl IntoIterator<Item = (AccountId, ShardUtilization)>,
    ) -> Trie {
        test_trie_inner(account_utilization, None)
    }

    fn test_trie_custom_shard(
        account_utilization: impl IntoIterator<Item = (AccountId, ShardUtilization)>,
        shard_left_boundary: &AccountId,
    ) -> Trie {
        test_trie_inner(account_utilization, Some(&shard_left_boundary))
    }

    #[test]
    fn test_prefixes() {
        let account_id: AccountId = "abcd".parse().unwrap();
        let prefixes = account_prefixes_within_shard(&account_id, None).collect_vec();
        let expected: Vec<&[u8]> = vec![b"", b"a", b"ab", b"abc", b"abcd"];
        assert_eq!(prefixes, expected);

        let boundary: AccountId = "ab".parse().unwrap();
        let prefixes = account_prefixes_within_shard(&account_id, Some(&boundary)).collect_vec();
        let expected: Vec<&[u8]> = vec![b"ab", b"abc", b"abcd"];
        assert_eq!(prefixes, expected);
    }

    #[test]
    fn empty_trie() {
        let trie = test_trie(None);
        let result = find_split_account(&trie).unwrap();
        assert!(result.is_none());
        assert_eq!(total_utilization(&trie).unwrap(), ShardUtilization::V1(0.into()));
    }

    #[test]
    fn account_outside_shard() {
        let account_id: AccountId = "abcd".parse().unwrap();
        let shard_left_boundary: AccountId = "bbb".parse().unwrap();
        let utilization = ShardUtilization::V1(100.into());
        let trie = test_trie_custom_shard(vec![(account_id, utilization)], &shard_left_boundary);
        let result = find_split_account(&trie).unwrap();
        // account lies outside the shard
        assert!(result.is_none());
        assert_eq!(total_utilization(&trie).unwrap(), ShardUtilization::V1(0.into()));
    }

    #[test]
    fn single_account() -> anyhow::Result<()> {
        let account_id: AccountId = "abcd".parse()?;
        let utilization = ShardUtilization::V1(100.into());
        let trie = test_trie(vec![(account_id.clone(), utilization)]);
        let result = find_split_account(&trie)?;
        assert_eq!(result, Some(account_id));
        assert_eq!(total_utilization(&trie)?, ShardUtilization::V1(100.into()));
        Ok(())
    }

    #[test]
    fn two_accounts_common_prefix() -> anyhow::Result<()> {
        let account1: AccountId = "abcd".parse()?;
        let account2: AccountId = "abce".parse()?;
        let utilization = ShardUtilization::V1(100.into());
        let trie = test_trie(vec![(account1, utilization), (account2.clone(), utilization)]);
        let result = find_split_account(&trie)?;
        assert_eq!(result, Some(account2));
        assert_eq!(total_utilization(&trie)?, ShardUtilization::V1(200.into()));
        Ok(())
    }

    #[test]
    fn two_accounts_no_common_prefix() -> anyhow::Result<()> {
        let account1: AccountId = "aaaa".parse()?;
        let account2: AccountId = "bbbb".parse()?;
        let utilization = ShardUtilization::V1(100.into());
        let trie = test_trie(vec![(account1, utilization), (account2, utilization)]);
        let result = find_split_account(&trie)?;
        // "bb" is the shortest prefix of account 2 that is a valid account ID
        let split_account: AccountId = "bb".parse()?;
        assert_eq!(result, Some(split_account));
        assert_eq!(total_utilization(&trie)?, ShardUtilization::V1(200.into()));
        Ok(())
    }

    #[test]
    fn accounts_as_prefixes() -> anyhow::Result<()> {
        let account1: AccountId = "aa".parse()?;
        let account2: AccountId = "aaaaa".parse()?;
        let account3: AccountId = "bb".parse()?;
        let account4: AccountId = "bbbbb".parse()?;
        let utilization = ShardUtilization::V1(100.into());
        let trie = test_trie(vec![
            (account1, utilization),
            (account2, utilization),
            (account3.clone(), utilization),
            (account4, utilization),
        ]);
        let result = find_split_account(&trie)?;
        assert_eq!(result, Some(account3));
        assert_eq!(total_utilization(&trie)?, ShardUtilization::V1(400.into()));
        Ok(())
    }

    #[test]
    fn account_updated_twice() -> anyhow::Result<()> {
        let account1: AccountId = "aa".parse()?;
        let account2: AccountId = "bb".parse()?;
        let account3: AccountId = "cc".parse()?;
        let account4: AccountId = "dd".parse()?;
        let utilization = ShardUtilization::V1(100.into());
        let trie = test_trie(vec![
            (account1, utilization),
            (account2, utilization),
            (account3, utilization),
            (account4.clone(), utilization),
            (account4.clone(), utilization * 2),
        ]);
        let result = find_split_account(&trie)?;
        // account4 has exactly half of the total utilization (100 + 200)
        assert_eq!(result, Some(account4));
        assert_eq!(total_utilization(&trie)?, ShardUtilization::V1(600.into()));
        Ok(())
    }

    #[test]
    fn big_random_trie() -> anyhow::Result<()> {
        // Generate 100 random accounts of (random) length between 5 and 10
        let num_accounts = 100;
        let length_dist = Uniform::try_from(5..=10)?;
        let chars = Uniform::try_from('a'..='z')?;
        let mut rng = StdRng::seed_from_u64(12345);
        let mut random_account = || -> AccountId {
            let length = length_dist.sample(&mut rng);
            let account_str: String = chars.sample_iter(&mut rng).take(length).collect();
            account_str.parse().unwrap()
        };
        let mut accounts = (0..num_accounts).map(|_| random_account()).collect_vec();
        accounts.sort();

        // Assign random usage between 100 and 300 to each account
        let utilization_dist = Uniform::try_from(100u64..=300)?;
        let utilizations = utilization_dist.sample_iter(&mut rng).take(num_accounts).collect_vec();
        let total_util: u64 = utilizations.iter().sum();

        // Find the split account by naive algorithm
        let mut split_account = None;
        let mut left_utilization = 0;
        for i in 0..num_accounts {
            if left_utilization + utilizations[i] > total_util / 2 {
                split_account = Some(accounts[i].clone());
                break;
            }
            left_utilization += utilizations[i];
        }

        // Make sure the result is consistent
        let trie = test_trie(std::iter::zip(
            accounts.into_iter(),
            utilizations.into_iter().map(|u| ShardUtilization::V1(u.into())),
        ));
        let result = find_split_account(&trie)?;
        assert_eq!(result, split_account);
        assert_eq!(total_utilization(&trie)?, ShardUtilization::V1(total_util.into()));
        Ok(())
    }
}
