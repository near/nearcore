use std::hash::Hash;
use std::collections::{HashMap, HashSet};
use super::message::MessageWeakRef;
use super::hashable_message::HashableMessage;

/// A group of messages associated with a single epoch that satisfy certain criteria.
/// Examples:
/// * representative message(s) of epoch X (there are more than one if there is a fork);
/// * kickout message(s) that kickout epoch X (again, more than one if there is a fork);
/// * messages of epoch X (it is perfectly normal to have multiple of them).
#[derive(Debug)]
pub struct Group<T: Hash> {
    /// Messages aggregated by owner uid.
    pub messages_by_owner: HashMap<u64, HashSet<HashableMessage<T>>>,
}

// TODO: Create alternative of Group called SingleOwnerGroup with {owner_uid: u64, messages: HashSet<...>}
// that is more efficient to use for representative messages and kickout messages.
impl<T: Hash> Group<T> {
    pub fn new() -> Self {
        Group {
            messages_by_owner: HashMap::new(),
        }
    }

    pub fn insert(&mut self, owner_uid: u64, hash: u64, message: &MessageWeakRef<T>) {
        self.messages_by_owner.entry(owner_uid).or_insert_with(|| HashSet::new())
            .insert(HashableMessage{hash, message: message.clone()});
    }

    pub fn union_update(&mut self, other: &Self) {
        for (owner_uid, per_owner) in &other.messages_by_owner {
            self.messages_by_owner.entry(*owner_uid).or_insert_with(|| HashSet::new()).extend(
                per_owner.into_iter().map(|m| m.clone()));
        }
    }

    pub fn contains_owner(&self, owner_uid: &u64) -> bool {
        self.messages_by_owner.contains_key(owner_uid)
    }

    pub fn filter_by_owner(&self, owner_uid: u64) -> Option<&HashSet<HashableMessage<T>>> {
        self.messages_by_owner.get(&owner_uid)
    }
}

impl<T: Hash> Clone for Group<T> {
    fn clone(&self) -> Self {
        Group {
            messages_by_owner: self.messages_by_owner.clone()
        }
    }
}

/// Mapping of groups to epochs.
#[derive(Debug)]
pub struct GroupsPerEpoch<T: Hash> {
    pub messages_by_epoch: HashMap<u64, Group<T>>,
}

impl<T: Hash> GroupsPerEpoch<T> {
    pub fn new() -> Self {
        GroupsPerEpoch {
            messages_by_epoch: HashMap::new(),
        }
    }

    pub fn insert(&mut self, epoch: u64, owner_uid: u64, hash: u64, message: &MessageWeakRef<T>) {
        self.messages_by_epoch.entry(epoch).or_insert_with(|| Group::new())
            .insert(owner_uid, hash, message);
    }

    pub fn union_update(&mut self, other: &Self) {
        for (epoch, per_epoch) in &other.messages_by_epoch {
           self.messages_by_epoch.entry(*epoch).or_insert_with(|| Group::new())
               .union_update(per_epoch);
        }
    }

    /// Filters out messages not owned by the given owner.
    /// Returns pairs: epoch -> messages of that owner in the given epoch.
    pub fn filter_by_owner(&self, owner_uid: u64) -> impl Iterator<Item=(&u64, &HashSet<HashableMessage<T>>)> {
        (&self.messages_by_epoch).into_iter().filter_map(move |(epoch, per_epoch)|
            match per_epoch.filter_by_owner(owner_uid) {
                None => None,
                Some(filter_epoch_messages) => Some((epoch, filter_epoch_messages))
            })
    }

    /// Filters out epochs that are present in another GroupsByEpoch.
    pub fn difference_by_epoch<'a>(&self, other: &Self) -> Self {
        let mut result = Self::new();
        for (epoch, group) in &self.messages_by_epoch {
            if !other.contains_epoch(*epoch) {
                result.messages_by_epoch.insert(*epoch, group.clone());
            }
        }
        result
    }

    pub fn filter_by_epoch(&self, epoch: u64) -> Option<&Group<T>> {
        self.messages_by_epoch.get(&epoch)
    }

    pub fn contains_epoch(&self, epoch: u64) -> bool {
        self.messages_by_epoch.contains_key(&epoch)
    }

    pub fn num_epochs(&self) -> usize {
        self.messages_by_epoch.len()
    }
}
