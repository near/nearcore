use std::hash::Hash;
use std::collections::HashMap;
use super::message::MessageWeakRef;

/// A group of messages associated with an epoch that satisfy certain criteria.
/// Examples:
/// * representative message(s) of epoch X (there are more than one if there is a fork);
/// * kickout message(s) that kickout epoch X (again, more than one if there is a fork);
/// * messages of epoch X (it is perfectly normal to have multiple of them).
#[derive(Debug)]
pub struct EpochMessages<T: Hash> {
    /// Messages aggregated by hash.
    pub messages_by_hash: HashMap<u64, MessageWeakRef<T>>,
    /// Messages aggregated by owner uid.
    pub messages_by_owner: HashMap<u64, HashMap<u64, MessageWeakRef<T>>>,
}

impl<T: Hash> EpochMessages<T> {
    pub fn new() -> EpochMessages<T> {
        EpochMessages {
            messages_by_hash: HashMap::new(),
            messages_by_owner: HashMap::new(),
        }
    }

    pub fn insert(&mut self, owner_uid: u64, hash: u64, message: &MessageWeakRef<T>) {
        self.messages_by_hash.insert(hash, message.clone());
        self.messages_by_owner.entry(owner_uid).or_insert_with(|| HashMap::new())
            .insert(hash, message.clone());
    }

    pub fn union_update(&mut self, other: &EpochMessages<T>) {
        {
            self.messages_by_hash.extend((&other.messages_by_hash).into_iter()
                .map(|(k,v)| (k.clone(), v.clone())));
        }
        for (owner_uid, per_owner) in &other.messages_by_owner {
            self.messages_by_owner.entry(*owner_uid).or_insert_with(|| HashMap::new()).extend(
                per_owner.into_iter().map(|(k,v)| (k.clone(), v.clone())));
        }
    }

    pub fn contains_owner(&self, owner_uid: &u64) -> bool {
        self.messages_by_owner.contains_key(owner_uid)
    }

    pub fn filter_by_owner(&self, owner_uid: u64) -> Option<&HashMap<u64, MessageWeakRef<T>>> {
        self.messages_by_owner.get(&owner_uid)
    }
}

/// Aggregates EpochMessages for all epochs.
#[derive(Debug)]
pub struct AllEpochMessages<T: Hash> {
    messages_by_epoch: HashMap<u64, EpochMessages<T>>,
}

impl<'a, T: Hash> AllEpochMessages<T> {
    pub fn new() -> AllEpochMessages<T> {
        AllEpochMessages {
            messages_by_epoch: HashMap::new(),
        }
    }

    pub fn insert(&mut self, epoch: u64, owner_uid: u64, hash: u64, message: &MessageWeakRef<T>) {
        self.messages_by_epoch.entry(epoch).or_insert_with(|| EpochMessages::new())
            .insert(owner_uid, hash, message);
    }

    pub fn union_update(&mut self, other: &AllEpochMessages<T>) {
        for (epoch, per_epoch) in &other.messages_by_epoch {
           self.messages_by_epoch.entry(*epoch).or_insert_with(|| EpochMessages::new())
               .union_update(per_epoch);
        }
    }

    pub fn filter_by_owner(&self, owner_uid: u64) -> impl Iterator<Item=(&u64, &HashMap<u64, MessageWeakRef<T>>)> {
        (&self.messages_by_epoch).into_iter().filter_map(move |(epoch, per_epoch)|
            match per_epoch.filter_by_owner(owner_uid) {
                None => None,
                Some(filter_epoch_messages) => Some((epoch, filter_epoch_messages))
            })
    }

    pub fn filter_by_epoch(&self, epoch: u64) -> Option<&EpochMessages<T>> {
        self.messages_by_epoch.get(&epoch)
    }

    pub fn contains_epoch(&self, epoch: u64) -> bool {
        self.messages_by_epoch.contains_key(&epoch)
    }

    pub fn num_epochs(&self) -> usize {
        self.messages_by_epoch.len()
    }
}
