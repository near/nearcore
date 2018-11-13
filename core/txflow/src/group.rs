use std::collections::{HashMap, HashSet};
use message::Message;
use primitives::traits::Payload;
use primitives::types::UID;

/// A group of messages associated that satisfy certain criteria.
/// Examples:
/// * representative message(s) of epoch X (there are more than one if there is a fork);
/// * kickout message(s) that kickout epoch X (again, more than one if there is a fork);
/// * messages of epoch X (it is perfectly normal to have multiple of them).
#[derive(Debug)]
pub struct Group<'a, P: 'a + Payload> {
    /// Messages aggregated by owner uid.
    pub messages_by_owner: HashMap<UID, HashSet<&'a Message<'a, P>>>,
    pub v: HashSet<&'a Message<'a, P>>,
}

// TODO: Create alternative of Group called SingleOwnerGroup with {owner_uid: u64, messages: HashSet<...>}
// that is more efficient to use for representative messages and kickout messages.
impl<'a, P: 'a + Payload> Group<'a, P> {
    pub fn new() -> Self {
        Group {
            messages_by_owner: HashMap::new(),
            v: HashSet::new(),
        }
    }

    pub fn insert(&mut self, message: &'a Message<'a, P>) {
        let owner_uid = message.data.body.owner_uid;
        self.messages_by_owner.entry(owner_uid).or_insert_with(|| HashSet::new()).insert(message);
    }

    pub fn union_update(&mut self, other: &Self) {
        for (owner_uid, per_owner) in &other.messages_by_owner {
            self.messages_by_owner.entry(*owner_uid).or_insert_with(|| HashSet::new())
                .extend(per_owner);
        }
    }

    pub fn contains_owner(&self, owner_uid: &UID) -> bool {
        self.messages_by_owner.contains_key(owner_uid)
    }

    pub fn filter_by_owner(&self, owner_uid: UID) -> Option<&HashSet<&Message<P>>> {
        self.messages_by_owner.get(&owner_uid)
    }
}

impl<'a, P: 'a + Payload> Clone for Group<'a, P> {
    fn clone(&self) -> Self {
        Group {
            messages_by_owner: self.messages_by_owner.clone(),
            v: HashSet::new()
        }
    }
}

/// Mapping of groups to epochs.
#[derive(Debug)]
pub struct GroupsPerEpoch<'a, P: 'a + Payload> {
    pub messages_by_epoch: HashMap<u64, Group<'a, P>>,
}

impl<'a, P: 'a + Payload> GroupsPerEpoch<'a, P> {
    pub fn new() -> Self {
        GroupsPerEpoch {
            messages_by_epoch: HashMap::new(),
        }
    }

    pub fn insert(&mut self, epoch: u64, message: &'a Message<'a, P>) {
        self.messages_by_epoch.entry(epoch).or_insert_with(|| Group::new()).insert(message);
    }

    pub fn union_update(&mut self, other: &Self) {
        for (epoch, per_epoch) in &other.messages_by_epoch {
           self.messages_by_epoch.entry(*epoch).or_insert_with(|| Group::new())
               .union_update(per_epoch);
        }
    }

    /// Filters out messages not owned by the given owner.
    /// Returns pairs: epoch -> messages of that owner in the given epoch.
    pub fn filter_by_owner(&'a self, owner_uid: UID) -> impl Iterator<Item=(&u64, &'a HashSet<&'a Message<'a, P>>)> {
        (&self.messages_by_epoch).into_iter().filter_map(move |(epoch, per_epoch)|
            match per_epoch.filter_by_owner(owner_uid) {
                None => None,
                Some(filter_epoch_messages) => Some((epoch, filter_epoch_messages))
            })
    }

    pub fn filter_by_epoch(&self, epoch: u64) -> Option<&Group<'a, P>> {
        self.messages_by_epoch.get(&epoch)
    }

    pub fn contains_epoch(&self, epoch: u64) -> bool {
        self.messages_by_epoch.contains_key(&epoch)
    }
}
