//! [`CryptoHash`](crate::hash::CryptoHash) is already a hashed representation of a key, so it is
//! no longer necessary to hash it again for use in HashMap as a key.
//!
//! Save the effort and use the `CryptoHash` itself as the hashed representation.
//!
//! DOS safe in so far as CryptoHash is difficult to find collisions for.

// Note for future implementors: you can roughly copy the implementations of the hashbrown crate's
// HashMap implementation for the method you want to add...

use crate::hash::CryptoHash;
use hashbrown::hash_table::HashTable;

/// A hashmap that stores `V`s keyed by a `CryptoHash`.
#[derive(Clone)]
pub struct CryptoHashMap<V> {
    table: HashTable<(CryptoHash, V)>,
}

impl<V> Default for CryptoHashMap<V> {
    fn default() -> Self {
        Self { table: Default::default() }
    }
}

fn u64hash(hash: &CryptoHash) -> u64 {
    let [.., a, b, c, d, e, f, g, h] = hash.as_bytes();
    u64::from_be_bytes([*a, *b, *c, *d, *e, *f, *g, *h])
}

fn eq<V>(expected: &CryptoHash, entry: &(CryptoHash, V)) -> bool {
    &entry.0 == expected
}

fn hasher<V>(entry: &(CryptoHash, V)) -> u64 {
    u64hash(&entry.0)
}

impl<V> CryptoHashMap<V> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self { table: HashTable::with_capacity(cap) }
    }

    pub fn iter(&self) -> hashbrown::hash_table::Iter<'_, (CryptoHash, V)> {
        self.table.iter()
    }

    pub fn entry<'a>(&'a mut self, key: CryptoHash) -> Entry<'a, V> {
        match self.table.entry(u64hash(&key), |e| eq(&key, e), hasher) {
            hashbrown::hash_table::Entry::Vacant(v) => Entry::Vacant(VacantEntry { key, entry: v }),
            hashbrown::hash_table::Entry::Occupied(o) => {
                Entry::Occupied(OccupiedEntry { entry: o })
            }
        }
    }

    pub fn insert(&mut self, key: CryptoHash, value: V) -> &mut V {
        match self.entry(key) {
            Entry::Occupied(o) => {
                let v = o.into_mut();
                *v = value;
                v
            }
            Entry::Vacant(v) => &mut v.entry.insert((key, value)).into_mut().1,
        }
    }

    pub fn get(&self, key: &CryptoHash) -> Option<&V> {
        self.table.find(u64hash(&key), |v| eq(&key, v)).map(|(_, v)| v)
    }

    pub fn get_mut(&mut self, key: &CryptoHash) -> Option<&mut V> {
        self.table.find_mut(u64hash(&key), |v| eq(&key, v)).map(|(_, v)| v)
    }

    pub fn contains_key(&self, key: &CryptoHash) -> bool {
        self.get(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn remove(&mut self, key: &CryptoHash) -> Option<V> {
        match self.entry(*key) {
            Entry::Occupied(o) => Some(o.entry.remove().0 .1),
            Entry::Vacant(_) => None,
        }
    }
}

impl<V> IntoIterator for CryptoHashMap<V> {
    type Item = (CryptoHash, V);

    type IntoIter = hashbrown::hash_table::IntoIter<(CryptoHash, V)>;

    fn into_iter(self) -> Self::IntoIter {
        self.table.into_iter()
    }
}

pub enum Entry<'a, V> {
    Occupied(OccupiedEntry<'a, V>),
    Vacant(VacantEntry<'a, V>),
}

impl<'a, V> Entry<'a, V> {
    pub fn or_default(self) -> &'a mut V
    where
        V: Default,
    {
        match self {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => v.insert(Default::default()).into_mut(),
        }
    }

    pub fn or_insert(self, value: V) -> &'a mut V
    where
        V: Default,
    {
        match self {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => v.insert(value).into_mut(),
        }
    }
}

pub struct OccupiedEntry<'a, V> {
    entry: hashbrown::hash_table::OccupiedEntry<'a, (CryptoHash, V)>,
}

impl<'a, V> OccupiedEntry<'a, V> {
    pub fn get(&self) -> &V {
        &self.entry.get().1
    }

    pub fn get_mut(&mut self) -> &mut V {
        &mut self.entry.get_mut().1
    }

    pub fn into_mut(self) -> &'a mut V {
        &mut self.entry.into_mut().1
    }

    pub fn remove(self) -> V {
        self.entry.remove().0 .1
    }
}

pub struct VacantEntry<'a, V> {
    key: CryptoHash,
    entry: hashbrown::hash_table::VacantEntry<'a, (CryptoHash, V)>,
}

impl<'a, V> VacantEntry<'a, V> {
    pub fn insert(self, v: V) -> OccupiedEntry<'a, V> {
        OccupiedEntry { entry: self.entry.insert((self.key, v)) }
    }
}
