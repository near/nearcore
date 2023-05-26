use indexmap::IndexMap;
use rkyv::Archive;
use std::hash::Hash;

#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
/// See [`IndexMap`]
pub struct ArchivableIndexMap<K: Hash + Ord + Archive, V: Archive> {
    entries: Vec<(K, V)>,
}

impl<K: Hash + Ord + Archive, V: Archive> ArchivedArchivableIndexMap<K, V> {
    pub fn iter(&self) -> core::slice::Iter<'_, (K::Archived, V::Archived)> {
        self.entries.iter()
    }
}

impl<K: Hash + Ord + Archive + Clone, V: Archive> From<IndexMap<K, V>>
    for ArchivableIndexMap<K, V>
{
    fn from(it: IndexMap<K, V>) -> Self {
        let mut r = Self { entries: Vec::new() };
        for (k, v) in it.into_iter() {
            r.entries.push((k, v));
        }
        r
    }
}

impl<K: Hash + Ord + Archive + Clone, V: Archive> Into<IndexMap<K, V>>
    for ArchivableIndexMap<K, V>
{
    fn into(self) -> IndexMap<K, V> {
        let mut r = IndexMap::new();
        for (k, v) in self.entries.into_iter() {
            r.insert(k, v);
        }
        r
    }
}
