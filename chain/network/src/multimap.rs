use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

struct OrdMultiSet<V>(BTreeMap<V,i64>);

impl<V> Default for OrdMultiSet<V> {
    fn default() -> Self { Self(BTreeMap::new()) }
}

impl<V:Ord> OrdMultiSet<V> {
    /// adds `n` copies of `v` to the multiset.
    /// Number of copies never goes below 0.
    pub fn add(&mut self, v:V, n:i64) {
        match self.0.entry(v) {
            Entry::Vacant(e) => if n>0 { e.insert(n); },
            Entry::Occupied(mut e) => {
                *e.get_mut() += n;
                if *e.get() <= 0 {
                    e.remove_entry();
                }
            }
        }
    }

    /// Iterate over elements in a range, ignoring duplicates.
    pub fn range_once(&self, range: impl std::ops::RangeBounds<V>) -> impl Iterator<Item=&V> {
        self.0.range(range).map(|(v,_)|v)
    }
}

#[derive(Eq,PartialEq)]
enum Prefix<K:Eq,V:Eq> {
    Key(K),
    KeyValue(K,V),
}

impl<K:Eq,V:Eq> Prefix<K,V> {
    fn pair(&self) -> (&K,Option<&V>) {
        match self {
            Prefix::Key(k) => (k,None),
            Prefix::KeyValue(k,v) => (k,Some(v)),
        }
    }
}

impl<K:Ord,V:Ord> std::cmp::PartialOrd for Prefix<K,V> {
    fn partial_cmp(&self, other:&Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K:Ord,V:Ord> std::cmp::Ord for Prefix<K,V> {
    fn cmp(&self, other:&Self) -> std::cmp::Ordering {
        // Note that None < Some(_) and pairs are compared lexicographically.
        self.pair().cmp(&other.pair())
    }
}

/// A MultiMap which supports (key,value) duplicates.
pub struct OrdMultiMap<K:Eq,V:Eq>(OrdMultiSet<Prefix<K,V>>);

impl<K:Eq,V:Eq> Default for OrdMultiMap<K,V> {
    fn default() -> Self { Self(OrdMultiSet::default()) }
}

impl<K:Ord+Clone,V:Ord> OrdMultiMap<K,V> {
    /// adds `n` copies of `(k,v)` to the multimap.
    pub fn add(&mut self, k:K,v:V,n:i64) {
        self.0.add(Prefix::KeyValue(k,v),n);
    }

    /// Iterates over values associated with key `k`, ignoring duplicates.
    pub fn iter_once_at(&self, start:K) -> impl Iterator<Item=&V> {
        self.0.range_once(Prefix::Key(start.clone())..).map_while(move |p|match p {
            Prefix::KeyValue(k,v) => if k==&start { Some(v) } else { None }
            Prefix::Key(_) => unreachable!(""),
        })
    }
}

