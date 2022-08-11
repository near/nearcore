
#[derive(Clone)]
struct OrdMultiSet<V>(im::OrdMap<V,i64>);

impl<V> Default for OrdMultiSet<V> {
    fn default() -> Self { Self(im::OrdMap::new()) }
}

impl<V:Ord+Clone> OrdMultiSet<V> {
    /// adds `n` copies of `v` to the multiset.
    /// Number of copies never goes below 0.
    pub fn add(&mut self, v:V, n:i64) {
        match self.0.entry(v) {
            im::ordmap::Entry::Vacant(e) => if n>0 { e.insert(n); },
            im::ordmap::Entry::Occupied(mut e) => {
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

// The derived order is lexographic and None < Some(_).
// TODO(gprusak): test it.
#[derive(Clone,PartialEq,Eq,PartialOrd,Ord)]
struct Prefix<K:Ord+Clone,V:Ord+Clone>(K,Option<V>);

/// A MultiMap which supports (key,value) duplicates.
#[derive(Clone)]
pub struct OrdMultiMap<K:Ord+Clone,V:Ord+Clone>(OrdMultiSet<Prefix<K,V>>);

impl<K:Ord+Clone,V:Ord+Clone> Default for OrdMultiMap<K,V> {
    fn default() -> Self { Self(OrdMultiSet::default()) }
}

impl<K:Ord+Clone,V:Ord+Clone> OrdMultiMap<K,V> {
    /// adds `n` copies of `(k,v)` to the multimap.
    pub fn add(&mut self, k:K,v:V,n:i64) {
        self.0.add(Prefix(k,Some(v)),n);
    }

    /// Iterates over values associated with key `k`, ignoring duplicates.
    pub fn iter_once_at(&self, start:K) -> impl Iterator<Item=&V> {
        self.0.range_once(Prefix(start.clone(),None)..).map_while(move |Prefix(k,v)| {
            debug_assert!(v.is_some());
            if k==&start { v.as_ref() } else { None }
        })
    }
}

