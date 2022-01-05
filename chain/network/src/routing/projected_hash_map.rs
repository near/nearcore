use near_primitives::borsh::maybestd::borrow::Borrow;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

pub trait ProjectedMapKey<Key>
where
    Key: Hash,
{
    fn projected_map_key(&self) -> &Key;
}

/// Wraps around `Edge` struct. The main feature of this struct, is that it's hashed by
/// `(Edge::key.0, Edge::key.1)` pair instead of `(Edge::key.0, Edge::key.1, Edge::nonce)`
/// triple.
struct HashMapHelper<T, V>
where
    V: ProjectedMapKey<T>,
    T: Hash + PartialEq + Eq,
{
    inner: V,
    pd: PhantomData<T>,
}

impl<T, V> Borrow<T> for HashMapHelper<T, V>
where
    V: ProjectedMapKey<T>,
    T: Hash + PartialEq + Eq,
{
    fn borrow(&self) -> &T {
        self.inner.projected_map_key()
    }
}

impl<T, V> PartialEq for HashMapHelper<T, V>
where
    V: ProjectedMapKey<T>,
    T: Hash + PartialEq + Eq,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner.projected_map_key() == other.inner.projected_map_key()
    }
}

impl<T, V> Eq for HashMapHelper<T, V>
where
    V: ProjectedMapKey<T>,
    T: Hash + PartialEq + Eq,
{
}

impl<T, V> Hash for HashMapHelper<T, V>
where
    V: ProjectedMapKey<T>,
    T: Hash + PartialEq + Eq,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.projected_map_key().hash(state)
    }
}

pub(crate) struct ProjectedHashMap<T, V>
where
    V: ProjectedMapKey<T>,
    T: Hash + PartialEq + Eq,
{
    pd: PhantomData<T>,
    repr: HashSet<HashMapHelper<T, V>>,
}

impl<T, V> Default for ProjectedHashMap<T, V>
where
    V: ProjectedMapKey<T>,
    T: Hash + PartialEq + Eq,
{
    fn default() -> Self {
        Self { repr: HashSet::new(), pd: PhantomData }
    }
}

impl<T, V> ProjectedHashMap<T, V>
where
    V: ProjectedMapKey<T>,
    T: Hash + PartialEq + Eq,
{
    pub(crate) fn get(&self, key: &T) -> Option<&V> {
        self.repr.get(key).map(|v| &v.inner)
    }

    /// Inserts `value` into the `ProjectedHashMap`.
    /// If the map contained an equal value, the old value is returned
    pub(crate) fn insert(&mut self, value: V) -> Option<V> {
        self.repr.replace(HashMapHelper { inner: value, pd: PhantomData }).map(|x| x.inner)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &V> + '_ {
        self.repr.iter().map(|it| &it.inner)
    }

    #[allow(unused)]
    pub(crate) fn len(&self) -> usize {
        self.repr.len()
    }

    pub(crate) fn remove(&mut self, key: &T) -> bool {
        self.repr.remove(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::routing::projected_hash_map::ProjectedHashMap;
    use crate::routing::Edge;
    use near_primitives::network::PeerId;

    #[test]
    fn test_hashset() {
        let p1 = PeerId::random();
        let p2 = PeerId::random();
        let p3 = PeerId::random();
        let e0 = Edge::make_fake_edge(p1.clone(), p3, 1);
        let e1 = Edge::make_fake_edge(p1.clone(), p2.clone(), 1);
        let e2 = Edge::make_fake_edge(p1.clone(), p2.clone(), 2);
        let e3 = Edge::make_fake_edge(p2, p1, 3);

        let mut se = ProjectedHashMap::default();
        se.insert(e0.clone());
        se.insert(e1);
        se.insert(e2);
        se.insert(e3.clone());

        let key3 = e3.key().clone();
        let key0 = e0.key().clone();

        assert_eq!(se.get(&key3).unwrap(), &e3);
        assert_eq!(se.get(&key0).unwrap(), &e0);
    }

    #[test]
    fn test_remove_key() {
        let p1 = PeerId::random();
        let p2 = PeerId::random();
        let p3 = PeerId::random();
        let p4 = PeerId::random();
        let e1 = Edge::make_fake_edge(p1, p2, 1);
        let e2 = Edge::make_fake_edge(p3, p4, 1);
        let mut se = ProjectedHashMap::default();
        se.insert(e2.clone());

        let key = e1.key().clone();
        se.insert(e1.clone());
        assert_eq!(se.get(&key).unwrap(), &e1);
        se.remove(e1.key());
        assert_eq!(se.get(&key), None);

        let key2 = e2.key().clone();
        assert_eq!(se.get(&key2).unwrap(), &e2);
    }
}
