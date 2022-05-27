use smart_default::SmartDefault;
/// Generic MultiSet implementation based on a HashMap.
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug, SmartDefault, PartialEq, Eq)]
pub struct MultiSet<T: Hash + Eq>(HashMap<T, usize>);

impl<T: Hash + Eq> MultiSet<T> {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert(&mut self, val: T) {
        *self.0.entry(val).or_default() += 1;
    }

    pub fn is_subset(&self, other: &Self) -> bool {
        for (k, v) in &self.0 {
            if other.0.get(&k).unwrap_or(&0) < &v {
                return false;
            }
        }
        true
    }
}

impl<T: Hash + Eq> FromIterator<T> for MultiSet<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut s = Self::new();
        for v in iter {
            s.insert(v);
        }
        s
    }
}
