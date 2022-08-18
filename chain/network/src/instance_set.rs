use im::hashmap::Entry;
use std::sync::{Arc,Mutex};
use std::fmt;
use std::hash::Hash;

/// A multiset of elements with guarded lifetimes.
#[derive(Clone)]
pub struct InstanceSet<T>(Arc<Mutex<im::HashMap<T,usize>>>);

/// Guard of an element of the InstanceSet.
/// Removes itself from the set when dropped.
pub struct Instance<T:Clone+Hash+Eq>(T,InstanceSet<T>);

impl<T:Clone+Hash+Eq> InstanceSet<T> {
    pub fn new() -> Self { Self(Arc::new(Mutex::new(im::HashMap::new()))) } 
    pub fn insert(&self, v:&T) -> Instance<T> {
        *self.0.lock().unwrap().entry(v.clone()).or_default() += 1;
        Instance(v.clone(),self.clone())
    }
    pub fn load(&self) -> im::HashMap<T,usize> {
        self.0.lock().unwrap().clone()
    }
}

impl<T:fmt::Debug+Clone+Hash+Eq> fmt::Debug for Instance<T> {
    fn fmt(&self, f:&mut fmt::Formatter<'_>) -> Result<(),fmt::Error> {
        self.0.fmt(f)
    }
}

impl<T:Clone+Hash+Eq> Drop for Instance<T> {
    fn drop(&mut self) {
        match self.1.0.lock().unwrap().entry(self.0.clone()) {
            Entry::Vacant(_) => unreachable!("found InstanceGuard for non-existing entry"),
            Entry::Occupied(mut e) => {
                *e.get_mut() -= 1;
                if *e.get()==0 {
                    e.remove();
                }
            }
        }
    }
}

