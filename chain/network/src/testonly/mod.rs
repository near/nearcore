use std::cmp::Eq;
use std::collections::HashSet;
use std::hash::Hash;

pub mod actix;
pub mod fake_client;
pub mod stream;

pub type Rng = rand_pcg::Pcg32;

pub fn make_rng(seed: u64) -> Rng {
    Rng::new(seed, 0xa02bdbf7bb3c0a7)
}

pub trait AsSet<'a, T> {
    fn as_set(&'a self) -> HashSet<&'a T>;
}

impl<'a, T: Hash + Eq> AsSet<'a, T> for Vec<T> {
    fn as_set(&'a self) -> HashSet<&'a T> {
        self.iter().collect()
    }
}

impl<'a, T: Hash + Eq> AsSet<'a, T> for [&'a T] {
    fn as_set(&'a self) -> HashSet<&'a T> {
        self.iter().cloned().collect()
    }
}
