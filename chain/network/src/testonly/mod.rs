use pretty_assertions::Comparison;
use std::cmp::Eq;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

pub mod actix;
pub mod fake_client;
pub mod stream;

pub type Rng = rand_xorshift::XorShiftRng;

pub fn make_rng(seed: u64) -> Rng {
    rand::SeedableRng::seed_from_u64(seed)
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

#[track_caller]
pub fn assert_is_superset<'a, T: Debug + Hash + Eq>(sup: &HashSet<&'a T>, sub: &HashSet<&'a T>) {
    if !sup.is_superset(sub) {
        panic!("expected a super set, got diff:\n{}", Comparison::new(sup, sub));
    }
}
