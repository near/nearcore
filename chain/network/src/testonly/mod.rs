use near_o11y::testonly::init_test_logger;
use pretty_assertions::Comparison;
use std::cmp::Eq;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

pub use super::actix;
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

/// Initializes the test logger and then, iff the current process is executed under
/// nextest in process-per-test mode, changes the behavior of the process to [panic=abort].
/// In particular it doesn't enable [panic=abort] when run via "cargo test".
/// Note that (unfortunately) some tests may expect a panic, so we cannot apply blindly
/// [panic=abort] in compilation time to all tests.
// TODO: investigate whether "-Zpanic-abort-tests" could replace this function once the flag
// becomes stable: https://github.com/rust-lang/rust/issues/67650, so we don't use it.
pub(crate) fn abort_on_panic() {
    init_test_logger();
    // I don't know a way to set panic=abort for nextest builds in compilation time, so we set it
    // in runtime. https://nexte.st/book/env-vars.html#environment-variables-nextest-sets
    let Ok(nextest) = std::env::var("NEXTEST") else { return };
    let Ok(nextest_execution_mode) = std::env::var("NEXTEST_EXECUTION_MODE") else { return };
    if nextest != "1" || nextest_execution_mode != "process-per-test" {
        return;
    }
    tracing::info!(target:"test", "[panic=abort] enabled");
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        std::process::abort();
    }));
}
