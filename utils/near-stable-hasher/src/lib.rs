#[allow(deprecated)]
use std::hash::{Hasher, SipHasher};

/// We not use stable hasher as it could change with Rust releases, so rely on stable SIP hash.
#[allow(deprecated)]
#[derive(Default, Clone)]
pub struct StableHasher(pub SipHasher);

impl StableHasher {
    #[allow(deprecated)]
    pub fn new() -> StableHasher {
        StableHasher(SipHasher::new())
    }
}

impl Hasher for StableHasher {
    fn finish(&self) -> u64 {
        self.0.finish()
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes)
    }
}
