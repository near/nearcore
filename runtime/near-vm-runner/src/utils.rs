use std::hash::{Hash, Hasher};

pub(crate) fn stable_hash<T: Hash>(value: T) -> u64 {
    // Need some kind of stable hash algorithm, previously was using SipHasher via near-stable-hasher
    #[allow(deprecated)]
    let mut hasher = std::hash::SipHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}
