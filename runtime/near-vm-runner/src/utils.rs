use std::hash::{Hash, Hasher};

// This method is not used on macOS so it's fine to allow it to be unused.
#[allow(dead_code)]
pub(crate) fn stable_hash<T: Hash>(value: T) -> u64 {
    // This is ported over from the previous uses, that relied on near-stable-hasher.
    // The need for stability here can certainly be discussed, and it could probably be replaced with DefaultHasher.
    // Not doing it in this refactor as it’s not the core of the issue and using SipHasher is an easy alternative.
    #[allow(deprecated)]
    let mut hasher = std::hash::SipHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}
