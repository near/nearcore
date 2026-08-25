use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Semaphore used to limit total size of network messages in the outgoing queues.
/// A message must acquire permits before being enqueued, if there are not enough permits the message is dropped.
pub(crate) struct OutgoingQueueLimiter {
    semaphore: Arc<Semaphore>,
}

impl OutgoingQueueLimiter {
    pub fn new(capacity_bytes: usize) -> Self {
        Self { semaphore: Arc::new(Semaphore::new(capacity_bytes)) }
    }

    /// Try to reserve `bytes` worth of permits. Returns `None` if not enough
    /// permits are currently available - the caller should drop the message.
    pub fn try_acquire(&self, bytes: usize) -> Option<OutgoingPermit> {
        // tokio's semaphore caps a single acquisition at u32::MAX; the byte
        // counts we care about (hundreds of MB) are well within range.
        let n = u32::try_from(bytes).ok()?;
        match self.semaphore.clone().try_acquire_many_owned(n) {
            Ok(permit) => Some(OutgoingPermit { permit }),
            Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => None,
        }
    }
}

/// Memory permit taken from the outgoing messages semaphore.
pub struct OutgoingPermit {
    permit: OwnedSemaphorePermit,
}

impl std::fmt::Debug for OutgoingPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutgoingPermit").field("bytes", &self.bytes()).finish()
    }
}

impl OutgoingPermit {
    pub fn bytes(&self) -> usize {
        self.permit.num_permits() as usize
    }

    /// Reduce this reservation down to `new_bytes`, releasing the surplus
    /// back to the limiter. No-op if `new_bytes >= self.bytes()`. Used
    /// when a message turns out to be smaller than the upper bound we
    /// reserved.
    pub fn shrink_to(&mut self, new_bytes: usize) {
        let cur = self.bytes();
        if new_bytes >= cur {
            return;
        }
        let to_release = cur - new_bytes;
        // `split` peels `to_release` permits off into a fresh OwnedSemaphorePermit.
        // Dropping that returns the bytes to the semaphore.
        if let Some(released) = self.permit.split(to_release) {
            drop(released);
        }
    }

    /// Test-only constructor.
    pub fn fake_for_test() -> Self {
        let sem = Arc::new(Semaphore::new(1));
        let permit = sem.try_acquire_owned().expect("fresh semaphore has capacity");
        Self { permit }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_acquire_succeeds_under_capacity() {
        let limiter = OutgoingQueueLimiter::new(1000);
        let p = limiter.try_acquire(400).unwrap();
        assert_eq!(p.bytes(), 400);
    }

    #[test]
    fn try_acquire_fails_over_capacity() {
        let limiter = OutgoingQueueLimiter::new(1000);
        let _p = limiter.try_acquire(800).unwrap();
        assert!(limiter.try_acquire(300).is_none());
    }

    #[test]
    fn permit_release_on_drop() {
        let limiter = OutgoingQueueLimiter::new(1000);
        {
            let _p = limiter.try_acquire(800).unwrap();
            assert!(limiter.try_acquire(300).is_none());
        }
        assert!(limiter.try_acquire(300).is_some());
    }

    #[test]
    fn shrink_releases_surplus() {
        let limiter = OutgoingQueueLimiter::new(1000);
        let mut p = limiter.try_acquire(800).unwrap();
        assert!(limiter.try_acquire(300).is_none());
        p.shrink_to(100);
        assert_eq!(p.bytes(), 100);
        // 1000 - 100 = 900 available now
        let _q = limiter.try_acquire(800).unwrap();
    }

    #[test]
    fn shrink_to_larger_is_noop() {
        let limiter = OutgoingQueueLimiter::new(1000);
        let mut p = limiter.try_acquire(500).unwrap();
        p.shrink_to(800);
        assert_eq!(p.bytes(), 500);
    }

    #[test]
    fn fake_for_test_does_not_touch_real_limiter() {
        let limiter = OutgoingQueueLimiter::new(100);
        let _exhaust = limiter.try_acquire(100).unwrap();
        // Production limiter is fully reserved, but the fake permit comes
        // from its own semaphore so construction still succeeds.
        let _fake = OutgoingPermit::fake_for_test();
    }
}
