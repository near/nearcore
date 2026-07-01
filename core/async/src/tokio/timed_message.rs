use std::cmp::Ordering;
use tokio::time::Instant;

#[derive(Debug)]
pub(crate) struct TimedMessage<T> {
    pub(crate) at: Instant,
    pub(crate) msg: T,
}

impl<T> Eq for TimedMessage<T> {}

impl<T> PartialEq for TimedMessage<T> {
    fn eq(&self, other: &Self) -> bool {
        self.at.eq(&other.at)
    }
}

impl<T> Ord for TimedMessage<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // reverse for min-heap behavior (soonest = highest priority)
        other.at.cmp(&self.at)
    }
}

impl<T> PartialOrd for TimedMessage<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
