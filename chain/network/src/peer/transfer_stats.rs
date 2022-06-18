use near_network_primitives::time;
/// The purpose of `TransferStats` is to keep track of transfer sizes in done in a period of 1 minute.
/// And then; to provide a summary, the count and total size in bytes when requested.
///
/// ```rust,ignore
/// use crate::peer::transfer_stats::TransferStats;
/// use std::time::time::Instant;
///
/// let ts = TransferStats::new();
/// let start = time::Instant::now();
///
/// ts.record(1234, start);
///
/// let later = time::Instant::now();
/// println!("{}", ts.minute_stats(later));
/// ```
use std::collections::VecDeque;

/// Defines how long should entries be tracked.
const TRANSFER_STATS_INTERVAL: time::Duration = time::Duration::seconds(60);

/// Represents a single event in time.
struct Event {
    /// Time when event happened.
    instant: time::Instant,
    /// Number of bytes
    bytes: u64,
}

/// Represents all events which happened in last minute.
#[derive(Default)]
pub(crate) struct TransferStats {
    /// We keep list of entries not older than 1m.
    /// Events in the queue have timestamps in non-decreasing order.
    events: VecDeque<Event>,
    /// Sum of bytes for all entries.
    total_bytes_in_events: u64,
}

/// Represents cumulative stats per minute.
#[derive(Eq, PartialEq, Debug)]
pub(crate) struct MinuteStats {
    /// Bytes per minute.
    pub(crate) bytes_per_min: u64,
    /// Messages per minute.
    pub(crate) count_per_min: usize,
}

impl TransferStats {
    /// Record event at current time `now` with `bytes` bytes.
    /// Time in `now` should be monotonically increasing.
    pub(crate) fn record(&mut self, bytes: u64, now: time::Instant) {
        self.remove_old_entries(now);

        debug_assert!(self.events.back().map(|e| e.instant).unwrap_or(now) <= now);

        self.total_bytes_in_events += bytes;
        self.events.push_back(Event { instant: now, bytes });
    }

    /// Get stats stored in `MinuteStats` struct.
    pub(crate) fn minute_stats(&mut self, now: time::Instant) -> MinuteStats {
        self.remove_old_entries(now);
        MinuteStats { bytes_per_min: self.total_bytes_in_events, count_per_min: self.events.len() }
    }

    /// Remove entries older than 1m.
    fn remove_old_entries(&mut self, now: time::Instant) {
        while let Some(event) = self.events.pop_front() {
            if now - event.instant > TRANSFER_STATS_INTERVAL {
                self.total_bytes_in_events -= event.bytes;
            } else {
                // add the event back
                self.events.push_front(event);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_transfer_stats() {
        let mut ts = TransferStats::default();
        let now = time::Instant::now();
        assert_eq!(ts.minute_stats(now), MinuteStats { bytes_per_min: 0, count_per_min: 0 });

        ts.record(10, now);

        assert_eq!(ts.minute_stats(now), MinuteStats { bytes_per_min: 10, count_per_min: 1 });

        ts.record(100, now + time::Duration::seconds(45));
        assert_eq!(
            ts.minute_stats(now + time::Duration::seconds(45)),
            MinuteStats { bytes_per_min: 110, count_per_min: 2 }
        );

        ts.record(1000, now + time::Duration::seconds(59));
        assert_eq!(
            ts.minute_stats(now + time::Duration::seconds(59)),
            MinuteStats { bytes_per_min: 1110, count_per_min: 3 }
        );

        assert_eq!(
            ts.minute_stats(now + time::Duration::seconds(61)),
            MinuteStats { bytes_per_min: 1100, count_per_min: 2 }
        );

        assert_eq!(
            ts.minute_stats(now + time::Duration::seconds(121)),
            MinuteStats { bytes_per_min: 0, count_per_min: 0 }
        );
    }
}
