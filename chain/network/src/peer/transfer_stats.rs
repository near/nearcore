/// The purpose of `TransferStats` is to keep track of transfer sizes in done in a period of 1 minute.
/// And then; to provide a summary, the count and total size in bytes when requested.
///
/// ```rust,ignore
/// use crate::peer::transfer_stats::TransferStats;
/// use std::time::Instant;
///
/// let ts = TransferStats::new();
/// let start = Instant::now();
///
/// ts.record(1234, start);
///
/// let later = Instant::now();
/// println!("{}", ts.minute_stats(later));
/// ```
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Defines how long should entries be tracked.
const TRANSFER_STATS_INTERVAL: Duration = Duration::from_secs(60);

/// Represents a single event in time.
struct Event {
    /// Time when event happened.
    instant: Instant,
    /// Number of bytes
    bytes: u64,
}

/// Represents all events which happened in last minute.
#[derive(Default)]
pub struct TransferStats {
    /// We keep list of entries not older than 1m.
    /// Events in the queue have timestamps in non-decreasing order.
    events: VecDeque<Event>,
    /// Sum of bytes for all entries.
    total_bytes_in_events: u64,
}

/// Represents cumulative stats per minute.
#[derive(Eq, PartialEq, Debug)]
pub struct MinuteStats {
    /// Bytes per minute.
    pub bytes_per_min: u64,
    /// Messages per minute.
    pub count_per_min: usize,
}

impl TransferStats {
    /// Record event at current time `now` with `bytes` bytes.
    /// Time in `now` should be monotonically increasing.
    pub fn record(&mut self, bytes: u64, now: Instant) {
        self.remove_old_entries(now);

        debug_assert!(self.events.back().map(|e| e.instant).unwrap_or(now) <= now);

        self.total_bytes_in_events += bytes;
        self.events.push_back(Event { instant: now, bytes });
    }

    /// Get stats stored in `MinuteStats` struct.
    pub fn minute_stats(&mut self, now: Instant) -> MinuteStats {
        self.remove_old_entries(now);
        MinuteStats { bytes_per_min: self.total_bytes_in_events, count_per_min: self.events.len() }
    }

    /// Remove entries older than 1m.
    fn remove_old_entries(&mut self, now: Instant) {
        while let Some(event) = self.events.pop_front() {
            if now.duration_since(event.instant) > TRANSFER_STATS_INTERVAL {
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
        let now = Instant::now();
        assert_eq!(ts.minute_stats(now), MinuteStats { bytes_per_min: 0, count_per_min: 0 });

        ts.record(10, now);

        assert_eq!(ts.minute_stats(now), MinuteStats { bytes_per_min: 10, count_per_min: 1 });

        ts.record(100, now + Duration::from_secs(45));
        assert_eq!(
            ts.minute_stats(now + Duration::from_secs(45)),
            MinuteStats { bytes_per_min: 110, count_per_min: 2 }
        );

        ts.record(1000, now + Duration::from_secs(59));
        assert_eq!(
            ts.minute_stats(now + Duration::from_secs(59)),
            MinuteStats { bytes_per_min: 1110, count_per_min: 3 }
        );

        assert_eq!(
            ts.minute_stats(now + Duration::from_secs(61)),
            MinuteStats { bytes_per_min: 1100, count_per_min: 2 }
        );

        assert_eq!(
            ts.minute_stats(now + Duration::from_secs(121)),
            MinuteStats { bytes_per_min: 0, count_per_min: 0 }
        );
    }
}
