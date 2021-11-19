// Copyright 2018 The Grin Developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

const MINUTE: Duration = Duration::from_secs(60);

/// Stores list of entries for `RateCounter`
struct Entry {
    /// bytes since last reset
    bytes: u64,
    /// Time we created the entry.
    recorded: SystemTime,
}

/// A rate counter tracks number of transfers, the amount of data exchanged and the rate of transfer
/// over the last minute.
pub struct RateCounter {
    entries: VecDeque<Entry>,
    bytes_sum: u64,
}

impl RateCounter {
    pub fn new() -> Self {
        RateCounter { entries: VecDeque::new(), bytes_sum: 0 }
    }

    /// Increment number of bytes transferred, updating counts and rates.
    pub fn increment(&mut self, bytes: u64) {
        let now = SystemTime::now();
        self.entries.push_back(Entry { bytes, recorded: now });
        self.bytes_sum += bytes;
        self.truncate(now);
    }

    pub fn bytes_per_min(&self) -> u64 {
        self.bytes_sum
    }

    pub fn count_per_min(&self) -> u64 {
        self.entries.len() as u64
    }

    fn truncate(&mut self, now: SystemTime) {
        // Remove entries older than 1m.
        while !self.entries.is_empty() && self.entries.front().unwrap().recorded < now - MINUTE {
            self.bytes_sum -= self.entries.pop_front().unwrap().bytes;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_counter() {
        let mut rc = RateCounter::new();

        rc.increment(1000);
        assert_eq!(rc.bytes_per_min(), 1000);
        assert_eq!(rc.count_per_min(), 1);

        rc.increment(123);

        assert_eq!(rc.bytes_per_min(), 1123);
        assert_eq!(rc.count_per_min(), 2);

        rc.truncate(SystemTime::now() + MINUTE + Duration::from_millis(1));

        assert_eq!(rc.bytes_per_min(), 0);
        assert_eq!(rc.count_per_min(), 0);
    }
}
