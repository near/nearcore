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

use std::time::{Duration, SystemTime};

const MINUTE_IN_MILLIS: u128 = 60_000;

struct Entry {
    bytes: u64,
    timestamp: u128,
}

impl Entry {
    fn new(bytes: u64) -> Self {
        Entry { bytes, timestamp: millis_since_epoch() }
    }
}

/// A rate counter tracks number of transfers, the amount of data exchanged and the rate of transfer
/// over the last minute.
pub struct RateCounter {
    last_min_entries: Vec<Entry>,
}

impl RateCounter {
    pub fn new() -> Self {
        RateCounter { last_min_entries: vec![] }
    }

    /// Increment number of bytes transferred, updating counts and rates.
    pub fn increment(&mut self, bytes: u64) {
        self.last_min_entries.push(Entry::new(bytes));
        self.truncate();
    }

    pub fn bytes_per_min(&self) -> u64 {
        self.last_min_entries.iter().map(|x| x.bytes).sum()
    }

    pub fn count_per_min(&self) -> u64 {
        self.last_min_entries.len() as u64
    }

    fn truncate(&mut self) {
        let now = millis_since_epoch();
        while self.last_min_entries.len() > 0 && self.last_min_entries[0].timestamp + MINUTE_IN_MILLIS < now {
            self.last_min_entries.remove(0);
        }
    }
}

/// Returns timestamp in milliseconds.
fn millis_since_epoch() -> u128 {
    let since_epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or(Duration::new(0, 0));
    since_epoch.as_millis()
}
