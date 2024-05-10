use crate::{DBCol, Store};
use std::{
    fmt::Display,
    ops::Range,
    str::FromStr,
    sync::{atomic::AtomicUsize, Arc, Mutex},
    time::Duration,
};

#[derive(Clone)]
struct ParallelIterationOptions {
    num_threads: usize,
    min_time_before_split: Duration,
    store: Store,
    column: DBCol,
    callback: Arc<dyn Fn(&[u8], Option<&[u8]>) + Send + Sync>,
}

/// Provides parallel iteration and lookup over a Store.
pub struct StoreParallelIterator {
    threads: Vec<std::thread::JoinHandle<()>>,
    shared_state: Arc<ParallelIterationSharedState>,
}

struct ParallelIterationSharedState {
    num_threads_free: AtomicUsize,
    thread_work: Mutex<Vec<Option<WorkItem>>>,
    num_remaining_tasks: AtomicUsize,
}

impl StoreParallelIterator {
    fn new(options: ParallelIterationOptions, initial_task: WorkItem) -> Self {
        let (tx, rx) = flume::unbounded::<WorkItem>();
        let mut threads = Vec::new();
        let shared_state = Arc::new(ParallelIterationSharedState {
            num_threads_free: AtomicUsize::new(options.num_threads),
            thread_work: Mutex::new(vec![None; options.num_threads]),
            num_remaining_tasks: AtomicUsize::new(1),
        });
        tx.send(initial_task).unwrap();
        for idx in 0..options.num_threads {
            let rx = rx.clone();
            let tx = tx.clone();
            let shared_state = shared_state.clone();
            let options = options.clone();
            let thread = std::thread::spawn(move || loop {
                if shared_state.num_remaining_tasks.load(std::sync::atomic::Ordering::Acquire) == 0
                {
                    // No more work to do.
                    break;
                }
                let Ok(item) = rx.recv_timeout(Duration::from_millis(10)) else {
                    continue;
                };
                shared_state.num_threads_free.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                shared_state.thread_work.lock().unwrap()[idx] = Some(item.clone());

                let start_time = std::time::Instant::now();
                let min_time_to_split = Duration::from_micros(
                    (options.min_time_before_split.as_micros() as f64
                        * (rand::random::<f64>() + 1.0)) as u64,
                );

                match item {
                    WorkItem::Range(range) => {
                        let start = if range.start.is_empty() {
                            None
                        } else {
                            Some(range.start.as_slice())
                        };
                        let end =
                            if range.end.is_empty() { None } else { Some(range.end.as_slice()) };
                        let iter = options.store.iter_range(options.column, start, end);

                        for kv in iter {
                            let (key, value) = kv.unwrap();
                            (options.callback)(&key, Some(&value));
                            if start_time.elapsed() > min_time_to_split {
                                if shared_state
                                    .num_threads_free
                                    .load(std::sync::atomic::Ordering::Relaxed)
                                    > 0
                                {
                                    let next_key = IterationRange::next_key(&key);
                                    if &next_key == &range.end {
                                        break;
                                    }
                                    let remaining_range =
                                        IterationRange::new(next_key, range.end.clone());
                                    let work_items = WorkItem::Range(remaining_range).divide();
                                    for work_item in work_items {
                                        shared_state
                                            .num_remaining_tasks
                                            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                                        tx.send(work_item).unwrap();
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    WorkItem::Lookup(keys, range) => {
                        for i in range.start..range.end {
                            let key = &keys[i];
                            let value = options.store.get_raw_bytes(options.column, key).unwrap();
                            (options.callback)(key, value.as_ref().map(|slice| slice.as_slice()));
                            if start_time.elapsed() > min_time_to_split && i < range.end - 1 {
                                if shared_state
                                    .num_threads_free
                                    .load(std::sync::atomic::Ordering::Relaxed)
                                    > 0
                                {
                                    let work_items =
                                        WorkItem::Lookup(keys.clone(), i..range.end).divide();
                                    for work_item in work_items {
                                        shared_state
                                            .num_remaining_tasks
                                            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                                        tx.send(work_item).unwrap();
                                    }
                                    break;
                                }
                            }
                        }
                    }
                };

                shared_state.num_threads_free.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                shared_state.thread_work.lock().unwrap()[idx] = None;
                shared_state.num_remaining_tasks.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            });
            threads.push(thread);
        }
        Self { threads, shared_state }
    }

    fn describe(&self) -> String {
        let mut result = String::new();
        for (idx, item) in self.shared_state.thread_work.lock().unwrap().iter().enumerate() {
            if let Some(item) = item {
                result.push_str(&format!("  [{}] {}\n", idx, item));
            } else {
                result.push_str(&format!("  [{}] free\n", idx));
            }
        }
        result.push_str(&format!(
            "  Total tasks: {}",
            self.shared_state.num_remaining_tasks.load(std::sync::atomic::Ordering::Relaxed)
        ));
        result
    }

    fn join(&mut self) {
        for thread in self.threads.drain(..) {
            thread.join().unwrap();
        }
    }

    /// Calls the callback function in any order for every key from start to end.
    /// Start is inclusive, end is exclusive. If end is the empty array, it is unbounded.
    /// This may be used no matter what the distribution of the keys are; it will
    /// internally automatically balance the work between the specified number of threads.
    pub fn for_each_in_range(
        store: Store,
        column: DBCol,
        start: Vec<u8>,
        end: Vec<u8>,
        num_threads: usize,
        callback: impl Fn(&[u8], &[u8]) + Send + Sync + 'static,
        print_progress: bool,
    ) {
        let options = ParallelIterationOptions {
            num_threads,
            min_time_before_split: Duration::from_millis(100),
            store,
            column,
            callback: Arc::new(move |key, value| callback(key, value.unwrap())),
        };
        let iter = StoreParallelIterator::new(options, WorkItem::new_range(start, end));
        if print_progress {
            while iter.shared_state.num_remaining_tasks.load(std::sync::atomic::Ordering::Relaxed)
                > 0
            {
                std::thread::sleep(Duration::from_secs(3));
                let desc = iter.describe();
                println!("Progress:\n{}", desc);
            }
        }
    }

    /// Calls the callback function in any order for every key being looked up.
    /// This may be used no matter what the key/value size distributions are; if there is
    /// any imbalance, it will internally automatically balance the work between the specified
    /// number of threads.
    pub fn lookup_keys(
        store: Store,
        column: DBCol,
        keys: Vec<Vec<u8>>,
        num_threads: usize,
        callback: impl Fn(&[u8], Option<&[u8]>) + Send + Sync + 'static,
        print_progress: bool,
    ) {
        let options = ParallelIterationOptions {
            num_threads,
            min_time_before_split: Duration::from_millis(100),
            store,
            column,
            callback: Arc::new(callback),
        };
        let num_keys = keys.len();
        let iter = StoreParallelIterator::new(options, WorkItem::new_lookup(keys, 0..num_keys));
        if print_progress {
            while iter.shared_state.num_remaining_tasks.load(std::sync::atomic::Ordering::Relaxed)
                > 0
            {
                std::thread::sleep(Duration::from_secs(3));
                let desc = iter.describe();
                println!("Progress:\n{}", desc);
            }
        }
    }
}

impl Drop for StoreParallelIterator {
    fn drop(&mut self) {
        self.join();
    }
}

#[derive(Clone)]
enum WorkItem {
    Range(IterationRange),
    Lookup(Arc<Vec<Vec<u8>>>, Range<usize>),
}

impl WorkItem {
    fn divide(&self) -> Vec<Self> {
        match self {
            WorkItem::Range(range) => range.divide().into_iter().map(WorkItem::Range).collect(),
            WorkItem::Lookup(keys, range) => {
                let start = range.start;
                let end = range.end;
                let chunks = 16.min(end - start);
                let chunk_size = (end - start) / chunks;
                let mut result = Vec::new();
                for i in 0..chunks {
                    let start = start + i * chunk_size;
                    let end = if i == chunks - 1 { end } else { start + chunk_size };
                    result.push(WorkItem::Lookup(keys.clone(), start..end));
                }
                result
            }
        }
    }

    fn new_range(start: Vec<u8>, end: Vec<u8>) -> Self {
        Self::Range(IterationRange::new(start, end))
    }

    fn new_lookup(keys: Vec<Vec<u8>>, range: Range<usize>) -> Self {
        assert!(range.start < range.end);
        Self::Lookup(Arc::new(keys), range)
    }
}

impl Display for WorkItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkItem::Range(range) => write!(f, "range {}", range),
            WorkItem::Lookup(_, range) => {
                write!(f, "keys {}..{}", range.start, range.end)
            }
        }
    }
}

#[derive(Clone)]
struct IterationRange {
    start: Vec<u8>,
    end: Vec<u8>, // empty means unbounded
}

impl IterationRange {
    fn new(start: Vec<u8>, end: Vec<u8>) -> Self {
        assert!(start < end || end.is_empty());
        Self { start, end }
    }

    /// Returns the smallest possible key that is greater than the given key.
    fn next_key(key: &[u8]) -> Vec<u8> {
        let mut key = key.to_vec();
        key.push(0);
        key
    }

    /// Divides the range into smaller ranges. The implementation can be anything as long as
    /// this will exponentially divide the key space.
    fn divide(&self) -> Vec<Self> {
        // First find the common prefix; the resulting ranges will always start with these,
        // so just ignore these for the remaining processing.
        let mut common_prefix = Vec::new();
        for (a, b) in self.start.iter().zip(self.end.iter()) {
            if a == b {
                common_prefix.push(*a);
            } else {
                break;
            }
        }

        // Remove the common prefix.
        let start = self.start[common_prefix.len()..].to_vec();
        let end = self.end[common_prefix.len()..].to_vec();

        // We'll find some split points and use these to split the ranges.
        let mut split_points = Vec::new();

        // If the start is the beginning, divide based on the first character
        if start.is_empty() {
            if end.len() == 1 {
                // If the end is just one byte, then that byte is exclusive.
                for byte in 0..end[0] {
                    let point = vec![byte];
                    split_points.push(point);
                }
            } else {
                // Otherwise the end byte is inclusive.
                let first_character_max = end.get(0).copied().unwrap_or(0xff);
                for byte in 0..=first_character_max {
                    let point = vec![byte];
                    split_points.push(point);
                }
            }
        }
        // If there's no end, then divide based on the first byte of start that is not 0xff
        else if end.is_empty() {
            let mut byte_not_ff = 0;
            for (i, byte) in start.iter().enumerate() {
                if *byte != 0xff {
                    byte_not_ff = i;
                    break;
                }
            }
            for byte in start[byte_not_ff] + 1..=0xff {
                let point = start
                    .iter()
                    .copied()
                    .take(byte_not_ff)
                    .chain(std::iter::once(byte))
                    .collect::<Vec<u8>>();
                split_points.push(point);
            }
        } else {
            // If there's both start and end, then the first byte must be different. Divide on that.
            let start_byte = start[0];
            let end_byte = end[0];
            if end.len() == 1 {
                // If the end is just one byte, then that byte is exclusive. In this case, the first
                // byte of the start may be just one less than the end byte, in which case we will
                // need to keep going to find the first start byte that is not 0xff.
                let mut start_prefix_len = 0;
                while start_prefix_len < start.len()
                    && ((start_prefix_len == 0 && start[0] + 1 == end[0])
                        || start[start_prefix_len] == 0xff)
                {
                    start_prefix_len += 1;
                }
                let next_byte_min =
                    start.get(start_prefix_len).copied().map(|v| v + 1).unwrap_or(0);
                let end_byte = if start_prefix_len == 0 { end_byte - 1 } else { 0xff };
                for byte in next_byte_min..=end_byte {
                    let point = start
                        .iter()
                        .copied()
                        .take(start_prefix_len)
                        .chain(std::iter::once(byte))
                        .collect::<Vec<u8>>();
                    split_points.push(point);
                }
            } else {
                // If the end is more than one byte, then just divide on the first byte.
                for byte in start_byte + 1..=end_byte {
                    let point = vec![byte];
                    split_points.push(point);
                }
            }
        }

        // Now we have the split points, we can create the ranges.
        let mut ranges = Vec::new();
        let mut prev = start.clone();
        for point in split_points {
            assert!(!point.is_empty());
            assert!(prev < point, "{} < {}", hex::encode(&prev), hex::encode(&point));
            assert!(
                point < end || end.is_empty(),
                "{} < {} while splitting {} - {}",
                hex::encode(&point),
                hex::encode(&end),
                hex::encode(&self.start),
                hex::encode(&self.end)
            );
            ranges.push(Self::new(prev.clone(), point.clone()));
            prev = point;
        }
        ranges.push(Self::new(prev, end));

        // Finally put the prefix back.
        ranges.iter_mut().for_each(|range| {
            range.start =
                common_prefix.iter().copied().chain(range.start.iter().copied()).collect();
            range.end = common_prefix.iter().copied().chain(range.end.iter().copied()).collect();
        });
        ranges
    }
}

impl FromStr for IterationRange {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split("..").collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid range format"));
        }
        let start = hex::decode(parts[0])?;
        let end = hex::decode(parts[1])?;
        Ok(Self::new(start, end))
    }
}

impl Display for IterationRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", hex::encode(&self.start), hex::encode(&self.end))
    }
}

#[cfg(test)]
mod tests {
    use super::StoreParallelIterator;
    use crate::parallel_iter::{IterationRange, WorkItem};
    use crate::{test_utils::create_test_store, DBCol};
    use std::collections::BTreeMap;
    use std::{
        str::FromStr,
        sync::{Arc, Mutex},
    };

    #[test]
    fn test_iteration_range_divide() {
        let ranges = IterationRange::from_str("..").unwrap().divide();
        assert_eq!(ranges.len(), 257);
        assert_eq!(ranges[0].to_string(), "..00");
        assert_eq!(ranges[5].to_string(), "04..05");
        assert_eq!(ranges[256].to_string(), "ff..");

        let ranges = IterationRange::from_str("00..").unwrap().divide();
        assert_eq!(ranges.len(), 256);
        assert_eq!(ranges[0].to_string(), "00..01");
        assert_eq!(ranges[5].to_string(), "05..06");
        assert_eq!(ranges[255].to_string(), "ff..");

        let ranges = IterationRange::from_str("00..01").unwrap().divide();
        assert_eq!(ranges.len(), 257);
        assert_eq!(ranges[0].to_string(), "00..0000");
        assert_eq!(ranges[5].to_string(), "0004..0005");
        assert_eq!(ranges[256].to_string(), "00ff..01");

        let ranges = IterationRange::from_str("00..02").unwrap().divide();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].to_string(), "00..01");
        assert_eq!(ranges[1].to_string(), "01..02");

        let ranges = IterationRange::from_str("00fd..01").unwrap().divide();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].to_string(), "00fd..00fe");
        assert_eq!(ranges[1].to_string(), "00fe..00ff");
        assert_eq!(ranges[2].to_string(), "00ff..01");

        let ranges = IterationRange::from_str("00fd..02").unwrap().divide();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].to_string(), "00fd..01");
        assert_eq!(ranges[1].to_string(), "01..02");

        let ranges = IterationRange::from_str("000000000000000030..01").unwrap().divide();
        assert_eq!(ranges.len(), 256);
        assert_eq!(ranges[0].to_string(), "000000000000000030..0001");
        assert_eq!(ranges[1].to_string(), "0001..0002");
        assert_eq!(ranges[255].to_string(), "00ff..01");

        let ranges = IterationRange::from_str("00fffffd..01").unwrap().divide();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].to_string(), "00fffffd..00fffffe");
        assert_eq!(ranges[1].to_string(), "00fffffe..00ffffff");
        assert_eq!(ranges[2].to_string(), "00ffffff..01");

        let ranges = IterationRange::from_str("00010203..02030405").unwrap().divide();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].to_string(), "00010203..01");
        assert_eq!(ranges[1].to_string(), "01..02");
        assert_eq!(ranges[2].to_string(), "02..02030405");

        let ranges = IterationRange::from_str("0100010203..0102030405").unwrap().divide();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].to_string(), "0100010203..0101");
        assert_eq!(ranges[1].to_string(), "0101..0102");
        assert_eq!(ranges[2].to_string(), "0102..0102030405");

        let ranges = IterationRange::from_str("01000000000000000030..0101").unwrap().divide();
        assert_eq!(ranges.len(), 256);
        assert_eq!(ranges[0].to_string(), "01000000000000000030..010001");
        assert_eq!(ranges[1].to_string(), "010001..010002");
        assert_eq!(ranges[255].to_string(), "0100ff..0101");
    }

    #[test]
    fn test_work_item_divide() {
        let range = WorkItem::Range(IterationRange::from_str("00..01").unwrap());
        let items = range.divide();
        assert_eq!(items.len(), 257);
        assert_eq!(items[0].to_string(), "range 00..0000");
        assert_eq!(items[5].to_string(), "range 0004..0005");
        assert_eq!(items[256].to_string(), "range 00ff..01");

        let item = WorkItem::Lookup(Arc::new(vec![vec![0], vec![1], vec![2]]), 0..3).divide();
        assert_eq!(item.len(), 3);
        assert_eq!(item[0].to_string(), "keys 0..1");
        assert_eq!(item[1].to_string(), "keys 1..2");
        assert_eq!(item[2].to_string(), "keys 2..3");
    }

    #[test]
    fn test_parallel_iteration() {
        let store = create_test_store();
        let mut update = store.store_update();
        let mut data = BTreeMap::new();
        for _ in 0..1000000 {
            let key_len = rand::random::<usize>() % 10;
            let key = (0..key_len).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
            let value_len = rand::random::<usize>() % 10;
            let value = (0..value_len).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
            if !data.contains_key(&key) {
                update.insert(DBCol::BlockHeader, key.clone(), value.clone());
                data.insert(key, value);
            }
        }
        update.commit().unwrap();

        let read_data = Arc::new(Mutex::new(BTreeMap::new()));
        {
            let read_data = read_data.clone();
            StoreParallelIterator::for_each_in_range(
                store,
                DBCol::BlockHeader,
                Vec::new(),
                Vec::new(),
                3,
                move |key: &[u8], value: &[u8]| {
                    read_data.lock().unwrap().insert(key.to_vec(), value.to_vec());
                },
                true,
            );
        }

        let read_data = read_data.lock().unwrap();
        assert_eq!(*read_data, data);
    }

    #[test]
    fn test_parallel_lookup() {
        let store = create_test_store();
        let mut update = store.store_update();
        let mut data = BTreeMap::new();
        for _ in 0..1000000 {
            let key_len = rand::random::<usize>() % 10;
            let key = (0..key_len).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
            let value_len = rand::random::<usize>() % 10;
            let value = (0..value_len).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
            if !data.contains_key(&key) {
                update.insert(DBCol::BlockHeader, key.clone(), value.clone());
                data.insert(key, value);
            }
        }
        update.commit().unwrap();

        let read_data = Arc::new(Mutex::new(BTreeMap::new()));
        {
            let read_data = read_data.clone();
            StoreParallelIterator::lookup_keys(
                store,
                DBCol::BlockHeader,
                data.keys().cloned().collect(),
                3,
                move |key: &[u8], value: Option<&[u8]>| {
                    read_data.lock().unwrap().insert(key.to_vec(), value.unwrap().to_vec());
                },
                true,
            );
        }

        let read_data: std::sync::MutexGuard<BTreeMap<Vec<u8>, Vec<u8>>> =
            read_data.lock().unwrap();
        assert_eq!(*read_data, data);
    }
}
