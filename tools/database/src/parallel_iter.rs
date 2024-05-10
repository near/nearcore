use std::{
    fmt::Display,
    ops::Range,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc, Mutex,
    },
    time::Duration,
};

use near_store::{DBCol, Store};

#[derive(Clone)]
pub struct ParallelIterationOptions {
    pub num_threads: usize,
    pub min_time_before_split: Duration,
}

#[derive(Clone)]
struct WorkItem {
    kind: WorkItemKind,
    column: DBCol,
}

#[derive(Clone)]
enum WorkItemKind {
    Range(IterationRange),
    Lookup(Arc<Vec<Vec<u8>>>, Range<usize>),
}

impl WorkItem {
    fn divide(&self) -> Vec<Self> {
        match &self.kind {
            WorkItemKind::Range(range) => range
                .divide()
                .into_iter()
                .map(|range| Self { kind: WorkItemKind::Range(range), column: self.column })
                .collect(),
            WorkItemKind::Lookup(keys, range) => {
                let start = range.start;
                let end = range.end;
                let chunks = 16.min(end - start);
                let chunk_size = (end - start) / chunks;
                let mut result = Vec::new();
                for i in 0..chunks {
                    let start = start + i * chunk_size;
                    let end = if i == chunks - 1 { end } else { start + chunk_size };
                    result.push(Self {
                        kind: WorkItemKind::Lookup(keys.clone(), start..end),
                        column: self.column,
                    });
                }
                result
            }
        }
    }

    fn new_range(column: DBCol, start: Vec<u8>, end: Vec<u8>) -> Self {
        Self { kind: WorkItemKind::Range(IterationRange::new(start, end)), column }
    }

    fn new_lookup(column: DBCol, keys: Vec<Vec<u8>>, range: Range<usize>) -> Self {
        assert!(range.start < range.end);
        Self { kind: WorkItemKind::Lookup(Arc::new(keys), range), column }
    }
}

impl Display for WorkItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            WorkItemKind::Range(range) => write!(f, "range {}", range),
            WorkItemKind::Lookup(_, range) => {
                write!(f, "keys {}..{})", range.start, range.end)
            }
        }
    }
}

pub struct RocksDBParallelIterator {
    db: Store,
    num_threads_free: Arc<AtomicUsize>,
    thread_work: Arc<Mutex<Vec<Option<WorkItem>>>>,

    work_queue: flume::Sender<WorkItem>,
    threads: Vec<std::thread::JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl RocksDBParallelIterator {
    pub fn new(
        options: ParallelIterationOptions,
        db: Store,
        callback: Arc<dyn Fn(&[u8], Option<&[u8]>) + Send + Sync>,
    ) -> Self {
        let (tx, rx) = flume::unbounded::<WorkItem>();
        let mut threads = Vec::new();
        let num_threads_free = Arc::new(AtomicUsize::new(options.num_threads));
        let thread_work =
            Arc::new(Mutex::new((0..options.num_threads).map(|_| None).collect::<Vec<_>>()));
        let stop = Arc::new(AtomicBool::new(false));
        for idx in 0..options.num_threads {
            let db = db.clone();
            let rx = rx.clone();
            let tx = tx.clone();
            let callback = callback.clone();
            let num_threads_free = num_threads_free.clone();
            let options = options.clone();
            let thread_work = thread_work.clone();
            let stop = stop.clone();
            let thread = std::thread::spawn(move || loop {
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                let Ok(item) = rx.recv_timeout(Duration::from_millis(100)) else {
                    continue;
                };
                num_threads_free.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                thread_work.lock().unwrap()[idx] = Some(item.clone());

                let start_time = std::time::Instant::now();
                let time_to_split = Duration::from_micros(
                    (options.min_time_before_split.as_micros() as f64
                        * (rand::random::<f64>() + 1.0)) as u64,
                );

                match item.kind {
                    WorkItemKind::Range(range) => {
                        let start = if range.start.is_empty() {
                            None
                        } else {
                            Some(range.start.as_slice())
                        };
                        let end =
                            if range.end.is_empty() { None } else { Some(range.end.as_slice()) };
                        let iter = db.iter_range(item.column, start, end);

                        for kv in iter {
                            let (key, value) = kv.unwrap();
                            callback(&key, Some(&value));
                            if start_time.elapsed() > time_to_split {
                                if num_threads_free.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                                    let next_key = IterationRange::next_key(&key);
                                    if &next_key == &range.end {
                                        break;
                                    }
                                    let remaining_range =
                                        IterationRange::new(next_key, range.end.clone());
                                    let work_items = WorkItem {
                                        kind: WorkItemKind::Range(remaining_range),
                                        column: item.column,
                                    }
                                    .divide();
                                    for work_item in work_items {
                                        tx.send(work_item).unwrap();
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    WorkItemKind::Lookup(keys, range) => {
                        for i in range.start..range.end {
                            let key = &keys[i];
                            let value = db.get_raw_bytes(item.column, key).unwrap();
                            callback(key, value.as_ref().map(|slice| slice.as_slice()));
                            if start_time.elapsed() > time_to_split && i < range.end - 1 {
                                if num_threads_free.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                                    let remaining_range =
                                        WorkItemKind::Lookup(keys.clone(), i..range.end);
                                    let work_items =
                                        WorkItem { kind: remaining_range, column: item.column }
                                            .divide();
                                    for work_item in work_items {
                                        tx.send(work_item).unwrap();
                                    }
                                    break;
                                }
                            }
                        }
                    }
                };

                num_threads_free.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                thread_work.lock().unwrap()[idx] = None;
            });
            threads.push(thread);
        }
        Self { db, num_threads_free, work_queue: tx, threads, thread_work, stop }
    }

    pub fn iter_range(&self, column: DBCol, start: Vec<u8>, end: Vec<u8>) {
        let work_item =
            WorkItem { kind: WorkItemKind::Range(IterationRange::new(start, end)), column };
        self.work_queue.send(work_item).unwrap();
    }

    pub fn lookup(&self, column: DBCol, keys: Vec<Vec<u8>>) {
        let num_keys = keys.len();
        let work_item =
            WorkItem { kind: WorkItemKind::Lookup(Arc::new(keys), 0..num_keys), column };
        self.work_queue.send(work_item).unwrap();
    }

    pub fn shutdown(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for thread in self.threads.drain(..) {
            thread.join().unwrap();
        }
    }

    pub fn describe(&self) -> String {
        let mut result = String::new();
        for (idx, item) in self.thread_work.lock().unwrap().iter().enumerate() {
            if let Some(item) = item {
                result.push_str(&format!("  [{}] {}\n", idx, item));
            } else {
                result.push_str(&format!("  [{}] free\n", idx));
            }
        }
        result.push_str(&format!("  Queue: {}", self.work_queue.len()));
        result
    }

    pub fn for_each_parallel(
        store: Store,
        column: DBCol,
        start: Vec<u8>,
        end: Vec<u8>,
        num_threads: usize,
        callback: impl Fn(&[u8], Option<&[u8]>) + Send + Sync + 'static,
    ) {
        let options = ParallelIterationOptions {
            num_threads,
            min_time_before_split: Duration::from_millis(100),
        };
        let iter = RocksDBParallelIterator::new(options, store, Arc::new(callback));
        iter.iter_range(column, start, end);
        loop {
            std::thread::sleep(Duration::from_secs(1));
            let desc = iter.describe();
            println!("Progress:\n{}", desc);
            if iter.num_threads_free.load(std::sync::atomic::Ordering::Relaxed) == num_threads
                && iter.work_queue.is_empty()
            {
                break;
            }
        }
    }

    pub fn lookup_parallel(
        store: Store,
        column: DBCol,
        keys: Vec<Vec<u8>>,
        num_threads: usize,
        callback: impl Fn(&[u8], Option<&[u8]>) + Send + Sync + 'static,
    ) {
        let options = ParallelIterationOptions {
            num_threads,
            min_time_before_split: Duration::from_millis(100),
        };
        let iter = RocksDBParallelIterator::new(options, store, Arc::new(callback));
        iter.lookup(column, keys);
        loop {
            std::thread::sleep(Duration::from_secs(1));
            let desc = iter.describe();
            println!("Progress:\n{}", desc);
            if iter.num_threads_free.load(std::sync::atomic::Ordering::Relaxed) == num_threads
                && iter.work_queue.is_empty()
            {
                break;
            }
        }
    }
}

impl Drop for RocksDBParallelIterator {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Clone)]
struct IterationRange {
    start: Vec<u8>,
    end: Vec<u8>, // empty means no end
}

impl IterationRange {
    fn new(start: Vec<u8>, end: Vec<u8>) -> Self {
        assert!(start < end || end.is_empty());
        Self { start, end }
    }

    fn next_key(key: &[u8]) -> Vec<u8> {
        let mut key = key.to_vec();
        key.push(0);
        key
    }

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

        let start = self.start[common_prefix.len()..].to_vec();
        let end = self.end[common_prefix.len()..].to_vec();

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
                // If the end is just one byte, then that byte is exclusive.
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
                for byte in start_byte + 1..=end_byte {
                    let point = vec![byte];
                    split_points.push(point);
                }
            }
        }

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

        // finally put the prefix back
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
        let parts: Vec<&str> = s.split(':').collect();
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
        write!(f, "{}:{}", hex::encode(&self.start), hex::encode(&self.end))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        str::FromStr,
        sync::{Arc, Mutex},
    };

    use near_store::{test_utils::create_test_store, DBCol};

    use crate::parallel_iter::IterationRange;

    use super::RocksDBParallelIterator;

    #[test]
    fn test_iteration_range_divide() {
        use super::IterationRange;
        let ranges = IterationRange::from_str(":").unwrap().divide();
        assert_eq!(ranges.len(), 257);
        assert_eq!(ranges[0].to_string(), ":00");
        assert_eq!(ranges[5].to_string(), "04:05");
        assert_eq!(ranges[256].to_string(), "ff:");

        let ranges = IterationRange::from_str("00:").unwrap().divide();
        assert_eq!(ranges.len(), 256);
        assert_eq!(ranges[0].to_string(), "00:01");
        assert_eq!(ranges[5].to_string(), "05:06");
        assert_eq!(ranges[255].to_string(), "ff:");

        let ranges = IterationRange::from_str("00:01").unwrap().divide();
        assert_eq!(ranges.len(), 257);
        assert_eq!(ranges[0].to_string(), "00:0000");
        assert_eq!(ranges[5].to_string(), "0004:0005");
        assert_eq!(ranges[256].to_string(), "00ff:01");

        let ranges = IterationRange::from_str("00:02").unwrap().divide();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].to_string(), "00:01");
        assert_eq!(ranges[1].to_string(), "01:02");

        let ranges = IterationRange::from_str("00fd:01").unwrap().divide();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].to_string(), "00fd:00fe");
        assert_eq!(ranges[1].to_string(), "00fe:00ff");
        assert_eq!(ranges[2].to_string(), "00ff:01");

        let ranges = IterationRange::from_str("00fd:02").unwrap().divide();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].to_string(), "00fd:01");
        assert_eq!(ranges[1].to_string(), "01:02");

        let ranges = IterationRange::from_str(
            "0000000000000000303432656b6b69387a6f396f2e75736572732e6b61696368696e6700:01",
        )
        .unwrap()
        .divide();
        assert_eq!(ranges.len(), 256);
        assert_eq!(
            ranges[0].to_string(),
            "0000000000000000303432656b6b69387a6f396f2e75736572732e6b61696368696e6700:0001"
        );
        assert_eq!(ranges[1].to_string(), "0001:0002");
        assert_eq!(ranges[255].to_string(), "00ff:01");

        let ranges = IterationRange::from_str("00fffffd:01").unwrap().divide();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].to_string(), "00fffffd:00fffffe");
        assert_eq!(ranges[1].to_string(), "00fffffe:00ffffff");
        assert_eq!(ranges[2].to_string(), "00ffffff:01");

        let ranges = IterationRange::from_str("00010203:02030405").unwrap().divide();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].to_string(), "00010203:01");
        assert_eq!(ranges[1].to_string(), "01:02");
        assert_eq!(ranges[2].to_string(), "02:02030405");

        let ranges = IterationRange::from_str("0100010203:0102030405").unwrap().divide();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].to_string(), "0100010203:0101");
        assert_eq!(ranges[1].to_string(), "0101:0102");
        assert_eq!(ranges[2].to_string(), "0102:0102030405");

        let ranges = IterationRange::from_str("0100000000000000003036306234373936623536313561343639393535393262613265613662396366663133623264326162326538613361333336363232393765653732663835616200:0101").unwrap().divide();
        assert_eq!(ranges.len(), 256);
        assert_eq!(
            ranges[0].to_string(),
            "0100000000000000003036306234373936623536313561343639393535393262613265613662396366663133623264326162326538613361333336363232393765653732663835616200:010001"
        );
        assert_eq!(ranges[1].to_string(), "010001:010002");
        assert_eq!(ranges[255].to_string(), "0100ff:0101");
    }

    fn test_work_item_divide() {
        use super::{WorkItem, WorkItemKind};
        let range = WorkItem {
            kind: WorkItemKind::Range(IterationRange::from_str("00:01").unwrap()),
            column: DBCol::BlockHeader,
        };
        let items = range.divide();
        assert_eq!(items.len(), 257);
        assert_eq!(items[0].to_string(), "range 00:00");
        assert_eq!(items[5].to_string(), "range 05:06");
        assert_eq!(items[256].to_string(), "range ff:");

        let item = WorkItem {
            kind: WorkItemKind::Lookup(Arc::new(vec![vec![0], vec![1], vec![2]]), 0..3),
            column: DBCol::BlockHeader,
        }
        .divide();
        assert_eq!(item.len(), 3);
        assert_eq!(item[0].to_string(), "keys 0..1");
        assert_eq!(item[1].to_string(), "keys 1..2");
        assert_eq!(item[2].to_string(), "keys 2..3");
    }

    #[test]
    fn test_parallel_iteration() {
        let store = create_test_store();
        let mut update = store.store_update();
        let mut keys = BTreeSet::new();
        for i in 0..1000 {
            let key_len = rand::random::<usize>() % 10;
            let key = (0..key_len).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
            let value = Vec::new();
            if keys.insert(key.clone()) {
                update.insert(DBCol::BlockHeader, key, value);
            }
        }
        update.commit().unwrap();

        let got_keys = Arc::new(Mutex::new(BTreeSet::new()));
        let got_keys_clone = got_keys.clone();
        let callback = move |key: &[u8], _value: Option<&[u8]>| {
            got_keys_clone.lock().unwrap().insert(key.to_vec());
        };

        RocksDBParallelIterator::for_each_parallel(
            store,
            DBCol::BlockHeader,
            Vec::new(),
            Vec::new(),
            3,
            callback,
        );

        let got_keys = got_keys.lock().unwrap();
        assert_eq!(*got_keys, keys);
    }

    #[test]
    fn test_parallel_lookup() {
        let store = create_test_store();
        let mut update = store.store_update();
        let mut keys = BTreeSet::new();
        for i in 0..1000 {
            let key_len = rand::random::<usize>() % 10;
            let key = (0..key_len).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
            let value = Vec::new();
            if keys.insert(key.clone()) {
                update.insert(DBCol::BlockHeader, key, value);
            }
        }
        update.commit().unwrap();

        let got_keys = Arc::new(Mutex::new(BTreeSet::new()));
        let got_keys_clone = got_keys.clone();
        let callback = move |key: &[u8], _value: Option<&[u8]>| {
            got_keys_clone.lock().unwrap().insert(key.to_vec());
        };

        RocksDBParallelIterator::lookup_parallel(
            store,
            DBCol::BlockHeader,
            keys.iter().cloned().collect(),
            3,
            callback,
        );

        let got_keys = got_keys.lock().unwrap();
        assert_eq!(*got_keys, keys);
    }
}
