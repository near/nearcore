use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use futures::future::Future;
use futures::Stream;
use log::warn;
use tokio::timer::Interval;

use crate::message::Message;
use crate::protocol::SimplePackedMessage;
use crate::proxy::ProxyHandler;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";
const MONITOR_LOG_PATH: &str = "monitor.log";
const DUMP_EVERY_X_SEC: u64 = 1;

pub struct BenchmarkHandler {
    started: Arc<RwLock<bool>>,
    message_type_counter: Arc<RwLock<Vec<usize>>>,
}

fn message_enum_index(message: &Message) -> usize {
    match message {
        Message::Gossip(_) => 0,
        Message::Connected(_) => 1,
        Message::Transaction(_) => 2,
        Message::Receipt(_) => 3,
        Message::BlockAnnounce(_) => 4,
        Message::BlockFetchRequest(_, _, _) => 5,
        Message::BlockResponse(_, _, _) => 6,
        Message::PayloadGossip(_) => 7,
        Message::PayloadRequest(_, _) => 8,
        Message::PayloadSnapshotRequest(_, _) => 9,
        Message::PayloadResponse(_, _) => 10,
        Message::PayloadSnapshotResponse(_, _) => 11,
        Message::JointBlockBLS(_) => 12,
    }
}

/// Build string identifier for Message.
/// Inverse of `message_enum_index` order.
fn message_enum_id(index: usize) -> String {
    match index {
        0 => "Gossip",
        1 => "Connected",
        2 => "Transaction",
        3 => "Receipt",
        4 => "BlockAnnounce",
        5 => "BlockFetchRequest",
        6 => "BlockResponse",
        7 => "PayloadGossip",
        8 => "PayloadRequest",
        9 => "PayloadSnapshotRequest",
        10 => "PayloadResponse",
        11 => "PayloadSnapshotResponse",
        12 => "JointBlockBLS",
        _ => panic!("Invalid message enum index."),
    }
    .to_string()
}

impl BenchmarkHandler {
    #[allow(clippy::new_without_default)]
    // Note: this is not default
    pub fn new() -> Self {
        Self {
            started: Arc::new(RwLock::new(false)),
            message_type_counter: Arc::new(RwLock::new(vec![0; 13])),
        }
    }

    /// This function can't be called on new method, because there must exist a tokio
    fn start(&self) {
        let counter = self.message_type_counter.clone();

        let dump_task = Interval::new_interval(Duration::from_secs(DUMP_EVERY_X_SEC))
            .for_each(move |_| {
                BenchmarkHandler::dump(counter.clone());
                Ok(())
            })
            .map(|_| ())
            .map_err(|e| warn!("Error dumping data. {:?}", e));

        tokio::spawn(dump_task);
    }

    fn is_started(&self) -> bool {
        *self.started.read().expect(POISONED_LOCK_ERR)
    }

    pub fn dump(message_type_counter: Arc<RwLock<Vec<usize>>>) {
        let counters = message_type_counter.read().expect(POISONED_LOCK_ERR);

        let path = Path::new(MONITOR_LOG_PATH);

        let mut file = match OpenOptions::new().create(true).append(true).open(path) {
            Err(why) => panic!("Fail opening file {:?}: {:?}", path, why),
            Ok(file) => file,
        };

        file.write_all(&format!("\n{:?}\n", Instant::now()).into_bytes())
            .unwrap_or_else(|_| panic!("Fail writing to {:?}", path));

        for (index, count) in counters.iter().enumerate() {
            file.write_all(&format!("{:?}: {:?}\n", message_enum_id(index), count).into_bytes())
                .unwrap_or_else(|_| panic!("Fail writing to {:?}", path));
        }
    }
}

/// Messages will be dropped with probability `dropout_rate `
impl ProxyHandler for BenchmarkHandler {
    fn pipe_stream(
        &self,
        stream: Box<Stream<Item = SimplePackedMessage, Error = ()> + Send + Sync>,
    ) -> Box<Stream<Item = SimplePackedMessage, Error = ()> + Send + Sync> {
        // Call start only once.
        if !self.is_started() {
            // Can't start without an active tokio runtime.
            self.start();
        }

        let counter = self.message_type_counter.clone();

        Box::new(stream.map(move |package| {
            let index = message_enum_index(&package.0);

            counter.write().expect(POISONED_LOCK_ERR)[index] += 1;

            package
        }))
    }
}
