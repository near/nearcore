use std::cmp::min;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Instant;

use futures::Stream;

use crate::protocol::SimplePackedMessage;
use crate::proxy::ProxyHandler;

pub struct BenchmarkHandler {
    instants: Arc<RwLock<Vec<Instant>>>
}

impl BenchmarkHandler {
    pub fn new() -> Self {
        Self {
            instants: Arc::new(RwLock::new(vec![]))
        }
    }
}

/// Messages will be dropped with probability `dropout_rate `
impl ProxyHandler for BenchmarkHandler {
    fn pipe_stream(&self, stream: Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync>) ->
    Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync>
    {
        let instants = self.instants.clone();

        Box::new(stream.map(move |package| {
            let now = Instant::now();

            instants.write().expect("The lock was poisoned.").push(now);

            let guard = instants.read().expect("The lock was poisoned.");

            let size = min(1000, guard.len());

            if size > 1 {
                let delta = guard[guard.len() - 1] - guard[guard.len() - size];
                let x = delta.as_millis() as f64;
                let messages_per_second = (size as f64) / x * 1000f64;
                println!("Messages per second: {}", messages_per_second);
            }

            package
        }))
    }
}
