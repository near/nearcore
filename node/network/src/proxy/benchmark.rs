use futures::Stream;
use rand::Rng;

use crate::protocol::Package;
use crate::proxy::ProxyHandler;
use std::time::Instant;
use std::sync::RwLock;
use std::sync::Arc;
use std::cmp::min;


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
    fn pipe_stream(&self, stream: Box<Stream<Item=Package, Error=()> + Send + Sync>) ->
    Box<Stream<Item=Package, Error=()> + Send + Sync>
    {
        Box::new(stream.map(|package| {
            let now = Instant::now();

            self.instants.write().expect("The lock was poisoned.").push(now);

            let guard = self.instants.read().expect("The lock was poisoned.");

            let size = min(1000, guard.len());

            if size > 1 {
                let delta = guard[guard.len() - 1] - guard[guard.len() - size];
                let x = delta.as_millis() as f64;
                let messages_per_second = (size as f64) / x * 1000f64;
                println!("{}", messages_per_second);
            }

            package
        }))
    }
}
