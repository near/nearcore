use std::time::Duration;

use futures::Stream;
use rand::Rng;

use crate::protocol::Package;
use crate::proxy::ProxyHandler;
use futures::sync::mpsc::channel;
use futures::sink::Sink;
use futures::future::Future;

pub struct Throttling {
    max_delay_ms: f64
}

impl Throttling {
    fn new(max_delay_ms: f64) -> Self {
        Self {
            max_delay_ms
        }
    }
}

/// Messages will be dropped with probability `dropout_rate `
impl ProxyHandler for Throttling {
    fn pipe_stream(&self, stream: Box<Stream<Item=Package, Error=()> + Send + Sync>) ->
    Box<Stream<Item=Package, Error=()> + Send + Sync>
    {
        let (message_tx, message_rx) = channel(1024);

        stream.for_each(move |package| {
            let mut rng = rand::thread_rng();
            let delay = (rng.gen::<f64>() * self.max_delay_ms) as u64;

            let interval = tokio::timer::Interval::new_interval(Duration::from_millis(delay));

            let wait_task = interval.and_then(|_| {
                message_tx
                    .send(package)
                    .map(|_| ())
                    .map_err(|_| ());
                Ok(())
            }).map_err(|_| ());

            tokio::spawn(wait_task);

            Ok(())
        });

        Box::new(message_rx)
    }
}
