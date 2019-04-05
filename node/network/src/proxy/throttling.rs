use std::time::{Duration, Instant};

use futures::future::Future;
use futures::sink::Sink;
use futures::Stream;
use futures::sync::mpsc::channel;
use rand::Rng;
use tokio::timer::Delay;

use crate::protocol::SimplePackedMessage;
use crate::proxy::ProxyHandler;

/// Messages passing through this handler will be delayed by a random number of milliseconds
/// in the range `[0, max_delay_ms)`
pub struct ThrottlingHandler {
    max_delay_ms: u64
}

impl ThrottlingHandler {
    pub fn new(max_delay_ms: u64) -> Self {
        Self {
            max_delay_ms
        }
    }
}

/// Messages will be dropped with probability `dropout_rate `
impl ProxyHandler for ThrottlingHandler {
    fn pipe_stream(&self, stream: Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync>) ->
    Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync>
    {
        let (message_tx, message_rx) = channel(1024);

        let max_delay_ms = self.max_delay_ms;

        let main_task = stream.for_each(move |package| {
            let mut rng = rand::thread_rng();
            let delay = (rng.gen::<f64>() * max_delay_ms as f64) as u64;

            let final_time = Delay::new(Instant::now() + Duration::from_millis(delay));
            let message_tx1 = message_tx.clone();

            let wait_task = final_time.and_then(|_| {
                let task = message_tx1
                    .send(package)
                    .map(|_| ())
                    .map_err(|_| ());

                tokio_utils::spawn(task);
                Ok(())
            }).map_err(|_| ());

            tokio_utils::spawn(wait_task);

            Ok(())
        });

        tokio::spawn(main_task);
        Box::new(message_rx)
    }
}
