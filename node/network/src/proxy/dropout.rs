use futures::Stream;
use rand::Rng;

use crate::protocol::SimplePackedMessage;
use crate::proxy::ProxyHandler;

/// Messages passing through this handler will be dropped with probability `dropout_rate`.
pub struct DropoutHandler {
    dropout_rate: f64
}

impl DropoutHandler {
    pub fn new(dropout_rate: f64) -> Self {
        Self {
            dropout_rate
        }
    }
}

impl ProxyHandler for DropoutHandler {
    fn pipe_stream(&self, stream: Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync>) ->
    Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync>
    {
        let dropout_rate = self.dropout_rate;
        Box::new(stream.filter(move |_| {
            let mut rng = rand::thread_rng();
            rng.gen::<f64>() >= dropout_rate
        }))
    }
}
