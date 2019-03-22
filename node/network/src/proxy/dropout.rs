use futures::Stream;
use rand::Rng;

use crate::protocol::Package;
use crate::proxy::ProxyHandler;

pub struct Dropout {
    dropout_rate: f64
}

impl Dropout {
    fn new(dropout_rate: f64) -> Self {
        Self {
            dropout_rate
        }
    }
}

/// Messages will be dropped with probability `dropout_rate `
impl ProxyHandler for Dropout {
    fn pipe_stream(&self, stream: Box<Stream<Item=Package, Error=()> + Send + Sync>) ->
    Box<Stream<Item=Package, Error=()> + Send + Sync>
    {
        let dropout_rate = self.dropout_rate;
        Box::new(stream.filter(move |_| {
            let mut rng = rand::thread_rng();
            rng.gen::<f64>() >= dropout_rate
        }))
    }
}
