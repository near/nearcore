use futures::Stream;

use crate::protocol::Package;
use crate::proxy::ProxyHandler;

pub struct DebugHandler {}

impl DebugHandler {
    pub fn new() -> Self {
        Self {}
    }
}

/// Messages will be dropped with probability `dropout_rate `
impl ProxyHandler for DebugHandler {
    fn pipe_stream(&self, stream: Box<Stream<Item=Package, Error=()> + Send + Sync>) ->
    Box<Stream<Item=Package, Error=()> + Send + Sync>
    {
        Box::new(stream.map(|package| {
            println!("{:?}", package);
            package
        }))
    }
}
