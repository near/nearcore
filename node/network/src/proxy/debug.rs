use futures::Stream;

use crate::protocol::SimplePackedMessage;
use crate::proxy::ProxyHandler;

pub struct DebugHandler {}

impl DebugHandler {
    pub fn new() -> Self {
        Self {}
    }
}

impl ProxyHandler for DebugHandler {
    fn pipe_stream(&self, stream: Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync>) ->
    Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync>
    {
        Box::new(stream.map(|package| {
            println!("{:?}", package);
            package
        }))
    }
}
