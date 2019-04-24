use futures::Stream;

use crate::protocol::SimplePackedMessage;
use crate::proxy::ProxyHandler;

pub struct FnProxyHandler {
    predicate: fn(SimplePackedMessage) -> Option<SimplePackedMessage>
}

/// Use `predicate` to change/filter package going through this proxy.
/// Packages can be changed or filtered out in the same way as in `filter_map`.
impl ProxyHandler for FnProxyHandler {
    fn pipe_stream(&self, stream: Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync>) ->
    Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync>
    {
        Box::new(stream.filter_map(self.predicate))
    }
}

impl FnProxyHandler {
    pub fn new(predicate: fn(SimplePackedMessage) -> Option<SimplePackedMessage>) -> Self {
        Self {
            predicate
        }
    }
}