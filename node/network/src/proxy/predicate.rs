use futures::Stream;

use crate::protocol::Package;
use crate::proxy::ProxyHandler;
use std::sync::Arc;

pub struct FnHandler<F> where F : Fn(Package) -> Option<Package>{
    predicate: Arc<Send + Sync + F>
}

/// Use `predicate` to change/filter package going through this proxy.
/// Packages can be changed or filtered out in the same way as in `filter_map`.
impl ProxyHandler for FnHandler {
    fn pipe_stream(&self, stream: Box<Stream<Item=Package, Error=()> + Send + Sync>) ->
    Box<Stream<Item=Package, Error=()> + Send + Sync>
    {
        Box::new(stream.filter_map(self.predicate.clone()))
    }
}

//fn create_handler() -> FnHandler {
//    FnHandler {
//        predicate: |package| {
//        }
//    }
//}