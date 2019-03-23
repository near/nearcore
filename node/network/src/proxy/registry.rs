use std::sync::Arc;

use crate::proxy::{Proxy, ProxyHandler};
use crate::proxy::debug::DebugHandler;
use crate::proxy::dropout::DropoutHandler;
use crate::proxy::throttling::ThrottlingHandler;

impl Proxy {
    pub fn get_handler(name: &str) -> Arc<ProxyHandler> {
        let handler: Arc<ProxyHandler> = match &name.to_lowercase()[..] {
//            "benchmark" => Arc::new(BenchmarkHandler::new()),

            "debug" => Arc::new(DebugHandler::new()),

            "dropout" => Arc::new(DropoutHandler::new(0.5f64)),

            "throttling" => Arc::new(ThrottlingHandler::new(50)),

            _ => panic!("Fail!")
        };

        handler
    }
}