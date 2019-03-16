use std::sync::Arc;

use super::message::Message;

pub mod dropout;

pub struct Proxy {
    handlers: Vec<Arc<ProxyHandler>>
}

/// All messages before being sent to other participants will be processed by several handlers
/// chained sequentially in a pipeline.
///
/// Each handler will receive an iterator of messages and will return (not necessarily the same)
/// iterator of messages. This method allows easy manipulation of messages before being emitted
/// to other authorities through the network.
///
/// The order in which handlers are grouped can affect the behavior of the proxy.
impl Proxy {
    pub fn new() -> Self {
        Self {
            handlers: vec![]
        }
    }

    pub fn pipe<'a>(&'a self, message: &'a Message) -> Vec<&'a Message> {
        let start_messages = vec![message];

        self.handlers
            .iter()
            .fold(start_messages, |cur_messages, handler| {
                handler.pipe(cur_messages)
            })
    }
}

/// ProxyHandler interface.
pub trait ProxyHandler where Self: Send + Sync {
    fn pipe(&self, mut messages: Vec<&Message>) -> Vec<&Message> {
        messages
            .drain(..)
            .map(|message| self.pipe_one(message))
            .fold(vec![], |mut cur_messages, new_messages| {
                cur_messages.extend(new_messages);
                cur_messages
            })
    }

    /// Logic of the handler. This function will be called once for every message that get
    /// to this point in the same order messages arrive to the proxy.
    ///
    /// The input of each handler is output of the previous handler in the `pipeline`.
    fn pipe_one(&self, message: &Message) -> Vec<&Message>;
}