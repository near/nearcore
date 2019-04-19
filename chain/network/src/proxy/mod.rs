use std::sync::Arc;

use futures::stream::Stream;
use futures::sync::mpsc::Receiver;

use primitives::network::PeerMessage;
use crate::protocol::{forward_msg, PackedMessage};
use crate::protocol::SimplePackedMessage;
use crate::message::encode_message;

/// Proxy Handlers implementations
pub mod benchmark;
pub mod debug;
pub mod dropout;
pub mod throttling;
pub mod predicate;

pub struct Proxy {
    handlers: Vec<Arc<ProxyHandler>>,
}

/// All messages before being sent to other participants will be processed by several handlers
/// chained sequentially in a pipeline.
///
/// Each handler will receive a stream of messages and will return (not necessarily the same)
/// stream of messages. This method allows easy manipulation of messages before being emitted
/// to other authorities through the network.
///
/// The order in which handlers are grouped can affect the behavior of the proxy.
impl Proxy {
    pub fn new(handlers: Vec<Arc<ProxyHandler>>) -> Self {
        Self {
            handlers,
        }
    }

    /// Spawn proxies, start task that polls messages and pass them through proxy handlers.
    pub fn spawn(&self, inc_messages: Receiver<PackedMessage>) {
        let mut stream: Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync> = Box::new(inc_messages.map(PackedMessage::to_stream).flatten());

        for handler in self.handlers.iter() {
            stream = handler.pipe_stream(stream);
        }

        self.send_final_message(stream);
    }

    /// Send message received from the proxy to their final destination.
    fn send_final_message(&self, stream: Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync>) {
        let task = stream.for_each(move |(message, channel)| {
            let data = encode_message((*message).clone()).unwrap();
            forward_msg(channel, PeerMessage::Message(data));
            Ok(())
        });

        tokio_utils::spawn(task);
    }
}

/// ProxyHandler interface.
pub trait ProxyHandler: Send + Sync {
    fn pipe_stream(&self, stream: Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync>) -> Box<Stream<Item=SimplePackedMessage, Error=()> + Send + Sync>;
}
