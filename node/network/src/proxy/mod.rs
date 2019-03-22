use std::sync::Arc;

use futures::Poll;
use futures::stream::Stream;
use futures::sync::mpsc::Receiver;
use tokio::prelude::Async;

use primitives::serialize::Encode;

use crate::peer::PeerMessage;
use crate::protocol::{forward_msg, PackedMessage};
use crate::protocol::Package;

/// Proxy Handlers implementations
pub mod dropout;
//pub mod throttling;
pub mod debug;

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
    pub fn spawn(&mut self, inc_messages: Receiver<PackedMessage>) {
        let mut stream: Box<Stream<Item=Package, Error=()> + Send + Sync> = Box::new(PackedMessageStream::new(inc_messages));

        for mut handler in self.handlers.iter() {
            stream = handler.pipe_stream(stream);
        }

        self.send_final_message(stream);
    }

    /// Send message received from the proxy to their final destination.
    fn send_final_message(&self, stream: Box<Stream<Item=Package, Error=()> + Send + Sync>) {
        let task = stream.for_each(move |(message, channel)| {
            let data = Encode::encode(&message).unwrap();
            forward_msg(channel, PeerMessage::Message(data));
            Ok(())
        });

        tokio::spawn(task);
    }
}

/// Data structure used uniquely to concatenate messages from `inc_messages`
struct PackedMessageStream {
    inc_message: Receiver<PackedMessage>,
    active_stream: Option<Box<Stream<Item=Package, Error=()>>>,
}

impl PackedMessageStream {
    fn new(inc_message: Receiver<PackedMessage>) -> Self {
        Self {
            inc_message,
            active_stream: None,
        }
    }
}

impl Stream for PackedMessageStream {
    type Item = Package;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            // Try to consume current stream
            if let Some(stream) = &mut self.active_stream {
                let package_option = try_ready!(stream.poll());

                if let Some(package) = package_option {
                    return Ok(Async::Ready(Some(package)));
                } else {
                    // If the stream was completely consumed drop it
                    self.active_stream.take();
                }
            }

            let packed_message_option = try_ready!(self.inc_message.poll());

            if let Some(packed_message) = packed_message_option {
                self.active_stream = Some(Box::new(packed_message.to_stream()));
            } else {
                break;
            }
        }

        Ok(Async::Ready(None))
    }
}

unsafe impl Send for PackedMessageStream {}

unsafe impl Sync for PackedMessageStream {}

/// ProxyHandler interface.
pub trait ProxyHandler: Send + Sync {
    fn pipe_stream(&self, stream: Box<Stream<Item=Package, Error=()> + Send + Sync>) -> Box<Stream<Item=Package, Error=()> + Send + Sync>;
}
