use std::sync::Arc;

use futures::stream::Stream;
use futures::sync::mpsc::{channel, Receiver, Sender};

use primitives::serialize::Encode;

use crate::peer::PeerMessage;
use crate::protocol::{forward_msg, PackedMessage};
use crate::message::Message;
use futures::Poll;
use crate::protocol::Package;

pub mod dropout;

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

    pub fn spawn(&mut self, inc_messages: Receiver<PackedMessage>) {
        let mut stream = Some(to_package_stream(inc_messages));

        for mut handler in self.handlers.iter() {
            stream = Some(handler.pipe_stream(stream.take().expect("Channel always exists.")));
        }

        self.send_final_message(stream.take().expect("Channel always exists."));
    }

    fn send_final_message(&self, stream: impl Stream<Item=Package, Error=()>) {
        let task = stream.for_each(|(message, channel)| {
            let data = Encode::encode(&message).unwrap();
            forward_msg(channel, PeerMessage::Message(data));
            Ok(())
        });

        tokio::spawn(task);
    }
}

fn to_package_stream(inc_messages: Receiver<PackedMessage>) -> Box<Stream<Item=Package, Error=()>> {
    unimplemented!()
}

/// ProxyHandler interface.
pub trait ProxyHandler {
    fn pipe_stream(&self, s: Box<Stream<Item=Package, Error=()>>) -> Box<Stream<Item=Package, Error=()>>;
}