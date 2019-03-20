use std::sync::Arc;

use futures::stream::Stream;
use futures::sync::mpsc::{channel, Receiver, Sender};

use primitives::serialize::Encode;

use crate::peer::PeerMessage;
use crate::protocol::{forward_msg, PackedMessage};

pub mod dropout;

pub struct Proxy {
    handlers: Vec<Arc<ProxyHandler>>,
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
    pub fn new(handlers: Vec<Arc<ProxyHandler>>) -> Self {
        Self {
            handlers,
        }
    }

    pub fn spawn(&mut self, inc_messages: Receiver<PackedMessage>) {
        let mut final_messages = Some(inc_messages);

        for mut handler in self.handlers.iter() {
            let (inc_proxy_message, out_proxy_message) = channel(1024);
            handler.spawn(final_messages.take().expect("Channel always exists."), inc_proxy_message);
            final_messages = Some(out_proxy_message);
        }

        self.send_final_message(final_messages.take().expect("Channel always exists."));
    }

    fn send_final_message(&self, inc_messages: Receiver<PackedMessage>) {
        let task = inc_messages.for_each(|PackedMessage(message, mut channels)| {
            for channel in channels.drain(..) {
                let data = Encode::encode(&message).unwrap();
                forward_msg(channel, PeerMessage::Message(data));
            }
            Ok(())
        });

        tokio::spawn(task);
    }
}

/// ProxyHandler interface.
pub trait ProxyHandler: Send {
    fn spawn(&mut self, inc_messages: Receiver<PackedMessage>, out_messages: Sender<PackedMessage>) {
        let task = inc_messages.for_each(|packed_message: PackedMessage| {
            self.pipe_one(packed_message, out_messages.clone());
            Ok(())
        });

        tokio::spawn(task);
    }

    /// The input of each handler is output of the previous handler in the `pipeline`.
    fn pipe_one(&mut self, packed_message: PackedMessage, out_messages: Sender<PackedMessage>);
}