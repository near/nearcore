use std::sync::Arc;

use futures::future::Future;
use futures::sink::Sink;
use futures::Stream;
use futures::sync::mpsc::Sender;
use log::warn;
use rand::Rng;

use crate::message::Message;
use crate::peer::PeerMessage;
use crate::protocol::Package;
use crate::protocol::PackedMessage;
use crate::proxy::ProxyHandler;

pub struct Dropout {
    dropout_rate: f64
}

/// Messages will be dropped with probability `dropout_rate `
impl ProxyHandler for Dropout {
    fn pipe_stream(&self, s: Box<Stream<Item=Package, Error=()>>) -> Box<Stream<Item=Package, Error=()>> {
        let mut rng = rand::thread_rng();
        s.filter(|_| rng.gen::<f64>() >= self.dropout_rate)
    }
}