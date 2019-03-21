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
    fn pipe_stream(&self, s: Box<Stream<Item=Package, Error=()> + Send + Sync>) -> Box<
        Stream<Item=Package, Error=()> + Send + Sync> {
        let dropout_rate = self.dropout_rate;
        Box::new(s.filter(move |_| {
            let mut rng = rand::thread_rng();
            rng.gen::<f64>() >=dropout_rate
        }))
    }
}
