use futures::future::Future;
use futures::sink::Sink;
use futures::sync::mpsc::Sender;
use log::warn;
use rand::Rng;

use crate::protocol::PackedMessage;
use crate::proxy::ProxyHandler;

pub struct Dropout {
    dropout_rate: f64
}

/// Messages will be dropped with probability `1/2`
impl ProxyHandler for Dropout {
    fn pipe_one(&mut self, packed_message: PackedMessage, out_messages: Sender<PackedMessage>) {
        let mut rng = rand::thread_rng();

        if rng.gen::<f64>() < self.dropout_rate {
            // Drop message with probability `dropout_rate`
        } else {
            let task =
                out_messages
                    .send(packed_message)
                    .map(|_| ())
                    .map_err(|e| warn!("Error sending message inside proxy."));
            tokio::spawn(task);
        }
    }
}