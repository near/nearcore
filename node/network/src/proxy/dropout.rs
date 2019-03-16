use rand::Rng;

use crate::message::Message;
use crate::proxy::ProxyHandler;

const DROPOUT_RATE: f64 = 0.5;

pub struct Dropout {}

/// Messages will be dropped with probability `1/2`
impl ProxyHandler for Dropout {
    fn pipe_one(&self, message: &Message) -> Vec<&Message> {
        let mut rng = rand::thread_rng();

        if rng.gen::<f64>() < DROPOUT_RATE {
            vec![]
        } else {
            vec![message]
        }
    }
}