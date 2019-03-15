use crate::proxy::ProxyHandler;
use crate::message::Message;
use rand::Rng;

pub struct Dropout {
    dropout_rate: f64,
}

impl ProxyHandler for Dropout {
    fn new() -> Self {
        Self {
            dropout_rate: 0.5
        }
    }

    fn pipe_once(&self, message: &Message) -> Vec<&Message> {
        let mut rng = rand::thread_rng();

        if rng.gen() < self.dropout_rate {
            vec![]
        } else {
            vec![message]
        }
    }
}