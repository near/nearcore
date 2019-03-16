use crate::proxy::ProxyHandler;
use crate::message::Message;
use rand::Rng;

const DROPOUT_RATE: f64 = 0.5;

pub struct Dropout {}

impl ProxyHandler for Dropout {
    fn pipe_once(&self, message: &Message) -> Vec<&Message> {
        let mut rng = rand::thread_rng();

        if rng.gen::<f64>() < DROPOUT_RATE {
            vec![]
        } else {
            vec![message]
        }
    }
}