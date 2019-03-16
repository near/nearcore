
use super::message::Message;
use std::sync::Arc;

pub mod dropout;

pub struct Proxy {
    handlers: Vec<Arc<ProxyHandler>>
}

impl Proxy {
    pub fn new() -> Self {
        Self {
            handlers: vec![]
        }
    }

    pub fn pipe<'a>(&'a self, message: &'a Message) -> Vec<&'a Message> {
        let start_messages = vec![message];

        self.handlers
            .iter()
            .fold(start_messages, |cur_messages, handler| {
                handler.pipe(cur_messages)
            })
    }
}

pub trait ProxyHandler where Self: Send + Sync {
    fn pipe(&self, mut messages: Vec<&Message>) -> Vec<&Message> {
        messages
            .drain(..)
            .map(|message| self.pipe_once(message))
            .fold(vec![], |mut cur_messages, new_messages| {
                cur_messages.extend(new_messages);
                cur_messages
            })
    }

    fn pipe_once(&self, message: &Message) -> Vec<&Message>;
}