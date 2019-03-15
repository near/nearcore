use std::sync::Arc;

use super::message::Message;

pub mod dropout;

pub struct Proxy<T: ProxyHandler + Send> {
    handlers: Vec<T>
}

impl<T: ProxyHandler + Send> Proxy<T> {
    pub fn new() -> Self {
        Self {
            handlers: vec![]
        }
    }

    pub fn pipe(&self, message: &Message) -> Vec<&Message> {
        let start_messages = vec![message];

        self.handlers
            .iter()
            .fold(start_messages, |cur_messages, handler| {
                handler.pipe(cur_messages)
            })
    }
}


pub trait ProxyHandler {
    fn new() -> Self {}

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