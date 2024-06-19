use std::sync::Arc;

use near_time::Duration;

use super::data::TestLoopData;

type TestLoopCallback = Box<dyn FnOnce(&mut TestLoopData) + Send>;

/// Interface to send an event with a delay (in virtual time).
#[derive(Clone)]
pub struct PendingEventsSender {
    client_index: usize,
    sender: Arc<dyn Fn(CallbackEvent) + Send + Sync>,
}

impl PendingEventsSender {
    pub(crate) fn new(f: impl Fn(CallbackEvent) + Send + Sync + 'static) -> Self {
        Self { client_index: 0, sender: Arc::new(f) }
    }

    pub(crate) fn set_index(&mut self, index: usize) {
        self.client_index = index;
    }

    /// Set the index of the actor that is sending the event.
    /// This is purely for debug purposes and does not affect the execution of the event.
    pub fn for_index(mut self, index: usize) -> Self {
        self.set_index(index);
        self
    }

    /// Schedule a callback to be executed. TestLoop follows the fifo order of executing events.
    pub fn send(&self, description: String, callback: TestLoopCallback) {
        self.send_with_delay(description, callback, Duration::ZERO);
    }

    /// Schedule a callback to be executed after a delay.
    pub fn send_with_delay(
        &self,
        description: String,
        callback: TestLoopCallback,
        delay: Duration,
    ) {
        let description = format!("({},{})", self.client_index, description);
        (self.sender)(CallbackEvent { description, callback, delay });
    }
}

/// CallbackEvent for testloop is a simple event with a single callback which gets executed. This takes in
/// testloop data as a parameter which can be used alongside with data handlers to access data.
/// This is very versatile and we can potentially have anything as a callback. For example, for the case of Senders
/// and Handlers, we can have a simple implementation of the CanSend function as a callback calling the Handle function
/// of the actor.
pub(crate) struct CallbackEvent {
    pub(crate) callback: TestLoopCallback,
    pub(crate) delay: Duration,
    pub(crate) description: String,
}
