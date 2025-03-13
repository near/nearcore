use std::sync::Arc;

use near_time::Duration;

use super::data::TestLoopData;

type TestLoopCallback = Box<dyn FnOnce(&mut TestLoopData) + Send>;

/// RawPendingEventsSender is used to construct a new PendingEventsSender with an identifier.
/// RawPendingEventsSender can not directly be used to send events.
#[derive(Clone)]
pub struct RawPendingEventsSender(Arc<dyn Fn(CallbackEvent) + Send + Sync>);

impl RawPendingEventsSender {
    pub(crate) fn new(f: impl Fn(CallbackEvent) + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    pub(crate) fn for_identifier(&self, identifier: &str) -> PendingEventsSender {
        PendingEventsSender { identifier: identifier.to_string(), sender: self.clone() }
    }
}

/// Interface to send an event with a delay (in virtual time).
/// This is constructed from a RawPendingEventsSender with an identifier.
/// The identifier is used to distinguish between different nodes.
/// Usually we used the `account_id` of the node as the identifier.
#[derive(Clone)]
pub struct PendingEventsSender {
    identifier: String,
    sender: RawPendingEventsSender,
}

impl PendingEventsSender {
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
        let identifier = self.identifier.clone();
        (self.sender.0)(CallbackEvent { identifier, description, callback, delay });
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
    pub(crate) identifier: String,
    pub(crate) description: String,
}
