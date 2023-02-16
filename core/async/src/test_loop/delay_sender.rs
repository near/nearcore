use std::{sync::Arc, time::Duration};

use crate::messaging;

use super::multi_instance::IndexedDelaySender;

/// Interface to send an event with a delay (in virtual time). It can be
/// converted to a Sender for any message type that can be converted into
/// the event type, so that a DelaySender given by the test loop may be passed
/// to production code that expects a Sender.
pub struct DelaySender<Event> {
    // We use an impl object here because it makes the interop with other
    // traits much easier.
    im: Arc<dyn CanSendWithDelay<Event>>,
}

impl<Message, Event: From<Message> + 'static> messaging::CanSend<Message> for DelaySender<Event> {
    fn send(&self, message: Message) {
        self.send_with_delay(message.into(), Duration::ZERO);
    }
}

/// Underlying trait for DelaySender.
pub(crate) trait CanSendWithDelay<Event>: Send + Sync {
    fn send_with_delay(&self, event: Event, delay: Duration);
}

/// Implements any CanSendWithDelay<Other> as long as Other can be converted into Event.
pub struct NarrowingDelaySender<Event> {
    sender: DelaySender<Event>,
}

impl<Event, OuterEvent: From<Event>> CanSendWithDelay<Event> for NarrowingDelaySender<OuterEvent> {
    fn send_with_delay(&self, event: Event, delay: Duration) {
        self.sender.send_with_delay(event.into(), delay);
    }
}

impl<Event> DelaySender<Event> {
    pub(crate) fn new<Impl: CanSendWithDelay<Event> + 'static>(im: Impl) -> Self {
        Self { im: Arc::new(im) }
    }

    pub fn send_with_delay(&self, event: Event, delay: Duration) {
        self.im.send_with_delay(event, delay);
    }

    /// Converts into a sender that can send any event convertible into this event type.
    pub fn narrow<InnerEvent>(self) -> DelaySender<InnerEvent>
    where
        Event: From<InnerEvent> + 'static,
    {
        DelaySender { im: Arc::new(NarrowingDelaySender { sender: self }) }
    }
}

impl<Event: 'static> DelaySender<(usize, Event)> {
    /// Converts a multi-instance sender to a single-instance sender.
    pub fn for_index(self, index: usize) -> DelaySender<Event> {
        DelaySender { im: Arc::new(IndexedDelaySender::new(self.im, index)) }
    }
}

/// Custom implementation because #derive wouldn't work if Event does not Clone.
impl<Event> Clone for DelaySender<Event> {
    fn clone(&self) -> Self {
        Self { im: self.im.clone() }
    }
}
