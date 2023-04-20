use crate::messaging;
use crate::time;
use std::sync::Arc;

/// Interface to send an event with a delay (in virtual time). It can be
/// converted to a Sender for any message type that can be converted into
/// the event type, so that a DelaySender given by the test loop may be passed
/// to production code that expects a Sender.
pub struct DelaySender<Event>(Arc<dyn Fn(Event, time::Duration) + Send + Sync>);

impl<Message, Event: From<Message> + 'static> messaging::CanSend<Message> for DelaySender<Event> {
    fn send(&self, message: Message) {
        self.send_with_delay(message.into(), time::Duration::ZERO);
    }
}

impl<Event> DelaySender<Event> {
    pub fn new(inner: impl Fn(Event, time::Duration) + Send + Sync + 'static) -> Self {
        Self(Arc::new(inner))
    }

    pub fn send_with_delay(&self, event: Event, delay: time::Duration) {
        self.0(event, delay);
    }

    pub fn narrow<InnerEvent>(self) -> DelaySender<InnerEvent>
    where
        Event: From<InnerEvent> + 'static,
    {
        DelaySender::<InnerEvent>::new(move |event, delay| {
            self.send_with_delay(event.into(), delay)
        })
    }
}

impl<Event: 'static> DelaySender<(usize, Event)> {
    /// Converts a multi-instance sender to a single-instance sender.
    pub fn for_index(self, index: usize) -> DelaySender<Event> {
        DelaySender::new(move |event, delay| {
            self.send_with_delay((index, event), delay);
        })
    }
}

/// Custom implementation because #derive wouldn't work if Event does not Clone.
impl<Event> Clone for DelaySender<Event> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
