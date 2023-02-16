use std::sync::Arc;

use super::{
    delay_sender::{CanSendWithDelay, DelaySender},
    event_handler::LoopEventHandler,
};

/// DelaySender that attaches an index to every event and forwards it on to
/// another DelaySender that takes a tuple of the index and the event.
pub(crate) struct IndexedDelaySender<Event> {
    outer: Arc<dyn CanSendWithDelay<(usize, Event)>>,
    index: usize,
}

impl<Event> IndexedDelaySender<Event> {
    pub fn new(outer: Arc<dyn CanSendWithDelay<(usize, Event)>>, index: usize) -> Self {
        Self { outer, index }
    }
}

impl<Event> CanSendWithDelay<Event> for IndexedDelaySender<Event> {
    fn send_with_delay(&self, event: Event, delay: std::time::Duration) {
        self.outer.send_with_delay((self.index, event), delay);
    }
}

/// Event handler that handles a specific single instance in a multi-instance
/// setup.
///
/// To convert a single-instance handler to a multi-instance handler
/// (for one instance), use handler.for_index(index).
pub struct IndexedLoopEventHandler<Data, Event> {
    inner: Box<dyn LoopEventHandler<Data, Event>>,
    index: usize,
}

impl<Data, Event> IndexedLoopEventHandler<Data, Event> {
    pub fn new(inner: Box<dyn LoopEventHandler<Data, Event>>, index: usize) -> Self {
        Self { inner, index }
    }
}

impl<Data, Event: 'static> LoopEventHandler<Vec<Data>, (usize, Event)>
    for IndexedLoopEventHandler<Data, Event>
{
    fn init(&mut self, sender: DelaySender<(usize, Event)>) {
        self.inner.init(sender.for_index(self.index))
    }

    fn handle(&mut self, event: (usize, Event), data: &mut Vec<Data>) -> Option<(usize, Event)> {
        if event.0 == self.index {
            self.inner.handle(event.1, &mut data[self.index]).map(|event| (self.index, event))
        } else {
            Some(event)
        }
    }
}
