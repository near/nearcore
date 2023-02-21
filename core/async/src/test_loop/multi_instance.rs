use super::{delay_sender::DelaySender, event_handler::LoopEventHandler};

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
    pub(crate) fn new(inner: Box<dyn LoopEventHandler<Data, Event>>, index: usize) -> Self {
        Self { inner, index }
    }
}

impl<Data, Event: 'static> LoopEventHandler<Vec<Data>, (usize, Event)>
    for IndexedLoopEventHandler<Data, Event>
{
    fn init(&mut self, sender: DelaySender<(usize, Event)>) {
        self.inner.init(sender.for_index(self.index))
    }

    fn handle(
        &mut self,
        event: (usize, Event),
        data: &mut Vec<Data>,
    ) -> Result<(), (usize, Event)> {
        if event.0 == self.index {
            self.inner.handle(event.1, &mut data[self.index]).map_err(|event| (self.index, event))
        } else {
            Err(event)
        }
    }
}
