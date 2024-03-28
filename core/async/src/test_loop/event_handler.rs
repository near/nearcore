/// An event handler registered on a test loop. Each event handler usually
/// handles only some events, so we will usually have multiple event handlers
/// registered to cover all event types.
pub struct LoopEventHandler<Data: 'static, Event: 'static>(
    Box<dyn FnMut(Event, &mut Data) -> Result<(), Event>>,
);

impl<Data, Event> LoopEventHandler<Data, Event> {
    /// Creates a handler from the handling logic function. The function is
    /// called on each event. It should return Ok(()) if the event was handled,
    /// or Err(event) if the event was not handled (which will cause it to be
    /// passed to the next handler).
    pub fn new(handler: impl FnMut(Event, &mut Data) -> Result<(), Event> + 'static) -> Self {
        Self(Box::new(handler))
    }

    /// Like new(), but the handler is not given the ability to reject the event.
    pub fn new_simple(mut handler: impl FnMut(Event, &mut Data) + 'static) -> Self {
        Self::new(move |event, data| {
            handler(event, data);
            Ok(())
        })
    }

    /// Adapts this handler to a handler whose data is a superset of our data
    /// and whose event is a superset of our event.
    ///   For data, A is a superset of B if A implements AsRef<B> and AsMut<B>.
    ///   For event, A is a superset of B if A implements From<B> and
    ///     TryIntoOrSelf<B>.
    pub fn widen<
        OuterData: AsMut<Data>,
        OuterEvent: TryIntoOrSelf<Event> + From<Event> + 'static,
    >(
        mut self,
    ) -> LoopEventHandler<OuterData, OuterEvent> {
        LoopEventHandler(Box::new(move |event, data| {
            let mut inner_data = data.as_mut();
            let inner_event = event.try_into_or_self()?;
            self.0(inner_event, &mut inner_data)?;
            Ok(())
        }))
    }

    /// Adapts this handler to a handler whose data is a vector of our data,
    /// and whose event is a is the tuple (index, our event), for a specific
    /// index.
    pub fn for_index(mut self, index: usize) -> LoopEventHandler<Vec<Data>, (usize, Event)> {
        LoopEventHandler(Box::new(move |event, data| {
            if event.0 == index {
                self.0(event.1, &mut data[index]).map_err(|event| (index, event))
            } else {
                Err(event)
            }
        }))
    }

    pub(crate) fn handle(&mut self, event: Event, data: &mut Data) -> Result<(), Event> {
        self.0(event, data)
    }
}

/// A convenient trait to TryInto, or else return the original object. It's useful
/// for implementing event handlers.
pub trait TryIntoOrSelf<R>: Sized {
    fn try_into_or_self(self) -> Result<R, Self>;
}

impl<R, T: TryInto<R, Error = T>> TryIntoOrSelf<R> for T {
    fn try_into_or_self(self) -> Result<R, Self> {
        self.try_into()
    }
}

/// An event handler that puts the event into a vector in the Data, as long as
/// the Data contains a Vec<CapturedEvent>. (Use widen() right after).
///
/// This is used on output events so that after the test loop finishes running
/// we can assert on those events.
pub fn capture_events<Event>() -> LoopEventHandler<Vec<Event>, Event> {
    LoopEventHandler::new_simple(|event, data: &mut Vec<Event>| data.push(event))
}

/// An event handler that ignores all events.
pub fn ignore_events<Event>() -> LoopEventHandler<(), Event> {
    LoopEventHandler::new_simple(|_, _| {})
}
