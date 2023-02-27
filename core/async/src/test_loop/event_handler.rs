use near_primitives::time;

use super::{delay_sender::DelaySender, multi_instance::IndexedLoopEventHandler};

/// Context given to the loop handler on each call.
pub struct LoopHandlerContext<Event> {
    /// The sender that can be used to send more messages to the loop.
    pub sender: DelaySender<Event>,
    /// The clock whose .now() returns the current virtual time maintained by
    /// the test loop.
    pub clock: time::Clock,
}

/// An event handler registered on a test loop. Each event handler usually
/// handles only some events, so we will usually have multiple event handlers
/// registered to cover all event types.
pub struct LoopEventHandler<Data: 'static, Event: 'static> {
    inner: Box<dyn LoopEventHandlerImpl<Data, Event>>,
}

impl<Data, Event> LoopEventHandler<Data, Event> {
    /// Creates a handler from the handling logic function. The function is
    /// called on each event. It should return Ok(()) if the event was handled,
    /// or Err(event) if the event was not handled (which will cause it to be
    /// passed to the next handler).
    pub fn new(
        handler: impl FnMut(Event, &mut Data, &LoopHandlerContext<Event>) -> Result<(), Event> + 'static,
    ) -> Self {
        Self {
            inner: Box::new(LoopEventHandlerImplByFunction {
                initial_event: None,
                handler: Box::new(handler),
                context: None,
            }),
        }
    }

    /// Like new(), but the handler function is only given an event and data,
    /// without the context, and also without the ability to reject the event.
    pub fn new_simple(mut handler: impl FnMut(Event, &mut Data) + 'static) -> Self {
        Self::new(move |event, data, _| {
            handler(event, data);
            Ok(())
        })
    }

    /// Like new(), but additionally sends an initial event with an initial
    /// delay. See periodic_interval() for why this is useful.
    pub fn new_with_initial_event(
        initial_event: Event,
        initial_delay: time::Duration,
        handler: impl FnMut(Event, &mut Data, &LoopHandlerContext<Event>) -> Result<(), Event> + 'static,
    ) -> Self {
        Self {
            inner: Box::new(LoopEventHandlerImplByFunction {
                initial_event: Some((initial_event, initial_delay)),
                handler: Box::new(handler),
                context: None,
            }),
        }
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
        self,
    ) -> LoopEventHandler<OuterData, OuterEvent> {
        LoopEventHandler { inner: Box::new(WideningEventHandler(self)) }
    }

    /// Adapts this handler to a handler whose data is a vector of our data,
    /// and whose event is a is the tuple (index, our event), for a specific
    /// index.
    pub fn for_index(self, index: usize) -> LoopEventHandler<Vec<Data>, (usize, Event)> {
        LoopEventHandler { inner: Box::new(IndexedLoopEventHandler { inner: self, index }) }
    }

    pub(crate) fn init(&mut self, context: LoopHandlerContext<Event>) {
        self.inner.init(context)
    }

    pub(crate) fn handle(&mut self, event: Event, data: &mut Data) -> Result<(), Event> {
        self.inner.handle(event, data)
    }
}

/// Internal implementation of LoopEventHandler.
pub(crate) trait LoopEventHandlerImpl<Data, Event> {
    /// init is called when the test loop runs for the first time.
    fn init(&mut self, context: LoopHandlerContext<Event>);
    /// handle is called when we have a pending event from the test loop.
    fn handle(&mut self, event: Event, data: &mut Data) -> Result<(), Event>;
}

/// Implementation of LoopEventHandlerImpl by a closure. We cache the context
/// upon receiving the init() call, so that we can pass a reference to the
/// closure every time we receive the handle() call.
struct LoopEventHandlerImplByFunction<Data, Event> {
    initial_event: Option<(Event, time::Duration)>,
    handler: Box<dyn FnMut(Event, &mut Data, &LoopHandlerContext<Event>) -> Result<(), Event>>,
    context: Option<LoopHandlerContext<Event>>,
}

impl<Data, Event> LoopEventHandlerImpl<Data, Event>
    for LoopEventHandlerImplByFunction<Data, Event>
{
    fn init(&mut self, context: LoopHandlerContext<Event>) {
        if let Some((event, delay)) = self.initial_event.take() {
            context.sender.send_with_delay(event, delay);
        }
        self.context = Some(context);
    }

    fn handle(&mut self, event: Event, data: &mut Data) -> Result<(), Event> {
        let context = self.context.as_ref().unwrap();
        (self.handler)(event, data, context)
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

/// Implements .widen() for an event handler.
struct WideningEventHandler<Data: 'static, Event: 'static>(LoopEventHandler<Data, Event>);

impl<
        Data,
        Event,
        OuterData: AsMut<Data>,
        OuterEvent: TryIntoOrSelf<Event> + From<Event> + 'static,
    > LoopEventHandlerImpl<OuterData, OuterEvent> for WideningEventHandler<Data, Event>
{
    fn init(&mut self, context: LoopHandlerContext<OuterEvent>) {
        self.0.init(LoopHandlerContext { sender: context.sender.narrow(), clock: context.clock })
    }

    fn handle(&mut self, event: OuterEvent, data: &mut OuterData) -> Result<(), OuterEvent> {
        let mut inner_data = data.as_mut();
        let inner_event = event.try_into_or_self()?;
        self.0.handle(inner_event, &mut inner_data)?;
        Ok(())
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

/// Periodically sends to the event loop the given event by the given interval.
/// Each time this event is handled, the given function is called.
/// The first invocation is triggered after the interval, not immediately.
pub fn interval<Data, Event: Clone + PartialEq>(
    interval: time::Duration,
    event: Event,
    func: impl Fn(&mut Data) + 'static,
) -> LoopEventHandler<Data, Event> {
    LoopEventHandler::new_with_initial_event(
        event.clone(),
        interval,
        move |actual_event, data, context| {
            if actual_event == event {
                func(data);
                context.sender.send_with_delay(actual_event, interval);
                Ok(())
            } else {
                Err(actual_event)
            }
        },
    )
}
