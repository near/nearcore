use std::time::Duration;

use super::{delay_sender::DelaySender, multi_instance::IndexedLoopEventHandler};

/// An event handler registered on a test loop. Each event handler usually
/// handles only some events, so we will usually have multiple event handlers
/// registered to cover all event types.
pub trait LoopEventHandler<Data, Event> {
    /// Called once, when the loop runs for the first time.
    fn init(&mut self, _sender: DelaySender<Event>) {}

    /// Handles an event. If this handler indeed handles the event, it should
    /// return Ok after handling it. Otherwise, it should return Err with
    /// the same event that was passed in, so that it can be given to the next
    /// event handler.
    fn handle(&mut self, event: Event, data: &mut Data) -> Result<(), Event>;

    /// Adapts this handler to a handler whose data is a superset of our data
    /// and whose event is a superset of our event.
    ///   For data, A is a superset of B if A implements AsRef<B> and AsMut<B>.
    ///   For event, A is a superset of B if A implements From<B> and
    ///     TryIntoOrSelf<B>.
    fn widen(self) -> WideningEventHandler<Data, Event>
    where
        Self: Sized + 'static,
    {
        WideningEventHandler { inner: Box::new(self) }
    }

    /// Adapts this handler to a handler whose data is a vector of our data,
    /// and whose event is a is the tuple (index, our event), for a specific
    /// index.
    fn for_index(self, index: usize) -> IndexedLoopEventHandler<Data, Event>
    where
        Self: Sized + 'static,
    {
        IndexedLoopEventHandler::new(Box::new(self), index)
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
pub struct WideningEventHandler<Data, Event> {
    inner: Box<dyn LoopEventHandler<Data, Event>>,
}

impl<
        Data,
        Event,
        OuterData: AsMut<Data>,
        OuterEvent: TryIntoOrSelf<Event> + From<Event> + 'static,
    > LoopEventHandler<OuterData, OuterEvent> for WideningEventHandler<Data, Event>
{
    fn init(&mut self, sender: DelaySender<OuterEvent>) {
        self.inner.init(sender.narrow())
    }

    fn handle(&mut self, event: OuterEvent, data: &mut OuterData) -> Result<(), OuterEvent> {
        let mut inner_data = data.as_mut();
        let inner_event = event.try_into_or_self()?;
        self.inner.handle(inner_event, &mut inner_data)?;
        Ok(())
    }
}

/// Implements capture_events().
struct CaptureEvents;

impl<Event> LoopEventHandler<Vec<Event>, Event> for CaptureEvents {
    fn handle(&mut self, event: Event, data: &mut Vec<Event>) -> Result<(), Event> {
        data.push(event);
        Ok(())
    }
}

/// An event handler that puts the event into a vector in the Data, as long as
/// the Data contains a Vec<CapturedEvent>. (Use widen() right after).
///
/// This is used on output events so that after the test loop finishes running
/// we can assert on those events.
pub fn capture_events<Event>() -> impl LoopEventHandler<Vec<Event>, Event> {
    CaptureEvents
}

/// Implements periodic_interval().
struct PeriodicInterval<Data, Event: Clone + PartialEq> {
    interval: Duration,
    event: Event,
    delay_sender: Option<DelaySender<Event>>,
    func: Box<dyn Fn(&mut Data) + 'static>,
}

impl<Data, Event: Clone + PartialEq> LoopEventHandler<Data, Event>
    for PeriodicInterval<Data, Event>
{
    fn init(&mut self, sender: DelaySender<Event>) {
        self.delay_sender = Some(sender);
        self.delay_sender.as_ref().unwrap().send_with_delay(self.event.clone(), self.interval);
    }

    fn handle(&mut self, event: Event, data: &mut Data) -> Result<(), Event> {
        if event == self.event {
            (self.func)(data);
            self.delay_sender.as_ref().unwrap().send_with_delay(self.event.clone(), self.interval);
            Ok(())
        } else {
            Err(event)
        }
    }
}

/// Periodically sends to the event loop the given event by the given interval.
/// Each time this event is handled, the given function is called.
/// The first invocation is triggered after the interval, not immediately.
pub fn periodic_interval<Data, Event: Clone + PartialEq>(
    interval: Duration,
    event: Event,
    func: impl Fn(&mut Data) + 'static,
) -> impl LoopEventHandler<Data, Event> {
    PeriodicInterval { interval, event, delay_sender: None, func: Box::new(func) }
}
