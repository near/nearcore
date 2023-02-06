//! This is a framework to test async code in a way that is versatile, deterministic,
//! easy-to-setup, and easy-to-debug.
//!
//! The primary concept here is an event loop that the test framework controls. The
//! event loop acts as a central hub for all messages, including Actix messages,
//! network messages, timers, etc. Business logic is only executed as a response to
//! such events.
//!
//! This brings several major benefits:
//!  - Ease of setup:
//!     - There is no need to set up mock objects that implement some
//!       message sender interface, instead, the test loop provides a sender object that
//!       can be used to send messages to the event loop. For example, suppose we were
//!       to make a Client whose constructor requires a network adapter; instead of having
//!       to make a mock for the network adapter, we can simply pass in `loop.sender()`.
//!     - Compared to writing synchronous tests, there is no need to manually deliver
//!       network messages or handle actix messages at certain points of the test. Instead,
//!       the event loop will invoke the appropriate event handlers whenever there is any
//!       event remaining in the event loop. This ensures that no messages are ever missed.
//!     - Test setup code can be modular and reusable, because the test specification
//!       consists entirely of registering the desired event handlers. Rather than passing
//!       a giant callback into a giant setup(...) function to customize one part of a huge
//!       integration test, we can flexibly compose specific event handlers. For example,
//!       we may add an event handler to route all ShardsManager-related network messages
//!       reliably, and at the same time another event handler to drop 50% of Block network
//!       messages. Also, we can use an event handler as long as it is relevant for a test
//!       (i.e. a ForwardShardsManagerRequest event handler can be used as long as the test
//!       involves ShardsManagers), regardless of the exact architecture of the test.
//!       See `LoopEventHandler` for more details.
//!
//!  - Debuggability:
//!     - Because ALL execution is in response of events, the whole test can be cleanly
//!       segmented into the response to each event. The framework automatically outputs
//!       a log message at the beginning of each event execution, so that the log output
//!       can be loaded into a visualizer to show the exact sequence of events, their
//!       relationship, the exact contents of the event messages, and the log output
//!       during the handling of each event. This is especially useful when debugging
//!       multi-instance tests.
//!       
//!  - Determinism:
//!     - Many tests, especially those that involve multiple instances, are most easily
//!       written by spawning actual actors and threads. This however makes the tests
//!       inherently asynchronous and may be more flaky.
//!     - The test loop framework also provides a synchronous and deterministic way to
//!       invoke timers without waiting for the actual duration. This makes tests run
//!       much faster than asynchronous tests.
//!
//!  - Versatilty:
//!     - A test can be constructed with any combination of components. The framework does
//!       not dictate what components should exist, or how many instances there should be.
//!       This allows for both small and focused tests, and large multi-instance tests.
//!     - Timed tests can be written to check the theoretical performance of certain tasks,
//!       such as distributing chunks to other nodes within X milliseconds provided that
//!       network messages have a 10ms delay.
//!     - The framework does not require major migrations to existing code, e.g. it is
//!       compatible with the Actix framework (and possibly futures in the future).
//!
//! A note on the order of execution of the events: all events that are due at the same
//! timestamp are executed in FIFO order. For example, if the events are emitted in the
//! following order: (A due 100ms), (B due 0ms), (C due 200ms), (D due 0ms), (E due 100ms)
//! then the actual order of execution is B, D, A, E, C.
use std::{
    collections::BinaryHeap,
    fmt::Debug,
    sync::{self, Arc},
    time::Duration,
};

use near_o11y::{testonly::init_test_logger, tracing::log::info};
use serde::Serialize;

use crate::messaging;

/// Main struct for the Test Loop framework.
/// The `Data` type should contain all the business logic state that is relevant
/// to the test. The `Event` type should contain all the possible events that
/// are sent to the event loop.
///
/// The convention is that, for single-instance tests,
///  - `Data` should be a struct with a derive_more::AsMut and derive_more::AsRef
///    (so that `Data` implements AsMut<Field> and AsRef<Field> for each of its
///    fields.)
///  - `Event` should be an enum with a derive(EnumTryInto, EnumFrom), so that it
///    implements TryInto<Variant> and From<Variant> for each of its variants.
/// and that for multi-instance tests, `Data` is `Vec<SingleData>` and `Event` is
/// `(usize, SingleEvent)`.
pub struct TestLoop<Data, Event: Debug + Send + 'static> {
    pub data: Data,

    /// The sender is used to send events to the event loop.
    sender: DelaySender<Event>,

    /// The events that are yet to be handled. They are kept in a heap so that
    /// events that shall execute earlier (by our own virtual clock) are popped
    /// first.
    events: BinaryHeap<EventInHeap<Event>>,
    /// The events that will enter the events heap upon the next iteration.
    /// This is the receiving end of all the senders that we give out.
    pending_events: sync::mpsc::Receiver<EventInFlight<Event>>,
    /// The next ID to assign to an event we receive.
    next_event_index: usize,
    /// The current virtual time.
    current_time: Duration,

    /// Handlers are initialized only once, upon the first call to run().
    handlers_initialized: bool,
    /// All the event handlers that are registered. We invoke them one by one
    /// for each event, until one of them handles the event (or panic if no one
    /// handles it).
    handlers: Vec<Box<dyn LoopEventHandler<Data, Event>>>,
}

/// An event waiting to be executed, ordered by the due time and then by ID.
struct EventInHeap<Event> {
    event: Event,
    due: Duration,
    id: usize,
}

impl<Event> PartialEq for EventInHeap<Event> {
    fn eq(&self, other: &Self) -> bool {
        self.due == other.due && self.id == other.id
    }
}

impl<Event> Eq for EventInHeap<Event> {}

impl<Event> PartialOrd for EventInHeap<Event> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<Event> Ord for EventInHeap<Event> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.due, self.id).cmp(&(other.due, other.id)).reverse()
    }
}

/// An event that is in-flight. The delay here is relative to the virtual time
/// when the handler that emitted this event is invoked (e.g. a network routing
/// handler may respond to an outbound message and emit an inbound message with
/// a 10ms delay).
struct EventInFlight<Event> {
    event: Event,
    delay: Duration,
}

/// Builder that should be used to construct a `TestLoop`. The reason why the
/// builder exists is that usually the `Data` type can only be constructed using
/// the event sender provided by the test loop, so this way we can avoid a
/// construction dependency cycle.
pub struct TestLoopBuilder<Event: Debug + Send + 'static> {
    pending_events: sync::mpsc::Receiver<EventInFlight<Event>>,
    pending_events_sender: DelaySender<Event>,
}

impl<Event: Debug + Send + 'static> TestLoopBuilder<Event> {
    pub fn new() -> Self {
        // Initialize the logger to make sure the test loop printouts are visible.
        init_test_logger();
        let (pending_events_sender, pending_events) = sync::mpsc::sync_channel(65536);
        Self {
            pending_events,
            pending_events_sender: DelaySender {
                im: Arc::new(LoopSender { event_sender: pending_events_sender }),
            },
        }
    }

    /// Returns a sender that can be used anywhere to send events to the loop.
    pub fn sender(&self) -> DelaySender<Event> {
        self.pending_events_sender.clone()
    }

    pub fn build<Data>(self, data: Data) -> TestLoop<Data, Event> {
        TestLoop::new(self.pending_events, self.pending_events_sender, data)
    }
}

/// An event handler registered on a test loop. Each event handler usually
/// handles only some events, so we will usually have multiple event handlers
/// registered to cover all event types.
pub trait LoopEventHandler<Data, Event> {
    /// Called once, when the loop runs for the first time.
    fn init(&mut self, _sender: DelaySender<Event>) {}

    /// Handles an event. If this handler indeed handles the event, it should
    /// return None after handling it. Otherwise, it should return Some with
    /// the same event that was passed in, so that it can be given to the next
    /// event handler.
    fn handle(&mut self, event: Event, data: &mut Data) -> Option<Event>;
}

/// The log output line that can be used to visualize the execution of a test.
/// It is only used to serialize into JSON. This is enough data to reconstruct
/// the event dependency graph, and to segment log messages.
#[derive(Serialize)]
struct EventStartLogOutput {
    /// Index of the current event we're about to handle.
    current_index: usize,
    /// The total number of events we have seen of so far (i.e.
    /// [previous total_events, total_events) are the events emitted by the
    /// previous event's handler).
    total_events: usize,
    /// The Debug representation of the event payload.
    current_event: String,
    /// The current virtual time.
    current_time_ms: u64,
}

impl<Data, Event: Debug + Send + 'static> TestLoop<Data, Event> {
    fn new(
        pending_events: sync::mpsc::Receiver<EventInFlight<Event>>,
        sender: DelaySender<Event>,
        data: Data,
    ) -> Self {
        Self {
            data,
            sender,
            events: BinaryHeap::new(),
            pending_events,
            next_event_index: 0,
            current_time: Duration::ZERO,
            handlers_initialized: false,
            handlers: Vec::new(),
        }
    }

    /// Registers a new event handler to the test loop.
    pub fn register_handler<T: LoopEventHandler<Data, Event> + 'static>(&mut self, handler: T) {
        assert!(!self.handlers_initialized, "Cannot register more handlers after run() is called");
        self.handlers.push(Box::new(handler));
    }

    fn maybe_initialize_handlers(&mut self) {
        if self.handlers_initialized {
            return;
        }
        for handler in &mut self.handlers {
            handler.init(self.sender.clone());
        }
    }

    /// Runs the test loop for the given duration. This function may be called
    /// multiple times, but further test handlers may not be registered after
    /// the first call.
    pub fn run(&mut self, duration: Duration) {
        self.maybe_initialize_handlers();
        let deadline = self.current_time + duration;
        'outer: loop {
            // Push events we have just received into the heap.
            while let Ok(event) = self.pending_events.try_recv() {
                self.events.push(EventInHeap {
                    due: self.current_time + event.delay,
                    event: event.event,
                    id: self.next_event_index,
                });
                self.next_event_index += 1;
            }
            // Don't execute any more events if we have reached the deadline.
            match self.events.peek() {
                Some(event) => {
                    if event.due >= deadline {
                        break;
                    }
                }
                None => break,
            }
            // Find the next event, log a line about it, and execute.
            let event = self.events.pop().unwrap();
            let json_printout = serde_json::to_string(&EventStartLogOutput {
                current_index: event.id,
                total_events: self.next_event_index,
                current_event: format!("{:?}", event.event),
                current_time_ms: event.due.as_millis() as u64,
            })
            .unwrap();
            info!(target: "test_loop", "TEST_LOOP_EVENT_START {}", json_printout);
            self.current_time = event.due;

            let mut current_event = event.event;
            for handler in &mut self.handlers {
                if let Some(event) = handler.handle(current_event, &mut self.data) {
                    current_event = event;
                } else {
                    continue 'outer;
                }
            }
            panic!("Unhandled event: {:?}", current_event);
        }
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
/// the Data contains a Vec<CapturedEvent>.
///
/// This is used on output events so that after the test loop finishes running
/// we can assert on those events.
pub struct CaptureEvents<CapturedEvent> {
    // This is just to suppress a compiler error that CapturedEvent is not used
    // in the struct's fields - and is what the compiler suggests to do. The
    // type parameter is only used as an inference hint for which impl of
    // LoopEventHandler we will convert to.
    _marker: std::marker::PhantomData<CapturedEvent>,
}

impl<CapturedEvent> CaptureEvents<CapturedEvent> {
    pub fn new() -> Self {
        Self { _marker: std::marker::PhantomData }
    }
}

impl<CapturedEvent, Data: AsMut<Vec<CapturedEvent>>, Event: TryIntoOrSelf<CapturedEvent>>
    LoopEventHandler<Data, Event> for CaptureEvents<CapturedEvent>
{
    fn handle(&mut self, event: Event, data: &mut Data) -> Option<Event> {
        match event.try_into_or_self() {
            Ok(event) => {
                data.as_mut().push(event);
                None
            }
            Err(event) => Some(event),
        }
    }
}

/// Interface to send an event with a delay. It implements Sender for any
/// message that can be converted into this event type.
pub struct DelaySender<Event> {
    // We use an impl object here because it makes the interop with other
    // traits much easier.
    im: Arc<dyn DelaySenderImpl<Event>>,
}

impl<Event> DelaySender<Event> {
    pub fn send_with_delay(&self, event: Event, delay: Duration) {
        self.im.send_with_delay(event, delay);
    }
}

impl<Event> Clone for DelaySender<Event> {
    fn clone(&self) -> Self {
        Self { im: self.im.clone() }
    }
}

/// Allows DelaySender to be used in contexts where we don't need a delay,
/// such as being given to something that expects an actix recipient.
impl<Message, Event: From<Message> + 'static> messaging::Sender<Message> for DelaySender<Event> {
    fn send(&self, message: Message) {
        self.send_with_delay(message.into(), Duration::ZERO);
    }
}

trait DelaySenderImpl<Event>: Send + Sync {
    fn send_with_delay(&self, event: Event, delay: Duration);
}

/// Implementation of DelaySender that sends events to the test loop.
pub struct LoopSender<Event: Send + 'static> {
    event_sender: sync::mpsc::SyncSender<EventInFlight<Event>>,
}

impl<Event: Send + 'static> DelaySenderImpl<Event> for LoopSender<Event> {
    fn send_with_delay(&self, event: Event, delay: Duration) {
        self.event_sender.send(EventInFlight { event, delay }).unwrap();
    }
}
