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
pub mod adhoc;
pub mod delay_sender;
pub mod event_handler;
pub mod futures;
pub mod multi_instance;

use self::{
    delay_sender::DelaySender,
    event_handler::LoopEventHandler,
    futures::{TestLoopFutureSpawner, TestLoopTask},
};
use crate::test_loop::event_handler::LoopHandlerContext;
use crate::time;
use near_o11y::{testonly::init_test_logger, tracing::log::info};
use serde::Serialize;
use std::{
    collections::BinaryHeap,
    fmt::Debug,
    sync::{self, Arc},
};

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
pub struct TestLoop<Data: 'static, Event: Debug + Send + 'static> {
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
    current_time: time::Duration,
    /// Fake clock that always returns the virtual time.
    clock: time::FakeClock,

    /// Handlers are initialized only once, upon the first call to run().
    handlers_initialized: bool,
    /// All the event handlers that are registered. We invoke them one by one
    /// for each event, until one of them handles the event (or panic if no one
    /// handles it).
    handlers: Vec<LoopEventHandler<Data, Event>>,
}

/// An event waiting to be executed, ordered by the due time and then by ID.
struct EventInHeap<Event> {
    event: Event,
    due: time::Duration,
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
    delay: time::Duration,
}

/// Builder that should be used to construct a `TestLoop`. The reason why the
/// builder exists is that usually the `Data` type can only be constructed using
/// the event sender provided by the test loop, so this way we can avoid a
/// construction dependency cycle.
pub struct TestLoopBuilder<Event: Debug + Send + 'static> {
    clock: time::FakeClock,
    pending_events: sync::mpsc::Receiver<EventInFlight<Event>>,
    pending_events_sender: DelaySender<Event>,
}

impl<Event: Debug + Send + 'static> TestLoopBuilder<Event> {
    pub fn new() -> Self {
        // Initialize the logger to make sure the test loop printouts are visible.
        init_test_logger();
        let (pending_events_sender, pending_events) = sync::mpsc::sync_channel(65536);
        Self {
            clock: time::FakeClock::default(),
            pending_events,
            pending_events_sender: DelaySender::new(move |event, delay| {
                pending_events_sender.send(EventInFlight { event, delay }).unwrap();
            }),
        }
    }

    /// Returns a sender that can be used anywhere to send events to the loop.
    pub fn sender(&self) -> DelaySender<Event> {
        self.pending_events_sender.clone()
    }

    /// Returns a clock that will always return the current virtual time.
    pub fn clock(&self) -> time::Clock {
        self.clock.clock()
    }

    /// Returns a FutureSpawner that can be used to spawn futures into the loop.
    pub fn future_spawner(&self) -> TestLoopFutureSpawner
    where
        Event: From<Arc<TestLoopTask>>,
    {
        self.sender().narrow()
    }

    pub fn build<Data>(self, data: Data) -> TestLoop<Data, Event> {
        TestLoop::new(self.pending_events, self.pending_events_sender, self.clock, data)
    }
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
        clock: time::FakeClock,
        data: Data,
    ) -> Self {
        Self {
            data,
            sender,
            events: BinaryHeap::new(),
            pending_events,
            next_event_index: 0,
            current_time: time::Duration::ZERO,
            clock,
            handlers_initialized: false,
            handlers: Vec::new(),
        }
    }

    pub fn sender(&self) -> DelaySender<Event> {
        self.sender.clone()
    }

    /// Registers a new event handler to the test loop.
    pub fn register_handler(&mut self, handler: LoopEventHandler<Data, Event>) {
        assert!(!self.handlers_initialized, "Cannot register more handlers after run() is called");
        self.handlers.push(handler);
    }

    fn maybe_initialize_handlers(&mut self) {
        if self.handlers_initialized {
            return;
        }
        for handler in &mut self.handlers {
            handler.init(LoopHandlerContext {
                sender: self.sender.clone(),
                clock: self.clock.clock(),
            });
        }
    }

    /// Helper to push events we have just received into the heap.
    fn queue_received_events(&mut self) {
        while let Ok(event) = self.pending_events.try_recv() {
            self.events.push(EventInHeap {
                due: self.current_time + event.delay,
                event: event.event,
                id: self.next_event_index,
            });
            self.next_event_index += 1;
        }
    }

    /// Runs the test loop for the given duration. This function may be called
    /// multiple times, but further test handlers may not be registered after
    /// the first call.
    pub fn run_for(&mut self, duration: time::Duration) {
        self.maybe_initialize_handlers();
        let deadline = self.current_time + duration;
        'outer: loop {
            // Push events we have just received into the heap.
            self.queue_received_events();
            // Don't execute any more events after the deadline.
            match self.events.peek() {
                Some(event) => {
                    if event.due > deadline {
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
                current_time_ms: event.due.whole_milliseconds() as u64,
            })
            .unwrap();
            info!(target: "test_loop", "TEST_LOOP_EVENT_START {}", json_printout);
            self.clock.advance(event.due - self.current_time);
            self.current_time = event.due;

            let mut current_event = event.event;
            for handler in &mut self.handlers {
                if let Err(event) = handler.handle(current_event, &mut self.data) {
                    current_event = event;
                } else {
                    continue 'outer;
                }
            }
            panic!("Unhandled event: {:?}", current_event);
        }
        self.current_time = deadline;
    }

    pub fn run_instant(&mut self) {
        self.run_for(time::Duration::ZERO);
    }
}

impl<Data: 'static, Event: Debug + Send + 'static> Drop for TestLoop<Data, Event> {
    fn drop(&mut self) {
        self.queue_received_events();
        'outer: for event in self.events.drain() {
            let mut current_event = event.event;
            for handler in &mut self.handlers {
                if let Err(event) = handler.try_drop(current_event) {
                    current_event = event;
                } else {
                    continue 'outer;
                }
            }
            panic!(
                "Important event scheduled at {} is not handled at the end of the test: {:?}.
                 Consider calling `test.run()` again, or with a longer duration.",
                event.due, current_event
            );
        }
    }
}
