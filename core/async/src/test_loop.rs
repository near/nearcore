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
use crate::time::Duration;
use near_o11y::{testonly::init_test_logger, tracing::info};
use serde::Serialize;
use std::sync::Mutex;
use std::{collections::BinaryHeap, fmt::Debug, sync::Arc};

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
    pending_events: Arc<Mutex<InFlightEvents<Event>>>,
    /// The next ID to assign to an event we receive.
    next_event_index: usize,
    /// The current virtual time.
    current_time: Duration,
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
    delay: Duration,
}

struct InFlightEvents<Event> {
    events: Vec<EventInFlight<Event>>,
    /// The TestLoop thread ID. This and the following field are used to detect unintended
    /// parallel processing.
    event_loop_thread_id: std::thread::ThreadId,
    /// Whether we're currently handling an event.
    is_handling_event: bool,
}

impl<Event: Debug> InFlightEvents<Event> {
    fn add(&mut self, event: Event, delay: Duration) {
        if !self.is_handling_event && std::thread::current().id() != self.event_loop_thread_id {
            // Another thread shall not be sending an event while we're not handling an event.
            // If that happens, it means we have a rogue thread spawned somewhere that has not been
            // converted to TestLoop. TestLoop tests should be single-threaded (or at least, look
            // as if it were single-threaded). So if we catch this, panic.
            panic!(
                "Event was sent from the wrong thread. TestLoop tests should be single-threaded. \
                    Check if there's any code that spawns computation on another thread such as \
                    rayon::spawn, and convert it to AsyncComputationSpawner or FutureSpawner. \
                    Event: {:?}",
                event
            );
        }
        self.events.push(EventInFlight { event, delay });
    }
}

/// Builder that should be used to construct a `TestLoop`. The reason why the
/// builder exists is that usually the `Data` type can only be constructed using
/// the event sender provided by the test loop, so this way we can avoid a
/// construction dependency cycle.
pub struct TestLoopBuilder<Event: Debug + Send + 'static> {
    clock: time::FakeClock,
    pending_events: Arc<Mutex<InFlightEvents<Event>>>,
    pending_events_sender: DelaySender<Event>,
}

impl<Event: Debug + Send + 'static> TestLoopBuilder<Event> {
    pub fn new() -> Self {
        // Initialize the logger to make sure the test loop printouts are visible.
        init_test_logger();
        let pending_events = Arc::new(Mutex::new(InFlightEvents {
            events: Vec::new(),
            event_loop_thread_id: std::thread::current().id(),
            is_handling_event: false,
        }));
        Self {
            clock: time::FakeClock::default(),
            pending_events: pending_events.clone(),
            pending_events_sender: DelaySender::new(move |event, delay| {
                pending_events.lock().unwrap().add(event, delay);
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
    /// See `EventEndLogOutput::total_events`.
    total_events: usize,
    /// The Debug representation of the event payload.
    current_event: String,
    /// The current virtual time.
    current_time_ms: u64,
}

#[derive(Serialize)]
struct EventEndLogOutput {
    /// The total number of events we have seen so far. This is combined with
    /// `EventStartLogOutput::total_events` to determine which new events are
    /// emitted by the current event's handler.
    total_events: usize,
}

impl<Data, Event: Debug + Send + 'static> TestLoop<Data, Event> {
    fn new(
        pending_events: Arc<Mutex<InFlightEvents<Event>>>,
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
        for event in self.pending_events.lock().unwrap().events.drain(..) {
            info!("Queuing new event at index {}: {:?}", self.next_event_index, event.event);
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
        // Push events we have received outside the test or during handler init into the heap.
        self.queue_received_events();
        let deadline = self.current_time + duration;
        loop {
            // Don't execute any more events after the deadline.
            match self.events.peek() {
                Some(event) => {
                    if event.due > deadline {
                        break;
                    }
                }
                None => break,
            }
            // Process the event.
            let event = self.events.pop().unwrap();
            self.process_event(event);
        }
        self.current_time = deadline;
    }

    /// Processes the given event, by logging a line first and then finding a handler to run it.
    fn process_event(&mut self, mut event: EventInHeap<Event>) {
        let start_json = serde_json::to_string(&EventStartLogOutput {
            current_index: event.id,
            total_events: self.next_event_index,
            current_event: format!("{:?}", event.event),
            current_time_ms: event.due.whole_milliseconds() as u64,
        })
        .unwrap();
        info!(target: "test_loop", "TEST_LOOP_EVENT_START {}", start_json);
        self.clock.advance(event.due - self.current_time);
        self.current_time = event.due;

        for handler in &mut self.handlers {
            if let Err(e) = handler.handle(event.event, &mut self.data) {
                event.event = e;
            } else {
                // Push any new events into the queue. Do this before emitting the end log line,
                // so that it contains the correct new total number of events.
                self.queue_received_events();
                let end_json = serde_json::to_string(&EventEndLogOutput {
                    total_events: self.next_event_index,
                })
                .unwrap();
                info!(target: "test_loop", "TEST_LOOP_EVENT_END {}", end_json);
                return;
            }
        }
        panic!("Unhandled event: {:?}", event.event);
    }

    /// Run until the given condition is true, asserting that it happens before the maximum duration
    /// is reached.
    ///
    /// To maximize logical consistency, the condition is only checked before the clock would
    /// advance. If it returns true, execution stops before advancing the clock.
    pub fn run_until(&mut self, condition: impl Fn(&mut Data) -> bool, maximum_duration: Duration) {
        self.maybe_initialize_handlers();
        // Push events we have received outside the test or during handler init into the heap.
        self.queue_received_events();
        let deadline = self.current_time + maximum_duration;
        loop {
            // Don't execute any more events after the deadline.
            match self.events.peek() {
                Some(event) => {
                    if event.due > deadline {
                        panic!("run_until did not fulfill the condition within the given deadline");
                    }
                    if event.due > self.current_time {
                        if condition(&mut self.data) {
                            return;
                        }
                    }
                }
                None => break,
            }
            // Process the event.
            let event = self.events.pop().unwrap();
            self.process_event(event);
        }
    }

    /// Used to finish off remaining events that are still in the loop. This can be necessary if the
    /// destructor of some components wait for certain condition to become true. Otherwise, the
    /// destructors may end up waiting forever. This also helps avoid a panic when destructing
    /// TestLoop itself, as it asserts that all important events have been handled.
    ///
    /// Note that events that are droppable are dropped and not handled. It would not be consistent
    /// to continue using the TestLoop, and therefore it is consumed by this function.
    pub fn finish_remaining_events(mut self, maximum_duration: Duration) {
        self.maybe_initialize_handlers();
        // Push events we have received outside the test or during handler init into the heap.
        self.queue_received_events();
        let max_time = self.current_time + maximum_duration;
        'outer: loop {
            // Don't execute any more events after the deadline.
            match self.events.peek() {
                Some(event) => {
                    if event.due > max_time {
                        panic!(
                            "finish_remaining_events could not finish all events; \
                            event still remaining: {:?}",
                            event.event
                        );
                    }
                }
                None => break,
            }
            // Only execute the event if we can't drop it.
            let mut event = self.events.pop().unwrap();
            for handler in &self.handlers {
                if let Err(e) = handler.try_drop(event.event) {
                    event.event = e;
                } else {
                    continue 'outer;
                }
            }
            // Process the event.
            self.process_event(event);
        }
    }

    pub fn run_instant(&mut self) {
        self.run_for(Duration::ZERO);
    }

    pub fn future_spawner(&self) -> TestLoopFutureSpawner
    where
        Event: From<Arc<TestLoopTask>>,
    {
        self.sender().narrow()
    }
}

impl<Data: 'static, Event: Debug + Send + 'static> Drop for TestLoop<Data, Event> {
    fn drop(&mut self) {
        self.queue_received_events();
        'outer: for event in self.events.drain() {
            let mut to_handle = event.event;
            for handler in &mut self.handlers {
                if let Err(e) = handler.try_drop(to_handle) {
                    to_handle = e;
                } else {
                    continue 'outer;
                }
            }
            panic!(
                "Important event scheduled at {} is not handled at the end of the test: {:?}.
                 Consider calling `test.run()` again, or with a longer duration.",
                event.due, to_handle
            );
        }
    }
}
