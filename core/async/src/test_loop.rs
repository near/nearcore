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
//!       to make a Client whose constructor requires a shards_manager adapter; instead
//!       of having to make a mock for the shards_manager adapter, we can simply register
//!       the shards_manager actor with testloop and pass in it's sender.
//!     - Compared to writing synchronous tests, there is no need to manually deliver
//!       network messages or handle actix messages at certain points of the test. Instead,
//!       the event loop will invoke the appropriate event handlers whenever there is any
//!       event remaining in the event loop. This ensures that no messages are ever missed.
//!     - Test setup code can be modular and reusable, because the test specification
//!       consists entirely of registering the data and actors. Rather than passing a giant
//!       callback into a giant setup(...) function to customize one part of a huge
//!       integration test, we can flexibly compose specific modules with event handlers.
//!       For example, we may add an event handler to route all ShardsManager-related network
//!       messages reliably, and at the same time another event handler to drop 50% of Block
//!       network messages. Also, we can use an event handler as long as it is relevant for a
//!       test (i.e. a ForwardShardsManagerRequest event handler can be used as long as the
//!       test involves ShardsManagers), regardless of the exact architecture of the test.
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
//!
//! A note on the order of execution of the events: all events that are due at the same
//! timestamp are executed in FIFO order. For example, if the events are emitted in the
//! following order: (A due 100ms), (B due 0ms), (C due 200ms), (D due 0ms), (E due 100ms)
//! then the actual order of execution is B, D, A, E, C.
pub mod data;
pub mod futures;
pub mod pending_events_sender;
pub mod sender;

use data::TestLoopData;
use futures::{TestLoopAsyncComputationSpawner, TestLoopFututeSpawner};
use near_time::{Clock, Duration, FakeClock};
use pending_events_sender::{CallbackEvent, PendingEventsSender};
use sender::TestLoopSender;
use serde::Serialize;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use time::ext::InstantExt;

use crate::messaging::{Actor, LateBoundSender};

/// Main struct for the Test Loop framework.
/// The `TestLoopData` should contain all the business logic state that is relevant
/// to the test. All possible `Event` that are sent to the event loop are callbacks.
/// See TestLoopData for mode details.
///
/// Events are sent to the testloop, with a possible delay, via the pending_events_sender.
pub struct TestLoopV2 {
    /// The data that is stored and accessed by the test loop.
    pub data: TestLoopData,
    /// The sender is used to send events to the event loop.
    pending_events_sender: PendingEventsSender,
    /// The events that are yet to be handled. They are kept in a heap so that
    /// events that shall execute earlier (by our own virtual clock) are popped
    /// first.
    events: BinaryHeap<EventInHeap>,
    /// The events that will enter the events heap upon the next iteration.
    pending_events: Arc<Mutex<InFlightEvents>>,
    /// The next ID to assign to an event we receive.
    next_event_index: usize,
    /// The current virtual time.
    current_time: Duration,
    /// Fake clock that always returns the virtual time.
    clock: near_time::FakeClock,
    /// Shutdown flag. When this flag is true, delayed action runners will no
    /// longer post any new events to the event loop.
    shutting_down: Arc<AtomicBool>,
}

/// An event waiting to be executed, ordered by the due time and then by ID.
struct EventInHeap {
    event: CallbackEvent,
    due: Duration,
    id: usize,
}

impl PartialEq for EventInHeap {
    fn eq(&self, other: &Self) -> bool {
        self.due == other.due && self.id == other.id
    }
}

impl Eq for EventInHeap {}

impl PartialOrd for EventInHeap {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EventInHeap {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.due, self.id).cmp(&(other.due, other.id)).reverse()
    }
}

struct InFlightEvents {
    events: Vec<CallbackEvent>,
    /// The TestLoop thread ID. This and the following field are used to detect unintended
    /// parallel processing.
    event_loop_thread_id: std::thread::ThreadId,
    /// Whether we're currently handling an event.
    is_handling_event: bool,
}

impl InFlightEvents {
    fn add(&mut self, event: CallbackEvent) {
        if !self.is_handling_event && std::thread::current().id() != self.event_loop_thread_id {
            // Another thread shall not be sending an event while we're not handling an event.
            // If that happens, it means we have a rogue thread spawned somewhere that has not been
            // converted to TestLoop. TestLoop tests should be single-threaded (or at least, look
            // as if it were single-threaded). So if we catch this, panic.
            panic!(
                "Event was sent from the wrong thread. TestLoop tests should be single-threaded. \
                    Check if there's any code that spawns computation on another thread such as \
                    rayon::spawn, and convert it to AsyncComputationSpawner or FutureSpawner. \
                    Event: {}",
                event.description
            );
        }
        self.events.push(event);
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

impl TestLoopV2 {
    pub fn new() -> Self {
        let pending_events = Arc::new(Mutex::new(InFlightEvents {
            events: Vec::new(),
            event_loop_thread_id: std::thread::current().id(),
            is_handling_event: false,
        }));
        let pending_events_clone = pending_events.clone();
        let pending_events_sender = PendingEventsSender::new(move |callback_event| {
            let mut pending_events = pending_events_clone.lock().unwrap();
            pending_events.add(callback_event);
        });
        let shutting_down = Arc::new(AtomicBool::new(false));
        Self {
            data: TestLoopData::new(pending_events_sender.clone(), shutting_down.clone()),
            events: BinaryHeap::new(),
            pending_events,
            pending_events_sender,
            next_event_index: 0,
            current_time: Duration::ZERO,
            clock: FakeClock::default(),
            shutting_down,
        }
    }

    /// Returns a FutureSpawner that can be used to spawn futures into the loop.
    pub fn future_spawner(&self) -> TestLoopFututeSpawner {
        self.pending_events_sender.clone()
    }

    /// Returns an AsyncComputationSpawner that can be used to spawn async computation into the
    /// loop. The `artificial_delay` allows the test to determine an artificial delay that the
    /// computation should take, based on the name of the computation.
    pub fn async_computation_spawner(
        &self,
        artificial_delay: impl Fn(&str) -> Duration + Send + Sync + 'static,
    ) -> TestLoopAsyncComputationSpawner {
        TestLoopAsyncComputationSpawner::new(self.pending_events_sender.clone(), artificial_delay)
    }

    /// Returns a sender that can be used anywhere to send events to the loop.
    pub fn sender(&self) -> PendingEventsSender {
        self.pending_events_sender.clone()
    }

    /// Sends any ad-hoc event to the loop.
    pub fn send_adhoc_event(
        &self,
        description: String,
        callback: impl FnOnce(&mut TestLoopData) + Send + 'static,
    ) {
        self.pending_events_sender.send(format!("Adhoc({})", description), Box::new(callback));
    }

    /// Returns a clock that will always return the current virtual time.
    pub fn clock(&self) -> Clock {
        self.clock.clock()
    }

    pub fn register_actor<A>(
        &mut self,
        actor: A,
        adapter: Option<Arc<LateBoundSender<TestLoopSender<A>>>>,
    ) -> TestLoopSender<A>
    where
        A: Actor + 'static,
    {
        self.data.register_actor_for_index(0, actor, adapter)
    }

    pub fn register_actor_for_index<A>(
        &mut self,
        index: usize,
        actor: A,
        adapter: Option<Arc<LateBoundSender<TestLoopSender<A>>>>,
    ) -> TestLoopSender<A>
    where
        A: Actor + 'static,
    {
        self.data.register_actor_for_index(index, actor, adapter)
    }

    /// Helper to push events we have just received into the heap.
    fn queue_received_events(&mut self) {
        for event in self.pending_events.lock().unwrap().events.drain(..) {
            self.events.push(EventInHeap {
                due: self.current_time + event.delay,
                id: self.next_event_index,
                event,
            });
            self.next_event_index += 1;
        }
    }

    /// Performs the logic to find the next event, advance to its time, and dequeue it.
    /// Takes a decider to determine whether to advance time, handle the next event, and/or to stop.
    fn advance_till_next_event(
        &mut self,
        decider: &impl Fn(Option<Duration>, &mut TestLoopData) -> AdvanceDecision,
    ) -> Option<EventInHeap> {
        loop {
            // New events may have been sent to the TestLoop from outside, and the previous
            // iteration of the loop may have made new futures ready, so queue up any received
            // events.
            self.queue_received_events();

            // Now there are two ways an event may be/become available. One is that the event is
            // queued into the event loop at a specific time; the other is that some future is
            // waiting on our fake clock to advance beyond a specific time. Pick the earliest.
            let next_timestamp = {
                let next_event_timestamp = self.events.peek().map(|event| event.due);
                let next_future_waiter_timestamp = self
                    .clock
                    .first_waiter()
                    .map(|time| time.signed_duration_since(self.clock.now() - self.current_time));
                next_event_timestamp
                    .map(|t1| next_future_waiter_timestamp.map(|t2| t2.min(t1)).unwrap_or(t1))
                    .or(next_future_waiter_timestamp)
            };
            // If the next event is immediately available (i.e. its time is same as current time),
            // just return that event; there's no decision to make (as we only give deciders a
            // chance to stop processing if we would advance the clock) and no need to advance time.
            if next_timestamp == Some(self.current_time) {
                let event = self.events.pop().expect("Programming error in TestLoop");
                assert_eq!(event.due, self.current_time);
                return Some(event);
            }
            // If we reach this point, it means we need to advance the clock. Let the decider choose
            // if we should do that, or if we should stop.
            let decision = decider(next_timestamp, &mut self.data);
            match decision {
                AdvanceDecision::AdvanceToNextEvent => {
                    let next_timestamp = next_timestamp.unwrap();
                    self.clock.advance(next_timestamp - self.current_time);
                    self.current_time = next_timestamp;
                    // Run the loop again, because if the reason why we advance the clock to this
                    // time is due to a possible future waiting on the clock, we may or may not get
                    // another future queued into the TestLoop, so we just check the whole thing
                    // again.
                    continue;
                }
                AdvanceDecision::AdvanceToAndStop(target) => {
                    self.clock.advance(target - self.current_time);
                    self.current_time = target;
                    return None;
                }
                AdvanceDecision::Stop => {
                    return None;
                }
            }
        }
    }

    /// Processes the given event, by logging a line first and then finding a handler to run it.
    fn process_event(&mut self, event: EventInHeap) {
        let start_json = serde_json::to_string(&EventStartLogOutput {
            current_index: event.id,
            total_events: self.next_event_index,
            current_event: event.event.description,
            current_time_ms: event.due.whole_milliseconds() as u64,
        })
        .unwrap();
        tracing::info!(target: "test_loop", "TEST_LOOP_EVENT_START {}", start_json);
        assert_eq!(self.current_time, event.due);

        let callback = event.event.callback;
        callback(&mut self.data);

        // Push any new events into the queue. Do this before emitting the end log line,
        // so that it contains the correct new total number of events.
        self.queue_received_events();
        let end_json =
            serde_json::to_string(&EventEndLogOutput { total_events: self.next_event_index })
                .unwrap();
        tracing::info!(target: "test_loop", "TEST_LOOP_EVENT_END {}", end_json);
    }

    /// Runs the test loop for the given duration. This function may be called
    /// multiple times, but further test handlers may not be registered after
    /// the first call.
    pub fn run_for(&mut self, duration: Duration) {
        let deadline = self.current_time + duration;
        while let Some(event) = self.advance_till_next_event(&|next_time, _| {
            if let Some(next_time) = next_time {
                if next_time <= deadline {
                    return AdvanceDecision::AdvanceToNextEvent;
                }
            }
            AdvanceDecision::AdvanceToAndStop(deadline)
        }) {
            self.process_event(event);
        }
    }

    /// Run until the given condition is true, asserting that it happens before the maximum duration
    /// is reached.
    ///
    /// To maximize logical consistency, the condition is only checked before the clock would
    /// advance. If it returns true, execution stops before advancing the clock.
    pub fn run_until(
        &mut self,
        condition: impl Fn(&mut TestLoopData) -> bool,
        maximum_duration: Duration,
    ) {
        let deadline = self.current_time + maximum_duration;
        let decider = |next_time, data: &mut TestLoopData| {
            if condition(data) {
                return AdvanceDecision::Stop;
            }
            if let Some(next_time) = next_time {
                if next_time <= deadline {
                    return AdvanceDecision::AdvanceToNextEvent;
                }
            }
            panic!("run_until did not fulfill the condition within the given deadline");
        };
        while let Some(event) = self.advance_till_next_event(&decider) {
            self.process_event(event);
        }
    }

    /// Used to finish off remaining events that are still in the loop. This can be necessary if the
    /// destructor of some components wait for certain condition to become true. Otherwise, the
    /// destructors may end up waiting forever. This also helps avoid a panic when destructing
    /// TestLoop itself, as it asserts that all events have been handled.
    pub fn shutdown_and_drain_remaining_events(mut self, maximum_duration: Duration) {
        self.shutting_down.store(true, Ordering::Relaxed);
        self.run_for(maximum_duration);
        // Implicitly dropped here, which asserts that no more events are remaining.
    }

    pub fn run_instant(&mut self) {
        self.run_for(Duration::ZERO);
    }
}

impl Drop for TestLoopV2 {
    fn drop(&mut self) {
        self.queue_received_events();
        if let Some(event) = self.events.pop() {
            panic!(
                "Event scheduled at {} is not handled at the end of the test: {}.
                 Consider calling `test.shutdown_and_drain_remaining_events(...)`.",
                event.due, event.event.description
            );
        }
    }
}

enum AdvanceDecision {
    AdvanceToNextEvent,
    AdvanceToAndStop(Duration),
    Stop,
}

#[cfg(test)]
mod tests {
    use crate::futures::FutureSpawnerExt;
    use crate::test_loop::TestLoopV2;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use time::Duration;

    // Tests that the TestLoop correctly handles futures that sleep on the fake clock.
    #[test]
    fn test_futures() {
        let mut test_loop = TestLoopV2::new();
        let clock = test_loop.clock();
        let start_time = clock.now();

        let finished = Arc::new(AtomicUsize::new(0));

        let clock1 = clock.clone();
        let finished1 = finished.clone();
        test_loop.future_spawner().spawn("test1", async move {
            assert_eq!(clock1.now(), start_time);
            clock1.sleep(Duration::seconds(10)).await;
            assert_eq!(clock1.now(), start_time + Duration::seconds(10));
            clock1.sleep(Duration::seconds(5)).await;
            assert_eq!(clock1.now(), start_time + Duration::seconds(15));
            finished1.fetch_add(1, Ordering::Relaxed);
        });

        test_loop.run_for(Duration::seconds(2));

        let clock2 = clock;
        let finished2 = finished.clone();
        test_loop.future_spawner().spawn("test2", async move {
            assert_eq!(clock2.now(), start_time + Duration::seconds(2));
            clock2.sleep(Duration::seconds(3)).await;
            assert_eq!(clock2.now(), start_time + Duration::seconds(5));
            clock2.sleep(Duration::seconds(20)).await;
            assert_eq!(clock2.now(), start_time + Duration::seconds(25));
            finished2.fetch_add(1, Ordering::Relaxed);
        });
        // During these 30 virtual seconds, the TestLoop should've automatically advanced the clock
        // to wake each future as they become ready to run again. The code inside the futures
        // assert that the fake clock does indeed have the expected times.
        test_loop.run_for(Duration::seconds(30));
        assert_eq!(finished.load(Ordering::Relaxed), 2);
    }
}
