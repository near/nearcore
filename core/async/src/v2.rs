use crate::test_loop::delay_sender::DelaySender;
use crate::test_loop::event_handler::LoopEventHandler;
use crate::time::{Clock, Duration, FakeClock};
use near_o11y::testonly::init_test_logger;
use serde::Serialize;
use std::any::Any;
use std::collections::{BinaryHeap, HashMap};
use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use time::ext::InstantExt;

type ErasedEventHandler =
    Box<dyn FnMut(usize, Box<dyn Any>, &mut TestLoopData) -> Result<(), Box<dyn Any>>>;

pub struct TestLoopData {
    data: Vec<Box<dyn Any>>,
}

struct EventInFlight {
    stream: usize,
    event: Box<dyn Any + Send>,
    delay: Duration,
}

struct InFlightEvents {
    events: Vec<EventInFlight>,
    /// The TestLoop thread ID. This and the following field are used to detect unintended
    /// parallel processing.
    event_loop_thread_id: std::thread::ThreadId,
    /// Whether we're currently handling an event.
    is_handling_event: bool,
}

impl InFlightEvents {
    fn new() -> Self {
        Self {
            events: Vec::new(),
            event_loop_thread_id: std::thread::current().id(),
            is_handling_event: false,
        }
    }

    fn queue_event(&mut self, event_stream: usize, event: Box<dyn Any + Send>, delay: Duration) {
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
        self.events.push(EventInFlight { stream: event_stream, event, delay });
    }
}

struct EventInHeap {
    stream: usize,
    event: Box<dyn Any>,
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

struct StreamInfo {
    debug: Box<dyn Fn(&dyn Any) -> String>,
}

pub struct TestLoop {
    data: TestLoopData,
    streams: Vec<StreamInfo>,
    handlers: Vec<ErasedEventHandler>,

    /// The events that will enter the events heap upon the next iteration.
    pending_events: Arc<Mutex<InFlightEvents>>,

    clock: FakeClock,
    shutting_down: Arc<AtomicBool>,
    /// The events that are yet to be handled. They are kept in a heap so that
    /// events that shall execute earlier (by our own virtual clock) are popped
    /// first.
    events: BinaryHeap<EventInHeap>,
    /// The next index to assign to an event we receive.
    next_event_id: usize,
    /// The current virtual time.
    current_time: Duration,
}

pub struct TestLoopDataHandle<Data: 'static> {
    id: usize,
    _phantom: std::marker::PhantomData<fn(Data)>,
}

impl<Data: 'static> Clone for TestLoopDataHandle<Data> {
    fn clone(&self) -> Self {
        Self { id: self.id, _phantom: std::marker::PhantomData }
    }
}

impl<Data: 'static> Copy for TestLoopDataHandle<Data> {}

impl<Data: 'static> TestLoopDataHandle<Data> {
    fn new(id: usize) -> Self {
        Self { id, _phantom: std::marker::PhantomData }
    }

    pub fn get<'a>(&self, data: &'a TestLoopData) -> &'a Data {
        data.data[self.id].downcast_ref().unwrap()
    }

    pub fn get_mut<'a>(&self, data: &'a mut TestLoopData) -> &'a mut Data {
        data.data[self.id].downcast_mut().unwrap()
    }
}

pub struct TestLoopEventHandle<Event: 'static> {
    stream: usize,
    events: Arc<Mutex<InFlightEvents>>,
    _phantom: std::marker::PhantomData<fn(Event)>,
}

impl<Event: 'static> Clone for TestLoopEventHandle<Event> {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream,
            events: self.events.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl TestLoopData {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn add_data<Data: 'static>(&mut self, data: Data) -> TestLoopDataHandle<Data> {
        let id = self.data.len();
        self.data.push(Box::new(data));
        TestLoopDataHandle::new(id)
    }
}

impl<Event: Send + 'static> TestLoopEventHandle<Event> {
    fn new(stream: usize, events: Arc<Mutex<InFlightEvents>>) -> Self {
        Self { stream, events, _phantom: std::marker::PhantomData }
    }

    pub fn sender(&self) -> DelaySender<Event> {
        let id = self.stream;
        let events = self.events.clone();
        DelaySender::new(move |event, delay| {
            events.lock().unwrap().queue_event(id, Box::new(event), delay);
        })
    }

    pub fn add_handler0(
        &self,
        testloop: &mut TestLoop,
        mut handler: impl FnMut(Event) -> Result<(), Event> + 'static,
    ) {
        let stream = self.stream;
        testloop.handlers.push(Box::new(move |event_stream, event, _data| {
            if event_stream == stream {
                let event = *event.downcast::<Event>().unwrap();
                handler(event).map_err(|event| Box::new(event) as Box<dyn Any>)
            } else {
                Err(event)
            }
        }));
    }

    pub fn add_handler0_legacy(
        &self,
        testloop: &mut TestLoop,
        mut handler: LoopEventHandler<(), Event>,
    ) {
        let stream = self.stream;
        testloop.handlers.push(Box::new(move |event_stream, event, _data| {
            if event_stream == stream {
                let event = *event.downcast::<Event>().unwrap();
                handler.handle(event, &mut ()).map_err(|event| Box::new(event) as Box<dyn Any>)
            } else {
                Err(event)
            }
        }));
    }

    pub fn add_handler1<Data: 'static>(
        &self,
        testloop: &mut TestLoop,
        data_handle: TestLoopDataHandle<Data>,
        mut handler: impl FnMut(Event, &mut Data) -> Result<(), Event> + 'static,
    ) {
        let stream = self.stream;
        testloop.handlers.push(Box::new(move |event_stream, event, data| {
            if event_stream == stream {
                let event = *event.downcast::<Event>().unwrap();
                let data = data_handle.get_mut(data);
                handler(event, data).map_err(|event| Box::new(event) as Box<dyn Any>)
            } else {
                Err(event)
            }
        }));
    }

    pub fn add_handler1_legacy<Data: 'static>(
        &self,
        testloop: &mut TestLoop,
        data_handle: TestLoopDataHandle<Data>,
        mut handler: LoopEventHandler<Data, Event>,
    ) {
        let stream = self.stream;
        testloop.handlers.push(Box::new(move |event_stream, event, data| {
            if event_stream == stream {
                let event = *event.downcast::<Event>().unwrap();
                let data = data_handle.get_mut(data);
                handler.handle(event, data).map_err(|event| Box::new(event) as Box<dyn Any>)
            } else {
                Err(event)
            }
        }));
    }

    pub fn add_handler1n<Data: 'static>(
        &self,
        testloop: &mut TestLoop,
        data_handles: Vec<TestLoopDataHandle<Data>>,
        mut handler: impl FnMut(Event, &mut [&mut Data]) -> Result<(), Event> + 'static,
    ) {
        let stream = self.stream;
        let data_ids = data_handles
            .iter()
            .enumerate()
            .map(|(i, handle)| (handle.id, i))
            .collect::<HashMap<_, _>>();
        testloop.handlers.push(Box::new(move |event_stream, event, data| {
            if event_stream == stream {
                let event = *event.downcast::<Event>().unwrap();
                let mut refs = data
                    .data
                    .iter_mut()
                    .enumerate()
                    .filter_map(|(id, data)| {
                        data_ids.get(&id).map(|i| (*i, data.downcast_mut::<Data>().unwrap()))
                    })
                    .collect::<Vec<_>>();
                refs.sort_by_key(|(i, _)| *i);
                let mut refs = refs.into_iter().map(|(_, data)| data).collect::<Vec<_>>();
                handler(event, &mut refs).map_err(|event| Box::new(event) as Box<dyn Any>)
            } else {
                Err(event)
            }
        }));
    }
}

impl TestLoop {
    pub fn register_event<Event: Debug + Send + 'static>(&mut self) -> TestLoopEventHandle<Event> {
        let stream = self.streams.len();
        let stream_info = StreamInfo {
            debug: Box::new(move |event| format!("{:?}", event.downcast_ref::<Event>().unwrap())),
        };
        self.streams.push(stream_info);
        TestLoopEventHandle::new(stream, self.pending_events.clone())
    }

    pub fn add_data<Data: 'static>(&mut self, data: Data) -> TestLoopDataHandle<Data> {
        self.data.add_data(data)
    }

    pub fn data<Data: 'static>(&self, handle: TestLoopDataHandle<Data>) -> &Data {
        handle.get(&self.data)
    }

    pub fn data_mut<Data: 'static>(&mut self, handle: TestLoopDataHandle<Data>) -> &mut Data {
        handle.get_mut(&mut self.data)
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

impl TestLoop {
    pub fn new() -> Self {
        // Initialize the logger to make sure the test loop printouts are visible.
        init_test_logger();

        Self {
            data: TestLoopData::new(),
            streams: Vec::new(),
            pending_events: Arc::new(Mutex::new(InFlightEvents::new())),
            events: BinaryHeap::new(),
            next_event_id: 0,
            current_time: time::Duration::ZERO,
            clock: FakeClock::default(),
            shutting_down: Arc::new(AtomicBool::new(false)),
            handlers: Vec::new(),
        }
    }

    pub fn clock(&self) -> Clock {
        self.clock.clock()
    }

    pub fn shutting_down(&self) -> Arc<AtomicBool> {
        self.shutting_down.clone()
    }

    /// Helper to push events we have just received into the heap.
    fn queue_received_events(&mut self) {
        for event in self.pending_events.lock().unwrap().events.drain(..) {
            self.events.push(EventInHeap {
                stream: event.stream,
                due: self.current_time + event.delay,
                event: event.event,
                id: self.next_event_id,
            });
            self.next_event_id += 1;
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
    fn process_event(&mut self, mut event: EventInHeap) {
        let start_json = serde_json::to_string(&EventStartLogOutput {
            current_index: event.id,
            total_events: self.next_event_id,
            current_event: format!(
                "({}, {})",
                event.stream,
                (self.streams[event.stream].debug)(event.event.as_ref())
            ),
            current_time_ms: event.due.whole_milliseconds() as u64,
        })
        .unwrap();
        tracing::info!(target: "test_loop", "TEST_LOOP_EVENT_START {}", start_json);
        assert_eq!(self.current_time, event.due);

        for handler in &mut self.handlers {
            if let Err(e) = handler(event.stream, event.event, &mut self.data) {
                event.event = e;
            } else {
                // Push any new events into the queue. Do this before emitting the end log line,
                // so that it contains the correct new total number of events.
                self.queue_received_events();
                let end_json =
                    serde_json::to_string(&EventEndLogOutput { total_events: self.next_event_id })
                        .unwrap();
                tracing::info!(target: "test_loop", "TEST_LOOP_EVENT_END {}", end_json);
                return;
            }
        }
        panic!("Unhandled event: {:?}", event.event);
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
        self.shutting_down.store(true, std::sync::atomic::Ordering::Relaxed);
        self.run_for(maximum_duration);
        // Implicitly dropped here, which asserts that no more events are remaining.
    }

    pub fn run_instant(&mut self) {
        self.run_for(Duration::ZERO);
    }
}

impl Drop for TestLoop {
    fn drop(&mut self) {
        self.queue_received_events();
        if let Some(event) = self.events.pop() {
            panic!(
                "Event scheduled at {} is not handled at the end of the test: {}.
                 Consider calling `test.shutdown_and_drain_remaining_events(...)`.",
                event.due,
                (self.streams[event.stream].debug)(event.event.as_ref())
            );
        }
    }
}

enum AdvanceDecision {
    AdvanceToNextEvent,
    AdvanceToAndStop(Duration),
    Stop,
}
