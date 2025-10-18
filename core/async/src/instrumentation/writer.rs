use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use near_time::Clock;

use crate::instrumentation::WINDOW_SIZE_NS;
use crate::instrumentation::data::{ALL_ACTOR_INSTRUMENTATIONS, InstrumentedThread};
use crate::instrumentation::queue::InstrumentedQueue;

thread_local! {
    static THREAD_WRITER: RefCell<Option<InstrumentedThreadWriter>> = const { RefCell::new(None) };
}

/// Writer that can be used to record instrumentation events for a specific
/// thread. It is associated with a specific actor thread, and maintains a
/// mapping from message type names. The actor thread will use the mutable
/// reference to this struct to record events.
pub struct InstrumentedThreadWriter {
    shared: Arc<InstrumentedThreadWriterSharedPart>,
    current_window_start_time_ns: u64,
    type_name_registry: HashMap<String, u32>,
    target: Arc<InstrumentedThread>,
}

/// Shared part of the instrumentation that is shared between all threads of an
/// actor. Most importantly, it contains the reference to the queue, which is
/// shared between all threads. It also contains the clock and reference
/// instant to compute timestamps.
pub struct InstrumentedThreadWriterSharedPart {
    actor_name: String,
    clock: Clock,
    reference_instant: Instant,
    queue: Arc<InstrumentedQueue>,
}

impl InstrumentedThreadWriter {
    /// Start an event for a specific message type, also recording the time spent in the queue.
    pub fn start_event(&mut self, message_type: &str, dequeue_time_ns: u64) {
        let start_time_ns =
            self.shared.clock.now().duration_since(self.shared.reference_instant).as_nanos() as u64;
        let message_type_id = if let Some(id) = self.type_name_registry.get(message_type) {
            *id
        } else {
            let id = self.type_name_registry.len() as u32;
            self.type_name_registry.insert(message_type.to_string(), id);
            self.target.message_type_registry.push_type(message_type.to_string());
            id
        };
        self.advance_window_if_needed_internal(start_time_ns);
        self.target.start_event(message_type_id, start_time_ns, dequeue_time_ns);
    }

    // End an event for a specific message type.
    // Note: easier to ask for message type here instead of storing a reverse mapping,
    // as the caller knows it.
    pub fn end_event(&mut self, _message_type: &str) {
        let end_time_ns =
            self.shared.clock.now().duration_since(self.shared.reference_instant).as_nanos() as u64;
        self.advance_window_if_needed_internal(end_time_ns);
        let _total_elapsed_ns = self.target.end_event(end_time_ns);
    }

    /// Advance the current window if needed, based on the current time.
    pub fn advance_window_if_needed(&mut self) {
        let current_time_ns =
            self.shared.clock.now().duration_since(self.shared.reference_instant).as_nanos() as u64;
        self.advance_window_if_needed_internal(current_time_ns);
    }

    fn advance_window_if_needed_internal(&mut self, current_time_ns: u64) {
        while current_time_ns >= self.current_window_start_time_ns + WINDOW_SIZE_NS {
            self.current_window_start_time_ns += WINDOW_SIZE_NS;
            self.target.advance_window(self.current_window_start_time_ns);
        }
    }
}

impl Drop for InstrumentedThreadWriter {
    fn drop(&mut self) {
        ALL_ACTOR_INSTRUMENTATIONS.threads.write().remove(&(Arc::as_ptr(&self.target) as usize));
    }
}

impl InstrumentedThreadWriterSharedPart {
    pub fn new(actor_name: String, queue: Arc<InstrumentedQueue>) -> Arc<Self> {
        let clock = Clock::real();
        let reference_instant = ALL_ACTOR_INSTRUMENTATIONS.reference_instant;
        Arc::new(Self { actor_name, clock, reference_instant, queue })
    }

    pub fn new_for_test(
        actor_name: String,
        clock: Clock,
        reference_instant: Instant,
        queue: Arc<InstrumentedQueue>,
    ) -> Arc<Self> {
        Arc::new(Self { actor_name, clock, reference_instant, queue })
    }

    pub fn current_time(&self) -> u64 {
        self.clock.now().duration_since(self.reference_instant).as_nanos() as u64
    }

    pub fn queue(&self) -> &InstrumentedQueue {
        &self.queue
    }

    pub fn new_writer_with_global_registration(
        self: &Arc<Self>,
        thread_index: Option<usize>,
    ) -> InstrumentedThreadWriter {
        let thread_name = if let Some(index) = thread_index {
            format!("{}-{}", self.actor_name, index)
        } else {
            self.actor_name.clone()
        };
        let start_time = self.current_time();
        let target = Arc::new(InstrumentedThread::new(
            thread_name,
            self.actor_name.clone(),
            self.queue.clone(),
            start_time,
        ));
        let key = Arc::as_ptr(&target) as usize;
        ALL_ACTOR_INSTRUMENTATIONS.threads.write().insert(key, target.clone());
        InstrumentedThreadWriter {
            current_window_start_time_ns: start_time / WINDOW_SIZE_NS * WINDOW_SIZE_NS,
            shared: self.clone(),
            type_name_registry: HashMap::new(),
            target,
        }
    }

    pub fn new_writer_for_test(
        self: &Arc<Self>,
        target: Arc<InstrumentedThread>,
    ) -> InstrumentedThreadWriter {
        let start_time = self.current_time();
        InstrumentedThreadWriter {
            current_window_start_time_ns: start_time / WINDOW_SIZE_NS * WINDOW_SIZE_NS,
            shared: self.clone(),
            type_name_registry: HashMap::new(),
            target,
        }
    }

    pub fn with_thread_local_writer(
        self: &Arc<Self>,
        f: impl FnOnce(&mut InstrumentedThreadWriter),
    ) {
        THREAD_WRITER.with(|cell| {
            let mut borrow: RefMut<Option<InstrumentedThreadWriter>> = cell.borrow_mut();
            if let Some(writer) = borrow.as_mut() {
                f(writer)
            } else {
                *borrow = Some(self.new_writer_with_global_registration(None));
                f(borrow.as_mut().unwrap())
            }
        })
    }
}
