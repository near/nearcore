use std::{collections::HashMap, sync::Arc, time::Instant};

use near_time::Clock;

use crate::instrumentation::{
    WINDOW_SIZE_NS,
    data::{ALL_ACTOR_INSTRUMENTATIONS, InstrumentedThread},
};

pub struct InstrumentedThreadWriter {
    clock: Clock,
    reference_instant: Instant,
    current_window_start_time_ns: u64,
    type_name_registry: HashMap<String, u32>,
    target: Arc<InstrumentedThread>,
}

impl InstrumentedThreadWriter {
    pub fn new(clock: Clock, reference_instant: Instant, target: Arc<InstrumentedThread>) -> Self {
        Self {
            clock,
            reference_instant,
            current_window_start_time_ns: 0,
            type_name_registry: HashMap::new(),
            target,
        }
    }

    pub fn new_from_global(thread_name: String) -> Self {
        let clock = Clock::real();
        let reference_instant = ALL_ACTOR_INSTRUMENTATIONS.reference_instant;
        let target = Arc::new(InstrumentedThread::new(
            thread_name,
            clock.now().duration_since(reference_instant).as_nanos() as u64,
        ));
        let key = Arc::as_ptr(&target) as usize;
        ALL_ACTOR_INSTRUMENTATIONS.threads.write().insert(key, target.clone());
        Self::new(clock, reference_instant, target)
    }

    pub fn start_event(&mut self, message_type: &str, dequeue_time_ns: u64) {
        let start_time_ns =
            self.clock.now().duration_since(self.reference_instant).as_nanos() as u64;
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

    pub fn end_event(&mut self) {
        let end_time_ns = self.clock.now().duration_since(self.reference_instant).as_nanos() as u64;
        self.advance_window_if_needed_internal(end_time_ns);
        self.target.end_event(end_time_ns);
    }

    pub fn advance_window_if_needed(&mut self) {
        let current_time_ns =
            self.clock.now().duration_since(self.reference_instant).as_nanos() as u64;
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
