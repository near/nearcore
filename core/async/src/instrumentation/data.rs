use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use parking_lot::RwLock;

use crate::instrumentation::{NUM_WINDOWS, WINDOW_SIZE_NS};
/// it needs to be at least NUM_WINDOWS + 1, but we round up to a power of two for efficiency
const WINDOW_ARRAY_SIZE: usize = NUM_WINDOWS + 4;

pub struct AllActorInstrumentations {
    /// This is the instant that all timestamps are in reference to.
    pub reference_instant: Instant,
    /// Map from the actor's unique identifier to its instrumentation data.
    pub threads: RwLock<HashMap<usize, Arc<InstrumentedThread>>>,
}

pub static ALL_ACTOR_INSTRUMENTATIONS: LazyLock<AllActorInstrumentations> =
    LazyLock::new(|| AllActorInstrumentations {
        reference_instant: Instant::now(),
        threads: RwLock::new(HashMap::new()),
    });

/// Tracks recent thread activity for the past number of windows.
/// Each window is about 1 second (configurable).
/// The actor thread which handles messages is the one pushing events to this struct,
/// and a debugging thread may pull it to display recent activity.
/// It is very important that pushing events is as efficient as possible: it should not be
/// blocked by any debugging threads, and it should not do big allocations except in the
/// very rare cases.
///
/// Note that this is not the entire state of instrumentation that is maintained by the
/// actor thread; rather, it is the state that is necessarily shared between the actor
/// thread and debugging threads.
///
/// Given that context, you may wonder why the design is so complex with atomics deep in
/// the data structures; after all, can't we just have the actor thread aggregate stats
/// locally and then give them (as an Arc) to the common data structure when a window is
/// complete, and that way we won't need any atomics inside the window structures? The
/// problem is that as soon as we the actor thread gives away the Arc, it has no chance
/// of reusing it, so all of that memory would have to be freed later (by the actor thread
/// most likely), and the next window would have to be reallocated from scratch. This would
/// lead to constant allocations and deallocations. It might not sound much, but when this
/// is done on every actor thread, it will be significant.
pub struct InstrumentedThread {
    pub thread_name: String,
    /// Time when this thread was started, in nanoseconds since reference_instant.
    pub started_time_ns: u64,
    /// Registry of message types that are seen so far on this thread. It is used to
    /// enable dense indexing of per-message-type stats in the InstrumentedWindowSummary.
    pub message_type_registry: MessageTypeRegistry,
    /// This is a fixed-size ring buffer of windows. Although each element is protected
    /// by a RwLock, the only time we need to write-lock it is when we need to initialize
    /// a new window (every time we advance to the next window). Doing this lock does not
    /// cause any contention because the reader thread would not be reading that window.
    ///
    /// If there are N windows we keep, then the size of this vector is N + 1. This is
    /// because the extra window is used for initialization. When we advance to the next
    /// window, we first write-lock the next window and initialize it (meanwhile knowing
    /// that any reader thread would not be touching that window at all because it is
    /// the extra window doesn't hold useful data yet), and only after that do we advance
    /// the current window index.
    ///
    /// All other operations (including when we record new events) only need a read lock,
    /// meaning there should be no contention at all.
    pub windows: Vec<RwLock<InstrumentedWindow>>,
    /// This is a monotonically increasing index of the current window;
    /// it does not wrap around. Rather, we calculate the actual index into the array
    /// by modding by the array's size.
    pub current_window_index: AtomicUsize,
    /// The event that is currently being processed, if any, encoded with
    /// encode_message_event().
    pub active_event: AtomicU64,
    pub active_event_start_ns: AtomicU64,
}

impl InstrumentedThread {
    pub fn new(thread_name: String, start_time: u64) -> Self {
        Self {
            thread_name,
            started_time_ns: start_time,
            message_type_registry: MessageTypeRegistry::default(),
            windows: (0..WINDOW_ARRAY_SIZE)
                .map(|_| RwLock::new(InstrumentedWindow::new()))
                .collect(),
            current_window_index: AtomicUsize::new(0),
            active_event: AtomicU64::new(0),
            active_event_start_ns: AtomicU64::new(0),
        }
    }

    pub fn start_event(&self, message_type_id: u32, timestamp_ns: u64, dequeue_time_ns: u64) {
        let encoded_event = encode_message_event(message_type_id, true);
        self.active_event_start_ns.store(timestamp_ns, Ordering::Relaxed);
        // Release order here because this atomic embeds an "is present" bit, and this
        // is used to synchronize the start timestamp stored above.
        // Note that this isn't actually very sound because it's possible that the reader
        // reads an active event but by the time it reads the timestamp, another event has
        // started. This seems very unlikely though because the reader only needs to do two
        // atomic reads whereas the writer has to do a bunch of other stuff. And we don't
        // need this to be absolutely perfect anyway.
        self.active_event.store(encoded_event, Ordering::Release);
        let current_window_index = self.current_window_index.load(Ordering::Relaxed);
        let window = &self.windows[current_window_index % WINDOW_ARRAY_SIZE];
        let window = window.read();
        window.events.push(encoded_event, timestamp_ns.saturating_sub(window.start_time_ns));
        window.dequeue_summary.add_message_time(
            current_window_index,
            message_type_id,
            dequeue_time_ns,
        );
    }

    pub fn end_event(&self, timestamp_ns: u64) {
        let active_event = self.active_event.load(Ordering::Relaxed);
        let message_type_id = active_event as u32;
        let start_timestamp = self.active_event_start_ns.load(Ordering::Relaxed);
        let encoded_event = encode_message_event(message_type_id, false);
        self.active_event.store(0, Ordering::Relaxed);
        let current_window_index = self.current_window_index.load(Ordering::Relaxed);
        let window = &self.windows[current_window_index % WINDOW_ARRAY_SIZE];
        let window = window.read();
        window.events.push(encoded_event, timestamp_ns.saturating_sub(window.start_time_ns));
        let elapsed_ns = timestamp_ns.saturating_sub(start_timestamp.max(window.start_time_ns));
        window.summary.add_message_time(current_window_index, message_type_id, elapsed_ns);
    }

    pub fn advance_window(&self, window_end_time_ns: u64) {
        let current_window_index = self.current_window_index.load(Ordering::Relaxed);
        let active_event = self.active_event.load(Ordering::Relaxed);
        if active_event != 0 {
            let active_event = active_event as u32;
            let elapsed_in_window = window_end_time_ns
                .saturating_sub(self.active_event_start_ns.load(Ordering::Relaxed))
                .min(WINDOW_SIZE_NS);
            self.windows[current_window_index % WINDOW_ARRAY_SIZE].read().summary.add_message_time(
                current_window_index,
                active_event,
                elapsed_in_window,
            );
        }
        let next_window_index = current_window_index + 1;
        let next_window = &self.windows[next_window_index % WINDOW_ARRAY_SIZE];
        let num_types = self.message_type_registry.types.read().len();
        next_window.write().reinitialize(next_window_index, window_end_time_ns, num_types);
        // Release ordering to indicate to any reader that the new window is ready for reading.
        self.current_window_index.store(next_window_index, Ordering::Release);
    }
}

/// This is the registry of message type indexes. It may be surprising to see that
/// there is no map from the type name to the index. This is because the purpose of
/// this registry is only to provide information on the mapping to the debug UI frontend.
/// To figure out what message type ID corresponds to a message type, it is the actor
/// thread's responsibility to remember and lookup the mapping - it is much easier there
/// because it does not need any locking.
#[derive(Default)]
pub struct MessageTypeRegistry {
    pub types: RwLock<Vec<String>>,
}

impl MessageTypeRegistry {
    pub fn push_type(&self, type_name: String) {
        let mut types = self.types.write();
        types.push(type_name);
    }
}

/// Stats in a single window. It is designed so that recording events and reading stats
/// are both lock-free. The window can still be mutated (as in, changing index or resizing
/// the buffers) when it is being reinitialized; see the comment in
/// InstrumentedThread::windows for details.
pub struct InstrumentedWindow {
    pub index: usize,
    pub start_time_ns: u64,
    /// Events recorded during this window. If the number of events exceed the capacity
    /// of this buffer, we will simply stop recording.
    pub events: InstrumentedEventBuffer,
    pub summary: InstrumentedWindowSummary,
    pub dequeue_summary: InstrumentedWindowSummary,
}

impl InstrumentedWindow {
    pub fn new() -> Self {
        Self {
            index: 0,
            start_time_ns: 0,
            events: InstrumentedEventBuffer::new(128),
            summary: InstrumentedWindowSummary::new(16),
            dequeue_summary: InstrumentedWindowSummary::new(16),
        }
    }

    pub fn reinitialize(&mut self, index: usize, start_time_ns: u64, num_message_types: usize) {
        self.index = index;
        self.start_time_ns = start_time_ns;
        self.events.clear();
        self.summary.resize(num_message_types);
    }
}

/// The summary of a window, containing the aggregated count and execution time
/// per message type. The array index of the vector corresponds to the message type id.
///
/// The way we handle still-running messages is: if this window is a complete window,
/// a message whose running time crosses the window boundary (left or right) is counted
/// during the window, whereas if the window is the current window, the still-running
/// message (if there is one) is not counted.
pub struct InstrumentedWindowSummary {
    pub time_by_message_type: Vec<AggregatedMessageTypeStats>,
    /// There is a small chance that the current window encountered a new event we haven't
    /// seen before, and we don't have the time to resize the time_by_message_type vector.
    /// In that case, the unknown total here will reflect the stats from the new event type;
    /// it's just that we won't know what the new message type is from the summary. In practice
    /// this should not matter because this should only happen around the first moments of a
    /// node's life.
    pub unknown_total: AggregatedMessageTypeStats,
}

impl InstrumentedWindowSummary {
    pub fn new(initial_capacity: usize) -> Self {
        Self {
            time_by_message_type: (0..initial_capacity)
                .map(|_| AggregatedMessageTypeStats::default())
                .collect(),
            unknown_total: AggregatedMessageTypeStats::default(),
        }
    }

    pub fn resize(&mut self, num_message_types: usize) {
        self.time_by_message_type.resize_with(num_message_types, Default::default);
        self.time_by_message_type
            .resize_with(self.time_by_message_type.capacity(), Default::default);
    }

    pub fn add_message_time(&self, window_index: usize, message_type_id: u32, time_ns: u64) {
        if let Some(stats) = self.time_by_message_type.get(message_type_id as usize) {
            stats.add(window_index, time_ns);
        } else {
            self.unknown_total.add(window_index, time_ns);
        }
    }
}

#[derive(Default)]
pub struct AggregatedMessageTypeStats {
    /// The data is treated as if they were zero, if this window index is not equal to the
    /// current window index. This is to avoid having to reset the stats to zero.
    pub window_index: AtomicUsize,
    pub count: AtomicUsize,
    pub total_time_ns: AtomicU64,
}

impl AggregatedMessageTypeStats {
    pub fn add(&self, window_index: usize, time_ns: u64) {
        let current_window_index = self.window_index.load(Ordering::Relaxed);
        let (current_count, current_time) = if current_window_index == window_index {
            (self.count.load(Ordering::Relaxed), self.total_time_ns.load(Ordering::Relaxed))
        } else {
            (0, 0)
        };
        self.count.store(current_count + 1, Ordering::Relaxed);
        self.total_time_ns.store(current_time + time_ns, Ordering::Relaxed);
        if current_window_index != window_index {
            // Use release ordering here because this indicates to the reader that the
            // count and time should now be visible (whereas they used to be ignored when the
            // window index was wrong).
            self.window_index.store(window_index, Ordering::Release);
        }
    }
}

/// Represents a vector of instrumented events, except that this is readable and
/// writeable without locking at all.
pub struct InstrumentedEventBuffer {
    pub buffer: Vec<InstrumentedEvent>,
    pub len: AtomicUsize,
}

impl InstrumentedEventBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: (0..capacity).map(|_| InstrumentedEvent::default()).collect(),
            len: AtomicUsize::new(0),
        }
    }

    /// Pushes a new event to the buffer. If the buffer is full, the event is dropped.
    /// This function is lock-free.
    /// This function assumes only one thread calls it.
    pub fn push(&self, encoded_event: u64, relative_timestamp_ns: u64) {
        let len = self.len.load(std::sync::atomic::Ordering::Relaxed);
        if len < self.buffer.len() {
            let event = &self.buffer[len];
            event.event.store(encoded_event, Ordering::Relaxed);
            event.relative_timestamp_ns.store(relative_timestamp_ns, Ordering::Relaxed);
        }
        // Increment even if the buffer is full - that way the reader knows if the
        // buffer is overfilled.
        // Release ordering because the length is used to tell any reader that
        // the event is ready to be read, so all previous writes must be visible.
        self.len.store(len + 1, Ordering::Release);
    }

    pub fn clear(&mut self) {
        // Relaxed because the synchronization is performed via the RwLock (which
        // is guaranteed because this function takes &mut self).
        self.len.store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Default)]
pub struct InstrumentedEvent {
    /// Encoded as message type id | is_start << 32
    pub event: AtomicU64,
    pub relative_timestamp_ns: AtomicU64,
}

fn encode_message_event(message_type_id: u32, is_start: bool) -> u64 {
    let mut event = message_type_id as u64;
    if is_start {
        event |= 1 << 32;
    }
    event
}
