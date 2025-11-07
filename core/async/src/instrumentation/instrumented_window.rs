use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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

    fn clear(&self) {
        // Relaxed because the synchronization is performed via the RwLock (which
        // is guaranteed because the caller of this function takes &mut self).
        self.len.store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Default)]
pub struct InstrumentedEvent {
    /// Encoded as message type id | is_start << 32
    pub event: AtomicU64,
    pub relative_timestamp_ns: AtomicU64,
}
