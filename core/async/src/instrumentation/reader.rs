use std::collections::HashMap;
use std::sync::atomic::Ordering;

use near_time::Clock;
use serde::Serialize;

use crate::instrumentation::WINDOW_SIZE_NS;
use crate::instrumentation::{
    NUM_WINDOWS,
    data::{
        AggregatedMessageTypeStats, AllActorInstrumentations, InstrumentedEvent,
        InstrumentedEventBuffer, InstrumentedThread, InstrumentedWindow, InstrumentedWindowSummary,
        MessageTypeRegistry,
    },
};

#[derive(Serialize, Debug)]
pub struct InstrumentedThreadsView {
    pub threads: Vec<InstrumentedThreadView>,
    pub current_time_unix_ms: u64,
    pub current_time_relative_ms: u64,
}

#[derive(Serialize, Debug)]
pub struct InstrumentedThreadView {
    pub thread_name: String,
    pub active_time_ns: u64,
    pub message_types: Vec<String>,
    pub windows: Vec<InstrumentedWindowView>,
    pub active_event: Option<InstrumentedActiveEventView>,
    pub queue: HashMap<String, u64>,
}

#[derive(Serialize, Debug)]
pub struct InstrumentedActiveEventView {
    pub message_type: u32,
    /// Nanoseconds active, as of current_time_ms in InstrumentedThreadsView.
    pub active_for_ns: u64,
}

#[derive(Serialize, Debug)]
pub struct InstrumentedWindowView {
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    pub events: Vec<InstrumentedEventView>,
    pub events_overfilled: bool,
    pub summary: InstrumentedWindowSummaryView,
    pub dequeue_summary: InstrumentedWindowSummaryView,
}

impl InstrumentedWindowView {
    /// Sometimes one or more recent windows have not yet been created because an event has been
    /// blocking the thread for so long. In that case, fill in the missing windows with maybe an
    /// active event.
    pub fn new_filler(
        start_time_ns: u64,
        end_time_ns: u64,
        active_event: Option<&InstrumentedActiveEventView>,
    ) -> Self {
        let mut view = InstrumentedWindowView {
            start_time_ms: start_time_ns / 1_000_000,
            end_time_ms: end_time_ns / 1_000_000,
            events: Vec::new(),
            events_overfilled: false,
            summary: InstrumentedWindowSummaryView { message_stats_by_type: Vec::new() },
            dequeue_summary: InstrumentedWindowSummaryView { message_stats_by_type: Vec::new() },
        };
        if let Some(active_event) = active_event {
            view.summary.message_stats_by_type.push(MessageStatsForTypeView {
                message_type: active_event.message_type as i32,
                count: 1,
                total_time_ns: end_time_ns - start_time_ns,
            });
        }
        view
    }
}

#[derive(Serialize, Debug)]
pub struct InstrumentedEventView {
    #[serde(rename = "m")]
    pub message_type: u32,
    #[serde(rename = "s")]
    pub is_start: bool,
    /// Relative to the beginning of the window.
    #[serde(rename = "t")]
    pub relative_timestamp_ns: u64,
}

#[derive(Serialize, Debug)]
pub struct InstrumentedWindowSummaryView {
    pub message_stats_by_type: Vec<MessageStatsForTypeView>,
}

#[derive(Serialize, Debug)]
pub struct MessageStatsForTypeView {
    /// Index into InstrumentedThreadView.message_types.
    /// May be -1 for unknown message types (if the type registry temporarily overflowed).
    #[serde(rename = "m")]
    pub message_type: i32,
    #[serde(rename = "c")]
    pub count: usize,
    #[serde(rename = "t")]
    pub total_time_ns: u64,
}

fn decode_message_event(encoded: u64) -> (u32, bool) {
    let message_type = (encoded & 0xFFFFFFFF) as u32;
    let is_start = (encoded & (1 << 32)) != 0;
    (message_type, is_start)
}

impl InstrumentedEvent {
    pub fn to_view(&self) -> InstrumentedEventView {
        let encoded = self.event.load(Ordering::Relaxed);
        let (message_type, is_start) = decode_message_event(encoded);
        InstrumentedEventView {
            message_type,
            is_start,
            relative_timestamp_ns: self.relative_timestamp_ns.load(Ordering::Acquire),
        }
    }
}

impl InstrumentedEventBuffer {
    /// Returns the events and whether the buffer is overfilled.
    pub fn to_view(&self) -> (Vec<InstrumentedEventView>, bool) {
        let len = self.len.load(Ordering::Acquire);
        let readable_len = len.min(self.buffer.len());
        (
            (0..readable_len).map(|i| self.buffer[i].to_view()).collect::<Vec<_>>(),
            len != readable_len,
        )
    }
}

impl InstrumentedWindowSummary {
    pub fn to_view(&self, window_index: usize) -> InstrumentedWindowSummaryView {
        let mut message_stats_by_type = Vec::new();
        for (i, stats) in self.time_by_message_type.iter().enumerate() {
            if let Some(view) = stats.to_view(window_index, i as i32) {
                message_stats_by_type.push(view);
            }
        }
        if let Some(unknown) = self.unknown_total.to_view(window_index, -1) {
            message_stats_by_type.push(unknown);
        }
        InstrumentedWindowSummaryView { message_stats_by_type }
    }
}

impl AggregatedMessageTypeStats {
    pub fn to_view(
        &self,
        window_index: usize,
        message_type_id: i32,
    ) -> Option<MessageStatsForTypeView> {
        if self.window_index.load(Ordering::Acquire) == window_index {
            let count = self.count.load(Ordering::Relaxed);
            if count > 0 {
                let total_time_ns = self.total_time_ns.load(Ordering::Relaxed);
                return Some(MessageStatsForTypeView {
                    message_type: message_type_id,
                    count,
                    total_time_ns,
                });
            }
        }
        None
    }
}

impl InstrumentedWindow {
    pub fn to_view(&self, end_time: u64) -> InstrumentedWindowView {
        let (events, events_overfilled) = self.events.to_view();
        InstrumentedWindowView {
            start_time_ms: self.start_time_ns / 1_000_000,
            end_time_ms: end_time / 1_000_000,
            events,
            events_overfilled,
            summary: self.summary.to_view(self.index),
            dequeue_summary: self.dequeue_summary.to_view(self.index),
        }
    }
}

impl MessageTypeRegistry {
    pub fn to_vec(&self) -> Vec<String> {
        self.types.read().clone()
    }
}

impl InstrumentedThread {
    pub fn to_view(&self, current_time_ns: u64) -> InstrumentedThreadView {
        let active_event = self.active_event.load(Ordering::Acquire);
        let active_event = if active_event != 0 {
            let start_time = self.active_event_start_ns.load(Ordering::Relaxed);
            let (message_type, _) = decode_message_event(active_event);
            Some(InstrumentedActiveEventView {
                message_type,
                active_for_ns: current_time_ns.saturating_sub(start_time),
            })
        } else {
            None
        };
        let current_window_index = self.current_window_index.load(Ordering::Acquire);
        let mut windows = Vec::new();
        let mut prev_window_start_time = current_time_ns;

        // Fill in missing windows if the current time is too far ahead of the last recorded window.
        let last_recorded_window_start_time =
            self.windows[current_window_index % self.windows.len()].read().start_time_ns;
        while prev_window_start_time > last_recorded_window_start_time + WINDOW_SIZE_NS {
            let modulo =
                (prev_window_start_time - last_recorded_window_start_time) % WINDOW_SIZE_NS;
            let filler_start_time = if modulo == 0 {
                prev_window_start_time - WINDOW_SIZE_NS
            } else {
                prev_window_start_time - modulo
            };
            windows.push(InstrumentedWindowView::new_filler(
                filler_start_time,
                prev_window_start_time,
                active_event.as_ref(),
            ));
            prev_window_start_time = filler_start_time;
        }
        let oldest_time_to_return =
            current_time_ns.saturating_sub(NUM_WINDOWS as u64 * WINDOW_SIZE_NS);

        for i in 0..NUM_WINDOWS.min(current_window_index + 1) {
            let window_index = current_window_index - i;
            let window = &self.windows[window_index % self.windows.len()];
            let read = window.read();
            if read.start_time_ns < oldest_time_to_return {
                break;
            }
            windows.push(read.to_view(prev_window_start_time));
            prev_window_start_time = read.start_time_ns;
        }
        InstrumentedThreadView {
            thread_name: self.thread_name.clone(),
            active_time_ns: (current_time_ns.saturating_sub(self.started_time_ns)) / 1_000_000,
            message_types: self.message_type_registry.to_vec(),
            windows,
            active_event,
            queue: self.queue.get_pending_events(),
        }
    }
}

impl AllActorInstrumentations {
    pub fn to_view(&self, clock: &Clock) -> InstrumentedThreadsView {
        #[allow(clippy::needless_collect)] // to avoid long locking
        let threads = self.threads.read().values().cloned().collect::<Vec<_>>();
        let current_time_ns = clock.now().duration_since(self.reference_instant).as_nanos() as u64;
        let current_time_unix_ms = (clock.now_utc().unix_timestamp_nanos() / 1000000) as u64;
        let mut threads =
            threads.into_iter().map(|thread| thread.to_view(current_time_ns)).collect::<Vec<_>>();
        threads.sort_by_key(|thread| -(thread.active_time_ns as i128));
        InstrumentedThreadsView {
            current_time_unix_ms,
            current_time_relative_ms: current_time_ns / 1_000_000,
            threads,
        }
    }
}
