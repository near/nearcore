use std::sync::atomic::Ordering;

use near_time::Clock;
use serde::Serialize;

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
}

#[derive(Serialize, Debug)]
pub struct InstrumentedThreadView {
    pub thread_name: String,
    pub active_time_ns: u64,
    pub message_types: Vec<String>,
    pub windows: Vec<InstrumentedWindowView>,
    pub active_event: Option<InstrumentedActiveEventView>,
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
    pub events: Vec<InstrumentedEventView>,
    pub summary: InstrumentedWindowSummaryView,
}

#[derive(Serialize, Debug)]
pub struct InstrumentedEventView {
    pub message_type: u32,
    pub is_start: bool,
    /// Relative to the beginning of the window.
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
    pub message_type: i32,
    pub count: usize,
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
    pub fn to_view(&self) -> Vec<InstrumentedEventView> {
        let len = self.len.load(Ordering::Acquire);
        (0..len).map(|i| self.buffer[i].to_view()).collect::<Vec<_>>()
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
    pub fn to_view(&self) -> InstrumentedWindowView {
        InstrumentedWindowView {
            start_time_ms: self.start_time_ns / 1_000_000,
            events: self.events.to_view(),
            summary: self.summary.to_view(self.index),
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
        for i in 0..NUM_WINDOWS.min(current_window_index) {
            let window_index = current_window_index - i;
            let window = &self.windows[window_index % self.windows.len()];
            windows.push(window.read().to_view());
        }
        InstrumentedThreadView {
            thread_name: self.thread_name.clone(),
            active_time_ns: (current_time_ns.saturating_sub(self.started_time_ns)) / 1_000_000,
            message_types: self.message_type_registry.to_vec(),
            windows,
            active_event,
        }
    }
}

impl AllActorInstrumentations {
    pub fn to_view(&self, clock: &Clock) -> InstrumentedThreadsView {
        let threads = self.threads.read().values().cloned().collect::<Vec<_>>();
        let current_time_ns = clock.now().duration_since(self.reference_instant).as_nanos() as u64;
        let current_time_unix_ms = (clock.now_utc().unix_timestamp_nanos() / 1000000) as u64;
        let mut threads =
            threads.into_iter().map(|thread| thread.to_view(current_time_ns)).collect::<Vec<_>>();
        threads.sort_by_key(|thread| -(thread.active_time_ns as i128));
        InstrumentedThreadsView { current_time_unix_ms, threads }
    }
}
