use near_time::Clock;

use crate::instrumentation::{data::ALL_ACTOR_INSTRUMENTATIONS, reader::InstrumentedThreadsView};

pub(crate) mod data;
pub mod queue;
pub mod reader;
pub(crate) mod writer;

/// Window size. Windows are aligned to whole multiples of this size since UNIX epoch,
/// regardless of when the actor thread started.
const WINDOW_SIZE_NS: u64 = 500_000_000; // 500ms
/// Number of most recent windows to maintain stats for.
const NUM_WINDOWS: usize = 60; // keep stats for the last 30 seconds

pub fn all_actor_instrumentations_view(clock: &Clock) -> InstrumentedThreadsView {
    ALL_ACTOR_INSTRUMENTATIONS.to_view(clock)
}

pub fn all_queues() -> queue::AllQueuesView {
    queue::ALL_QUEUES.to_view()
}
