use crate::instrumentation::reader::InstrumentedThreadsView;

/// Calculate total processing and dequeue times for threads with actor_name in their name.
pub fn get_total_times(actor_name: &str, view: &InstrumentedThreadsView) -> (u64, u64) {
    let mut total_processing_time_ns = 0;
    let mut total_dequeue_time_ns = 0;
    for thread in &view.threads {
        if !thread.thread_name.contains(actor_name) {
            continue;
        }
        for window in &thread.windows {
            for stat in &window.summary.message_stats_by_type {
                total_processing_time_ns += stat.total_time_ns;
            }
            for stat in &window.dequeue_summary.message_stats_by_type {
                total_dequeue_time_ns += stat.total_time_ns;
            }
        }
    }
    (total_processing_time_ns, total_dequeue_time_ns)
}
