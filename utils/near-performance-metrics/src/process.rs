use crate::stats::print_performance_stats;
use std::thread;
use std::time::Duration;
use tracing::{error, info};

pub fn schedule_printing_performance_stats(sleep_time: Duration) {
    if cfg!(feature = "performance_stats") {
        if sleep_time.is_zero() {
            info!("print_performance_stats: disabled");
            return;
        }
        info!("print_performance_stats: enabled");

        if let Err(err) =
            thread::Builder::new().name("PerformanceMetrics".to_string()).spawn(move || loop {
                print_performance_stats(sleep_time);
                thread::sleep(sleep_time);
            })
        {
            error!("failed to spawn the thread: {}", err);
        }
    }
}
