use std::thread;
use std::time::Duration;

use log::{error, info};

use crate::stats::print_performance_stats;

pub fn schedule_printing_performance_stats(interval: u64) {
    if cfg!(feature = "performance_stats") {
        if interval == 0 {
            info!("print_performance_stats: disabled");
            return;
        }
        info!("print_performance_stats: enabled");

        let sleep_time = Duration::from_secs(60);

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
