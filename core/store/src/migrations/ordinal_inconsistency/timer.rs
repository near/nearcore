use std::time::Duration;

pub struct WorkTimer {
    name: String,
    start: std::time::Instant,
    last_report_time: std::time::Instant,
    total: usize,
    expected_total: usize,
}

impl WorkTimer {
    pub fn new(name: impl ToString, expected_total: usize) -> Self {
        let name = name.to_string();
        tracing::info!(target: "db", "\"{}\": Started", name);
        Self {
            name,
            start: std::time::Instant::now(),
            last_report_time: std::time::Instant::now(),
            total: 0,
            expected_total,
        }
    }

    pub fn add_processed(&mut self, processed: usize) {
        self.total += processed;
        if self.last_report_time.elapsed() > Duration::from_secs(5) {
            tracing::info!(
                target: "db",
                "{}: {}/{} ({:.2}%) in {:.2?}, ETA: {:.2?}s",
                self.name,
                self.total,
                self.expected_total,
                (self.total as f64 / self.expected_total as f64) * 100.0,
                self.start.elapsed(),
                self.expected_total.saturating_sub(self.total) as f64 / self.total as f64
                    * self.start.elapsed().as_secs_f64()
            );
            self.last_report_time = std::time::Instant::now();
        }
    }

    pub fn finish(&self) {
        tracing::info!(
            target: "db",
            "{}: Finished - processed {} in {:.2?}",
            self.name,
            self.total,
            self.start.elapsed()
        );
    }
}
