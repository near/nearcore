use log::info;
use std::time::{Duration, Instant};

pub struct DelayDetector<'a> {
    msg: &'a str,
    started: Instant,
}

impl<'a> DelayDetector<'a> {
    pub fn new(msg: &'a str) -> Self {
        Self { msg, started: Instant::now() }
    }
}

impl<'a> Drop for DelayDetector<'a> {
    fn drop(&mut self) {
        let elapsed = Instant::now() - self.started;
        if elapsed > Duration::from_millis(50) {
            info!(target: "chain", "Took {:?} processing {}", elapsed, self.msg);
        }
        if elapsed > Duration::from_millis(500) || self.msg.starts_with("network request \"sync") {
            info!(target: "chain", "WTF Took {:?} processing {}", elapsed, self.msg);
        }
    }
}
