use log::{info, warn};
use std::borrow::Cow;
use std::time::{Duration, Instant};

pub struct DelayDetector<'a> {
    msg: Cow<'a, str>,
    started: Instant,
    snapshots: Vec<((String, String), Duration)>,
    last_snapshot: Option<(String, Instant)>,
}

impl<'a> DelayDetector<'a> {
    pub fn new(msg: Cow<'a, str>) -> Self {
        Self { msg, started: Instant::now(), snapshots: vec![], last_snapshot: None }
    }

    pub fn snapshot(&mut self, msg: &str) {
        let now = Instant::now();
        if let Some((s, started)) = self.last_snapshot.take() {
            self.snapshots.push(((s, msg.to_string()), now - started));
        }
        self.last_snapshot = Some((msg.to_string(), now));
    }
}

impl<'a> Drop for DelayDetector<'a> {
    fn drop(&mut self) {
        let elapsed = Instant::now() - self.started;
        if elapsed > Duration::from_millis(50) && elapsed <= Duration::from_millis(500) {
            info!(target: "delay_detector", "Took {:?} processing {}", elapsed, self.msg);
        }
        if elapsed > Duration::from_millis(500) {
            warn!(target: "delay_detector", "LONG DELAY! Took {:?} processing {}", elapsed, self.msg);
        }
        if elapsed > Duration::from_millis(50) {
            if self.last_snapshot.is_some() {
                self.snapshot("end");
            }
            self.snapshots.sort_by(|a, b| b.1.cmp(&a.1));
            for ((s1, s2), duration) in self.snapshots.drain(..) {
                info!(target: "delay_detector", "Took {:?} between {} and {}", duration, s1, s2);
            }
        }
    }
}
