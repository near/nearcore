use log::{info, warn};
use std::borrow::Cow;
use std::time::{Duration, Instant};

pub struct DelayDetector<'a> {
    min_delay: Duration,
    msg: Cow<'a, str>,
    started: Instant,
    snapshots: Vec<((String, String), Duration)>,
    last_snapshot: Option<(String, Instant)>,
}

impl<'a> DelayDetector<'a> {
    pub fn new(msg: Cow<'a, str>) -> Self {
        Self {
            msg,
            started: Instant::now(),
            snapshots: vec![],
            last_snapshot: None,
            min_delay: Duration::from_millis(50),
        }
    }

    pub fn min_delay(mut self, min_delay: Duration) -> Self {
        self.min_delay = min_delay;
        self
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
        let long_delay = self.min_delay * 10;
        if elapsed > self.min_delay && elapsed <= long_delay {
            info!(target: "delay_detector", "Took {:?} processing {}", elapsed, self.msg);
        }
        if elapsed > long_delay {
            warn!(target: "delay_detector", "LONG DELAY! Took {:?} processing {}", elapsed, self.msg);
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
