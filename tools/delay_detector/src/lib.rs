use cpu_time::ProcessTime;
use log::{info, warn};
use std::borrow::Cow;
use std::time::{Duration, Instant};

struct Snapshot {
    real_time: Duration,
    cpu_time: Duration,
}

struct SnapshotInstant {
    real_time: Instant,
    cpu_time: ProcessTime,
}

pub struct DelayDetector<'a> {
    msg: Cow<'a, str>,
    started: Instant,
    started_cpu_time: ProcessTime,
    snapshots: Vec<((String, String), Snapshot)>,
    last_snapshot: Option<(String, SnapshotInstant)>,
}

impl<'a> DelayDetector<'a> {
    pub fn new(msg: Cow<'a, str>) -> Self {
        Self {
            msg,
            started: Instant::now(),
            started_cpu_time: ProcessTime::now(),
            snapshots: vec![],
            last_snapshot: None,
        }
    }

    pub fn snapshot(&mut self, msg: &str) {
        let now = Instant::now();
        let cpu_time = ProcessTime::now();
        if let Some((s, started)) = self.last_snapshot.take() {
            self.snapshots.push((
                (s, msg.to_string()),
                Snapshot {
                    real_time: now - started.real_time,
                    cpu_time: started.cpu_time.elapsed(),
                },
            ));
        }
        self.last_snapshot = Some((msg.to_string(), SnapshotInstant { real_time: now, cpu_time }));
    }
}

impl<'a> Drop for DelayDetector<'a> {
    fn drop(&mut self) {
        let elapsed = self.started_cpu_time.elapsed();
        let elapsed_real = self.started.elapsed();
        if elapsed > Duration::from_millis(50) && elapsed <= Duration::from_millis(500) {
            info!(target: "delay_detector", "Took {:?} cpu_time, {:?} real_time processing {}", elapsed, elapsed_real, self.msg);
        }
        if elapsed > Duration::from_millis(500) {
            warn!(target: "delay_detector", "LONG DELAY! Took {:?} cpu_time, {:?} real_time processing {}", elapsed, elapsed_real, self.msg);
            if self.last_snapshot.is_some() {
                self.snapshot("end");
            }
            self.snapshots.sort_by(|a, b| b.1.cpu_time.cmp(&a.1.cpu_time));
            for ((s1, s2), Snapshot { cpu_time, real_time }) in self.snapshots.drain(..) {
                info!(target: "delay_detector", "Took {:?} cpu_time, {:?} real_time between {} and {}", cpu_time, real_time, s1, s2);
            }
        }
    }
}
