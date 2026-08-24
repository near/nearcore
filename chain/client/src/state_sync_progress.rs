use crate::SyncStatus;
use near_async::time::{Clock, Instant};
use near_client_primitives::types::StateSyncStatus;
use near_primitives::types::BlockHeight;
use std::fmt::Write;
use time::ext::InstantExt as _;

const NEW_SAMPLE_WEIGHT: f64 = 0.3;

/// How often to repeat the warning that state sync will miss its deadline.
const DEADLINE_WARNING_INTERVAL_SECONDS: f64 = 60.0;

struct SyncSample {
    sampled_at: Instant,
    parts_downloaded: u64,
    highest_height: BlockHeight,
}

/// Tracks state sync throughput across ticks, so the remaining download can be
/// projected against the deadline at which the node wipes its database.
#[derive(Default)]
pub struct StateSyncProgressTracker {
    prev_sample: Option<SyncSample>,
    rates: Option<SyncRates>,
    last_deadline_warning: Option<Instant>,
}

impl StateSyncProgressTracker {
    /// Last known rates, for callers that do not drive the sampling themselves.
    pub fn rates(&self) -> Option<SyncRates> {
        self.rates
    }

    /// Returns the rates once two samples exist.
    pub fn update_rates_and_warn(
        &mut self,
        clock: &Clock,
        sync_status: &SyncStatus,
    ) -> Option<SyncRates> {
        let SyncStatus::StateSync(status) = sync_status else {
            self.prev_sample = None;
            self.rates = None;
            return None;
        };
        let heights = status.heights?;
        let now = clock.now();
        let (parts_downloaded, _) = status.parts_progress();
        let previous = self.prev_sample.replace(SyncSample {
            sampled_at: now,
            parts_downloaded,
            highest_height: heights.highest,
        });

        let previous = previous?;
        let elapsed = now.signed_duration_since(previous.sampled_at).as_seconds_f64();
        if elapsed <= 0.0 {
            return self.rates;
        }
        let sample = SyncRates {
            parts_per_sec: parts_downloaded.saturating_sub(previous.parts_downloaded) as f64
                / elapsed,
            blocks_per_sec: heights.highest.saturating_sub(previous.highest_height) as f64
                / elapsed,
        };
        let rates = self.rates.map_or(sample, |previous| previous.weighted_average_with(sample));
        self.rates = Some(rates);
        self.warn_if_deadline_will_be_missed(status, rates, now);
        Some(rates)
    }

    /// Losing the deadline race wipes the database, so say so while there is
    /// still time to act on it.
    fn warn_if_deadline_will_be_missed(
        &mut self,
        status: &StateSyncStatus,
        rates: SyncRates,
        now: Instant,
    ) {
        let Some(projection) = deadline_projection(status, rates) else { return };
        if !projection.will_miss_deadline() {
            self.last_deadline_warning = None;
            return;
        }
        let recently_warned = self.last_deadline_warning.is_some_and(|last| {
            now.signed_duration_since(last).as_seconds_f64() < DEADLINE_WARNING_INTERVAL_SECONDS
        });
        if recently_warned {
            return;
        }
        self.last_deadline_warning = Some(now);

        let Some(heights) = status.heights else { return };
        let (downloaded, total) = status.parts_progress();
        tracing::warn!(
            target: "sync",
            parts_downloaded = downloaded,
            parts_total = total,
            parts_per_sec = format!("{:.2}", rates.parts_per_sec),
            download_eta = format_duration(projection.download_seconds),
            deadline_in = format_duration(projection.deadline_seconds),
            sync_block_height = heights.sync_block,
            stale_deadline_height = heights.stale_deadline,
            highest_height = heights.highest,
            "state sync will not finish before the sync hash goes stale; the node \
             will wipe its database and restart. State parts are served over inbound \
             tier3 connections, so check that this node is reachable at its advertised \
             address."
        );
    }
}

/// Smoothed rates used to project state sync completion against its deadline.
#[derive(Clone, Copy, Debug)]
pub struct SyncRates {
    pub parts_per_sec: f64,
    /// Rate at which the network head advances.
    pub blocks_per_sec: f64,
}

impl SyncRates {
    fn weighted_average_with(self, sample: SyncRates) -> SyncRates {
        let weighted_average = |previous: f64, sample: f64| {
            previous * (1.0 - NEW_SAMPLE_WEIGHT) + sample * NEW_SAMPLE_WEIGHT
        };
        SyncRates {
            parts_per_sec: weighted_average(self.parts_per_sec, sample.parts_per_sec),
            blocks_per_sec: weighted_average(self.blocks_per_sec, sample.blocks_per_sec),
        }
    }
}

/// The race between finishing the state part download and the sync hash going
/// stale. Losing it costs the node its database.
#[derive(Clone, Copy, Debug)]
pub struct DeadlineProjection {
    pub download_seconds: f64,
    pub deadline_seconds: f64,
}

impl DeadlineProjection {
    pub fn will_miss_deadline(&self) -> bool {
        self.download_seconds >= self.deadline_seconds
    }
}

/// Projects the remaining download against the deadline. `None` until the head
/// rate is known, or once every part has been downloaded.
fn deadline_projection(status: &StateSyncStatus, rates: SyncRates) -> Option<DeadlineProjection> {
    let (downloaded, total) = status.parts_progress();
    let remaining = total.saturating_sub(downloaded);
    let headroom = status.deadline_headroom()?;
    if remaining == 0 || rates.blocks_per_sec <= 0.0 {
        return None;
    }
    // A download making no progress never finishes, which is the case most
    // worth reporting rather than staying silent about.
    let download_seconds = if rates.parts_per_sec > 0.0 {
        remaining as f64 / rates.parts_per_sec
    } else {
        f64::INFINITY
    };
    Some(DeadlineProjection {
        download_seconds,
        deadline_seconds: headroom as f64 / rates.blocks_per_sec,
    })
}

/// A stalled download projects to absurd durations, so anything past a few
/// days is reported as "too long" rather than a meaningless digit count.
const LONGEST_REPORTED_DURATION_SECONDS: u64 = 99 * 3600;

fn format_duration(seconds: f64) -> String {
    if seconds.is_nan() || seconds < 0.0 {
        return "unknown".to_string();
    }
    if seconds > LONGEST_REPORTED_DURATION_SECONDS as f64 {
        return ">99h".to_string();
    }
    let seconds = seconds as u64;
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else {
        format!("{}h{:02}m", seconds / 3600, (seconds % 3600) / 60)
    }
}

/// Renders overall part progress and the deadline race, for example
/// `parts 7981/8885 (90%) · download ETA 8m · deadline in 1975 blk (~43m) · OK`.
pub fn format_state_sync_progress(
    status: &StateSyncStatus,
    rates: Option<SyncRates>,
) -> Option<String> {
    let (downloaded, total) = status.parts_progress();
    if total == 0 {
        return None;
    }
    let projection = rates.and_then(|rates| deadline_projection(status, rates));

    let mut res = format!("parts {downloaded}/{total} ({}%)", downloaded * 100 / total);
    if let Some(projection) = projection {
        write!(res, " · download ETA {}", format_duration(projection.download_seconds)).unwrap();
    }
    if let Some(headroom) = status.deadline_headroom() {
        write!(res, " · deadline in {headroom} blk").unwrap();
        if let Some(projection) = projection {
            write!(res, " (~{})", format_duration(projection.deadline_seconds)).unwrap();
        }
    }
    if let Some(projection) = projection {
        let verdict = if projection.will_miss_deadline() { "WILL MISS" } else { "OK" };
        write!(res, " · {verdict}").unwrap();
    }
    Some(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_async::time::{Duration, FakeClock, Utc};
    use near_client_primitives::types::{ShardSyncStatus, StateSyncHeights};
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::{BlockHeightDelta, ShardId};

    const SYNC_BLOCK: BlockHeight = 1_000;
    const EPOCH_LENGTH: BlockHeightDelta = 43_200;
    const STALE_DEADLINE: BlockHeight = SYNC_BLOCK + EPOCH_LENGTH + 100;

    fn state_sync_status(
        shards: &[(u64, ShardSyncStatus, u64)],
        highest_height: BlockHeight,
    ) -> StateSyncStatus {
        let mut status = StateSyncStatus::new(CryptoHash::default());
        for (shard_id, shard_status, parts_per_shard) in shards {
            status.sync_status.insert(ShardId::new(*shard_id), *shard_status);
            status.parts_per_shard.insert(ShardId::new(*shard_id), *parts_per_shard);
        }
        status.heights = Some(StateSyncHeights {
            sync_block: SYNC_BLOCK,
            stale_deadline: STALE_DEADLINE,
            highest: highest_height,
        });
        status
    }

    #[test]
    fn parts_progress_counts_shards_past_download_as_complete() {
        let status = state_sync_status(
            &[
                (0, ShardSyncStatus::StateDownloadHeader, 100),
                (1, ShardSyncStatus::StateDownloadParts { done: 30, total: 200 }, 200),
                (2, ShardSyncStatus::StateApplyInProgress { done: 5, total: 300 }, 300),
                (3, ShardSyncStatus::StateSyncDone, 400),
            ],
            1_000,
        );
        assert_eq!(status.parts_progress(), (30 + 300 + 400, 1000));
    }

    #[test]
    fn deadline_headroom_is_none_before_first_state_sync_iteration() {
        let status = StateSyncStatus::new(CryptoHash::default());
        assert_eq!(status.deadline_headroom(), None);
    }

    #[test]
    fn deadline_headroom_saturates_once_deadline_passes() {
        let status = state_sync_status(&[], 100_000);
        assert_eq!(status.deadline_headroom(), Some(0));
    }

    #[test]
    fn progress_reports_ok_when_download_beats_deadline() {
        // 900 parts left at 3/s is 300s; 6000 blocks at 1/s is 6000s.
        let status = state_sync_status(
            &[(0, ShardSyncStatus::StateDownloadParts { done: 100, total: 1000 }, 1000)],
            38_300,
        );
        let rates = SyncRates { parts_per_sec: 3.0, blocks_per_sec: 1.0 };
        let progress = format_state_sync_progress(&status, Some(rates)).unwrap();
        assert!(progress.starts_with("parts 100/1000 (10%)"), "{progress}");
        assert!(progress.contains("download ETA 5m"), "{progress}");
        assert!(progress.ends_with("· OK"), "{progress}");
    }

    #[test]
    fn progress_reports_will_miss_when_download_loses_deadline() {
        // 900 parts left at 0.01/s is 90000s; 600 blocks at 1/s is 600s.
        let status = state_sync_status(
            &[(0, ShardSyncStatus::StateDownloadParts { done: 100, total: 1000 }, 1000)],
            43_700,
        );
        let rates = SyncRates { parts_per_sec: 0.01, blocks_per_sec: 1.0 };
        let progress = format_state_sync_progress(&status, Some(rates)).unwrap();
        assert!(progress.ends_with("· WILL MISS"), "{progress}");
        let projection = deadline_projection(&status, rates).unwrap();
        assert!(projection.will_miss_deadline());
    }

    #[test]
    fn progress_omits_projection_until_rates_are_known() {
        let status = state_sync_status(
            &[(0, ShardSyncStatus::StateDownloadParts { done: 100, total: 1000 }, 1000)],
            1_000,
        );
        let progress = format_state_sync_progress(&status, None).unwrap();
        assert_eq!(progress, "parts 100/1000 (10%) · deadline in 43300 blk");
    }

    #[test]
    fn progress_is_absent_before_any_shard_reports_its_part_count() {
        let status = StateSyncStatus::new(CryptoHash::default());
        assert_eq!(format_state_sync_progress(&status, None), None);
    }

    #[test]
    fn finished_download_has_no_projection() {
        let status = state_sync_status(
            &[(0, ShardSyncStatus::StateApplyInProgress { done: 1, total: 1000 }, 1000)],
            1_000,
        );
        let rates = SyncRates { parts_per_sec: 3.0, blocks_per_sec: 1.0 };
        assert!(deadline_projection(&status, rates).is_none());
    }

    #[test]
    fn durations_are_formatted_by_magnitude() {
        assert_eq!(format_duration(45.0), "45s");
        assert_eq!(format_duration(300.0), "5m");
        assert_eq!(format_duration(3600.0 * 2.0 + 180.0), "2h03m");
        assert_eq!(format_duration(98.0 * 3600.0), "98h00m");
        assert_eq!(format_duration(f64::INFINITY), ">99h");
        assert_eq!(format_duration(-1.0), "unknown");
    }

    fn downloading(parts_done: u64, highest_height: BlockHeight) -> SyncStatus {
        SyncStatus::StateSync(state_sync_status(
            &[(
                0,
                ShardSyncStatus::StateDownloadParts { done: parts_done, total: 100_000 },
                100_000,
            )],
            highest_height,
        ))
    }

    fn fake_clock() -> FakeClock {
        FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap())
    }

    /// Two samples 10s apart, establishing 3 parts/s and 2 blocks/s.
    fn tracker_with_established_rates(clock: &FakeClock) -> StateSyncProgressTracker {
        let mut tracker = StateSyncProgressTracker::default();
        tracker.update_rates_and_warn(&clock.clock(), &downloading(0, 1_000));
        clock.advance(Duration::seconds(10));
        tracker.update_rates_and_warn(&clock.clock(), &downloading(30, 1_020));
        tracker
    }

    #[test]
    fn single_sample_yields_no_rates() {
        let clock = fake_clock();
        let mut tracker = StateSyncProgressTracker::default();
        assert!(tracker.update_rates_and_warn(&clock.clock(), &downloading(0, 1_000)).is_none());
        assert!(tracker.rates().is_none());
    }

    #[test]
    fn rates_are_derived_from_two_samples() {
        let clock = fake_clock();
        let mut tracker = StateSyncProgressTracker::default();
        tracker.update_rates_and_warn(&clock.clock(), &downloading(0, 1_000));
        clock.advance(Duration::seconds(10));
        let rates = tracker.update_rates_and_warn(&clock.clock(), &downloading(30, 1_020)).unwrap();
        assert_eq!(rates.parts_per_sec, 3.0);
        assert_eq!(rates.blocks_per_sec, 2.0);
    }

    #[test]
    fn stalled_sample_does_not_erase_established_rate() {
        let clock = fake_clock();
        let mut tracker = tracker_with_established_rates(&clock);
        clock.advance(Duration::seconds(10));
        let rates = tracker.update_rates_and_warn(&clock.clock(), &downloading(30, 1_020)).unwrap();
        assert!((rates.parts_per_sec - 3.0 * (1.0 - NEW_SAMPLE_WEIGHT)).abs() < 1e-9);
    }

    #[test]
    fn second_sample_at_same_instant_keeps_previous_rates() {
        let clock = fake_clock();
        let mut tracker = tracker_with_established_rates(&clock);
        let rates = tracker.update_rates_and_warn(&clock.clock(), &downloading(60, 1_040)).unwrap();
        assert_eq!(rates.parts_per_sec, 3.0);
    }

    #[test]
    fn leaving_state_sync_clears_rates() {
        let clock = fake_clock();
        let mut tracker = tracker_with_established_rates(&clock);
        assert!(tracker.rates().is_some());

        assert!(tracker.update_rates_and_warn(&clock.clock(), &SyncStatus::NoSync).is_none());
        assert!(tracker.rates().is_none());
    }

    #[test]
    fn stalled_download_still_loses_deadline_race() {
        let status = state_sync_status(
            &[(0, ShardSyncStatus::StateDownloadParts { done: 100, total: 1000 }, 1000)],
            1_000,
        );
        let stalled = SyncRates { parts_per_sec: 0.0, blocks_per_sec: 1.0 };
        let projection = deadline_projection(&status, stalled).unwrap();
        assert!(projection.will_miss_deadline());
        let progress = format_state_sync_progress(&status, Some(stalled)).unwrap();
        assert!(progress.contains("download ETA >99h"), "{progress}");
        assert!(progress.ends_with("· WILL MISS"), "{progress}");
    }
}
