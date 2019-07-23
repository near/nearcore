use std::time::Instant;

use actix::Addr;
use ansi_term::Color::{Cyan, Green, White, Yellow};
use log::info;
use serde_json::json;
use sysinfo::{get_current_pid, Pid, ProcessExt, System, SystemExt};

use near_chain::Tip;
use near_telemetry::{telemetry, TelemetryActor};

use crate::types::{NetworkInfo, ShardSyncStatus, SyncStatus};

/// A helper that prints information about current chain and reports to telemetry.
pub struct InfoHelper {
    /// Telemetry actor.
    telemetry_actor: Addr<TelemetryActor>,
    /// Timestamp when client was started.
    started: Instant,
    /// Total number of blocks processed.
    num_blocks_processed: u64,
    /// Total number of transactions processed.
    num_tx_processed: u64,
    /// Process id to query resources.
    pid: Option<Pid>,
    /// System reference.
    sys: System,
}

impl InfoHelper {
    pub fn new(telemetry_actor: Addr<TelemetryActor>) -> Self {
        InfoHelper {
            telemetry_actor,
            started: Instant::now(),
            num_blocks_processed: 0,
            num_tx_processed: 0,
            pid: get_current_pid().ok(),
            sys: System::new(),
        }
    }

    pub fn block_processed(&mut self, num_transactions: u64) {
        self.num_blocks_processed += 1;
        self.num_tx_processed += num_transactions;
    }

    pub fn info(
        &mut self,
        head: &Tip,
        sync_status: &SyncStatus,
        network_info: &NetworkInfo,
        is_validator: bool,
        num_validators: usize,
    ) {
        let (cpu_usage, memory) = if let Some(pid) = self.pid {
            if self.sys.refresh_process(pid) {
                let proc = self
                    .sys
                    .get_process(pid)
                    .expect("refresh_process succeeds, this should be not None");
                (proc.cpu_usage(), proc.memory())
            } else {
                (0.0, 0)
            }
        } else {
            (0.0, 0)
        };

        // Block#, Block Hash, is validator/# validators, active/max peers, traffic, blocks/sec & tx/sec
        let avg_bls = (self.num_blocks_processed as f64)
            / (self.started.elapsed().as_millis() as f64)
            * 1000.0;
        let avg_tps =
            (self.num_tx_processed as f64) / (self.started.elapsed().as_millis() as f64) * 1000.0;
        info!(target: "info", "{} {} {} {} {} {}",
              Yellow.bold().paint(display_sync_status(&sync_status, &head)),
              White.bold().paint(format!("{}/{}", if is_validator { "V" } else { "-" }, num_validators)),
              Cyan.bold().paint(format!("{:2}/{:?}/{:2} peers", network_info.num_active_peers, network_info.most_weight_peers.len(), network_info.peer_max_count)),
              Cyan.bold().paint(format!("⬇ {} ⬆ {}", pretty_bytes_per_sec(network_info.received_bytes_per_sec), pretty_bytes_per_sec(network_info.sent_bytes_per_sec))),
              Green.bold().paint(format!("{:.2} bls {:.2} tps", avg_bls, avg_tps)),
              Cyan.bold().paint(format!("CPU: {:.2}, Mem: {}", cpu_usage, memory))
        );
        self.started = Instant::now();
        self.num_blocks_processed = 0;
        self.num_tx_processed = 0;

        telemetry(
            &self.telemetry_actor,
            json!({
                "status": display_sync_status(&sync_status, &head),
                "latest_block_hash": head.last_block_hash,
                "latest_block_height": head.height,
                "num_peers":  network_info.num_active_peers,
                "bandwidth_download": network_info.received_bytes_per_sec,
                "bandwidth_upload": network_info.sent_bytes_per_sec,
                "cpu": cpu_usage,
                "memory": memory,
            }),
        );
    }
}

fn display_sync_status(sync_status: &SyncStatus, head: &Tip) -> String {
    match sync_status {
        SyncStatus::AwaitingPeers => format!("#{:>8} Waiting for peers", head.height),
        SyncStatus::NoSync => format!("#{:>8} {}", head.height, head.last_block_hash),
        SyncStatus::HeaderSync { current_height, highest_height } => {
            let percent =
                if *highest_height == 0 { 0 } else { current_height * 100 / highest_height };
            format!("#{:>8} Downloading headers {}%", head.height, percent)
        }
        SyncStatus::BodySync { current_height, highest_height } => {
            let percent =
                if *highest_height == 0 { 0 } else { current_height * 100 / highest_height };
            format!("#{:>8} Downloading blocks {}%", head.height, percent)
        }
        SyncStatus::StateSync(_sync_hash, shard_statuses) => {
            let mut res = String::from("State ");
            for (shard_id, shard_status) in shard_statuses {
                res = res
                    + format!(
                        "{}: {}",
                        shard_id,
                        match shard_status {
                            ShardSyncStatus::StateDownload {
                                start_time: _,
                                prev_update_time: _,
                                prev_downloaded_size: _,
                                downloaded_size: _,
                                total_size: _,
                            } => format!("download"),
                            ShardSyncStatus::StateValidation => format!("validation"),
                            ShardSyncStatus::StateDone => format!("done"),
                            ShardSyncStatus::Error(error) => format!("error {}", error),
                        }
                    )
                    .as_str();
            }
            res
        }
        SyncStatus::StateSyncDone => format!("State sync done"),
    }
}

/// Format bytes per second in a nice way.
fn pretty_bytes_per_sec(num: u64) -> String {
    if num < 100 {
        // Under 0.1 kiB, display in bytes.
        format!("{} B/s", num)
    } else if num < 1024 * 1024 {
        // Under 1.0 MiB/sec display in kiB/sec.
        format!("{:.1}kiB/s", num as f64 / 1024.0)
    } else {
        format!("{:.1}MiB/s", num as f64 / (1024.0 * 1024.0))
    }
}
