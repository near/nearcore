use std::cmp::min;
use std::time::Instant;

use actix::Addr;
use ansi_term::Color::{Blue, Cyan, Green, White, Yellow};
use log::info;
use serde_json::json;
use sysinfo::{get_current_pid, set_open_files_limit, Pid, ProcessExt, System, SystemExt};

use near_chain::Tip;
use near_network::types::{NetworkInfo, PeerId};
use near_primitives::serialize::to_base;
use near_primitives::types::Gas;
use near_primitives::types::Version;
use near_telemetry::{telemetry, TelemetryActor};

use crate::types::{BlockProducer, ClientConfig, ShardSyncStatus, SyncStatus};

/// A helper that prints information about current chain and reports to telemetry.
pub struct InfoHelper {
    /// Nearcore agent (executable) version
    nearcore_version: Version,
    /// System reference.
    sys: System,
    /// Process id to query resources.
    pid: Option<Pid>,
    /// Timestamp when client was started.
    started: Instant,
    /// Sign telemetry with block producer key if available.
    block_producer: Option<BlockProducer>,
    /// Total number of blocks processed.
    num_blocks_processed: u64,
    /// Total gas used during period.
    gas_used: u64,
    /// Total gas limit during period.
    gas_limit: u64,
    /// Telemetry actor.
    telemetry_actor: Addr<TelemetryActor>,
}

impl InfoHelper {
    pub fn new(
        telemetry_actor: Addr<TelemetryActor>,
        client_config: &ClientConfig,
        block_producer: Option<BlockProducer>,
    ) -> Self {
        set_open_files_limit(0);
        InfoHelper {
            nearcore_version: client_config.version.clone(),
            sys: System::new(),
            pid: get_current_pid().ok(),
            started: Instant::now(),
            num_blocks_processed: 0,
            gas_used: 0,
            gas_limit: 0,
            telemetry_actor,
            block_producer,
        }
    }

    pub fn block_processed(&mut self, gas_used: Gas, gas_limit: Gas) {
        self.num_blocks_processed += 1;
        self.gas_used += gas_used;
        self.gas_limit += gas_limit;
    }

    pub fn info(
        &mut self,
        head: &Tip,
        sync_status: &SyncStatus,
        node_id: &PeerId,
        network_info: &NetworkInfo,
        is_validator: bool,
        is_fisherman: bool,
        num_validators: usize,
    ) {
        let (cpu_usage, memory_usage) = if let Some(pid) = self.pid {
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
        let avg_gas_used =
            ((self.gas_used as f64) / (self.started.elapsed().as_millis() as f64) * 1000.0) as u64;
        info!(target: "stats", "{} {} {} {} {} {}",
              Yellow.bold().paint(display_sync_status(&sync_status, &head)),
              White.bold().paint(format!("{}/{}", if is_validator { "V" } else if is_fisherman { "F" } else { "-" }, num_validators)),
              Cyan.bold().paint(format!("{:2}/{:?}/{:2} peers", network_info.num_active_peers, network_info.highest_height_peers.len(), network_info.peer_max_count)),
              Cyan.bold().paint(format!("⬇ {} ⬆ {}", pretty_bytes_per_sec(network_info.received_bytes_per_sec), pretty_bytes_per_sec(network_info.sent_bytes_per_sec))),
              Green.bold().paint(format!("{:.2} bps {}", avg_bls, gas_used_per_sec(avg_gas_used))),
              Blue.bold().paint(format!("CPU: {:.0}%, Mem: {}", cpu_usage, pretty_bytes(memory_usage * 1024)))
        );

        self.started = Instant::now();
        self.num_blocks_processed = 0;
        self.gas_used = 0;
        self.gas_limit = 0;

        telemetry(
            &self.telemetry_actor,
            try_sign_json(
                json!({
                    "agent": {
                        "name": "near-rs",
                        "version": self.nearcore_version.version,
                        "build": self.nearcore_version.build,
                    },
                    "system": {
                        "bandwidth_download": network_info.received_bytes_per_sec,
                        "bandwidth_upload": network_info.sent_bytes_per_sec,
                        "cpu_usage": cpu_usage,
                        "memory_usage": memory_usage,
                    },
                    "chain": {
                        "node_id": node_id.to_string(),
                        "account_id": self.block_producer.clone().map(|bp| bp.account_id).unwrap_or("".to_string()),
                        "is_validator": is_validator,
                        "status": sync_status.as_variant_name(),
                        "latest_block_hash": to_base(&head.last_block_hash),
                        "latest_block_height": head.height,
                        "num_peers":  network_info.num_active_peers,
                    }
                }),
                &self.block_producer,
            ),
        );
    }
}

/// Tries to sign given JSON with block producer if it's present and all succeeds.
fn try_sign_json(
    mut value: serde_json::Value,
    block_producer: &Option<BlockProducer>,
) -> serde_json::Value {
    let mut signature = "".to_string();
    if let Some(bp) = block_producer {
        if let Ok(s) = serde_json::to_string(&value) {
            signature = format!("{}", bp.signer.sign(s.as_bytes()));
        }
    }
    value["signature"] = signature.into();
    value
}

fn display_sync_status(sync_status: &SyncStatus, head: &Tip) -> String {
    match sync_status {
        SyncStatus::AwaitingPeers => format!("#{:>8} Waiting for peers", head.height),
        SyncStatus::NoSync => format!("#{:>8} {:>44}", head.height, head.last_block_hash),
        SyncStatus::HeaderSync { current_height, highest_height } => {
            let percent = if *highest_height == 0 {
                0
            } else {
                min(current_height, highest_height) * 100 / highest_height
            };
            format!("#{:>8} Downloading headers {}%", head.height, percent)
        }
        SyncStatus::BodySync { current_height, highest_height } => {
            let percent =
                if *highest_height == 0 { 0 } else { current_height * 100 / highest_height };
            format!("#{:>8} Downloading blocks {}%", current_height, percent)
        }
        SyncStatus::StateSync(_sync_hash, shard_statuses) => {
            let mut res = String::from("State ");
            let mut shard_statuses: Vec<_> = shard_statuses.iter().collect();
            shard_statuses.sort_by_key(|(shard_id, _)| *shard_id);
            for (shard_id, shard_status) in shard_statuses {
                res = res
                    + format!(
                        "[{}: {}]",
                        shard_id,
                        match shard_status.status {
                            ShardSyncStatus::StateDownloadHeader => format!("header"),
                            ShardSyncStatus::StateDownloadParts => format!("parts"),
                            ShardSyncStatus::StateDownloadFinalize => format!("finalization"),
                            ShardSyncStatus::StateDownloadComplete => format!("done"),
                        }
                    )
                    .as_str();
            }
            res
        }
        SyncStatus::StateSyncDone => format!("State sync done"),
    }
}

const KILOBYTE: u64 = 1024;
const MEGABYTE: u64 = KILOBYTE * 1024;
const GIGABYTE: u64 = MEGABYTE * 1024;

/// Format bytes per second in a nice way.
fn pretty_bytes_per_sec(num: u64) -> String {
    if num < 100 {
        // Under 0.1 kiB, display in bytes.
        format!("{} B/s", num)
    } else if num < MEGABYTE {
        // Under 1.0 MiB/sec display in kiB/sec.
        format!("{:.1}kiB/s", num as f64 / KILOBYTE as f64)
    } else {
        format!("{:.1}MiB/s", num as f64 / MEGABYTE as f64)
    }
}

fn pretty_bytes(num: u64) -> String {
    if num < 1024 {
        format!("{} B", num)
    } else if num < MEGABYTE {
        format!("{:.1} kiB", num as f64 / KILOBYTE as f64)
    } else if num < GIGABYTE {
        format!("{:.1} MiB", num as f64 / MEGABYTE as f64)
    } else {
        format!("{:.1} GiB", num as f64 / GIGABYTE as f64)
    }
}

fn gas_used_per_sec(num: u64) -> String {
    if num < 1000 {
        format!("{} gas/s", num)
    } else if num < 1_000_000 {
        format!("{:.1} Kgas/s", num as f64 / 1_000.0)
    } else {
        format!("{:.1} Mgas/s", num as f64 / 1_000_000.0)
    }
}
