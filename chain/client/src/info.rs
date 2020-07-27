use std::cmp::min;
use std::sync::Arc;
use std::time::Instant;

use actix::Addr;
use ansi_term::Color::{Blue, Cyan, Green, White, Yellow};
use log::info;
use sysinfo::{get_current_pid, set_open_files_limit, Pid, ProcessExt, System, SystemExt};

use near_chain_configs::ClientConfig;
use near_metrics::set_gauge;
use near_network::types::NetworkInfo;
use near_primitives::block::Tip;
use near_primitives::network::PeerId;
use near_primitives::serialize::to_base;
use near_primitives::telemetry::{
    TelemetryAgentInfo, TelemetryChainInfo, TelemetryInfo, TelemetrySystemInfo,
};
use near_primitives::types::{BlockHeight, Gas};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::Version;
use near_telemetry::{telemetry, TelemetryActor};

use crate::metrics;
use crate::types::ShardSyncStatus;
use crate::SyncStatus;

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
    /// Total number of blocks processed.
    num_blocks_processed: u64,
    /// Total gas used during period.
    gas_used: u64,
    /// Total gas limit during period.
    gas_limit: u64,
    /// Sign telemetry with block producer key if available.
    validator_signer: Option<Arc<dyn ValidatorSigner>>,
    /// Telemetry actor.
    telemetry_actor: Addr<TelemetryActor>,
}

impl InfoHelper {
    pub fn new(
        telemetry_actor: Addr<TelemetryActor>,
        client_config: &ClientConfig,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
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
            validator_signer,
        }
    }

    pub fn block_processed(&mut self, gas_used: Gas, gas_limit: Gas) {
        self.num_blocks_processed += 1;
        self.gas_used += gas_used;
        self.gas_limit += gas_limit;
    }

    pub fn info(
        &mut self,
        genesis_height: BlockHeight,
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
              Yellow.bold().paint(display_sync_status(&sync_status, &head, genesis_height)),
              White.bold().paint(format!("{}/{}", if is_validator { "V" } else if is_fisherman { "F" } else { "-" }, num_validators)),
              Cyan.bold().paint(format!("{:2}/{:?}/{:2} peers", network_info.num_active_peers, network_info.highest_height_peers.len(), network_info.peer_max_count)),
              Cyan.bold().paint(format!("⬇ {} ⬆ {}", pretty_bytes_per_sec(network_info.received_bytes_per_sec), pretty_bytes_per_sec(network_info.sent_bytes_per_sec))),
              Green.bold().paint(format!("{:.2} bps {}", avg_bls, gas_used_per_sec(avg_gas_used))),
              Blue.bold().paint(format!("CPU: {:.0}%, Mem: {}", cpu_usage, pretty_bytes(memory_usage * 1024)))
        );

        set_gauge(&metrics::IS_VALIDATOR, is_validator as i64);
        set_gauge(&metrics::RECEIVED_BYTES_PER_SECOND, network_info.received_bytes_per_sec as i64);
        set_gauge(&metrics::SENT_BYTES_PER_SECOND, network_info.sent_bytes_per_sec as i64);
        set_gauge(&metrics::BLOCKS_PER_MINUTE, (avg_bls * (60 as f64)) as i64);
        set_gauge(&metrics::CPU_USAGE, cpu_usage as i64);
        set_gauge(&metrics::MEMORY_USAGE, (memory_usage * 1024) as i64);

        self.started = Instant::now();
        self.num_blocks_processed = 0;
        self.gas_used = 0;
        self.gas_limit = 0;

        let info = TelemetryInfo {
            agent: TelemetryAgentInfo {
                name: "near-rs".to_string(),
                version: self.nearcore_version.version.clone(),
                build: self.nearcore_version.build.clone(),
            },
            system: TelemetrySystemInfo {
                bandwidth_download: network_info.received_bytes_per_sec,
                bandwidth_upload: network_info.sent_bytes_per_sec,
                cpu_usage,
                memory_usage,
            },
            chain: TelemetryChainInfo {
                node_id: node_id.to_string(),
                account_id: self
                    .validator_signer
                    .clone()
                    .map(|bp| bp.validator_id().clone())
                    .unwrap_or_else(String::new),
                is_validator,
                status: sync_status.as_variant_name().to_string(),
                latest_block_hash: to_base(&head.last_block_hash),
                latest_block_height: head.height,
                num_peers: network_info.num_active_peers,
            },
        };
        // Sign telemetry if there is a signer present.
        let content = if let Some(vs) = self.validator_signer.as_ref() {
            vs.sign_telemetry(&info)
        } else {
            serde_json::to_value(&info).expect("Telemetry must serialize to json")
        };
        telemetry(&self.telemetry_actor, content);
    }
}

fn display_sync_status(
    sync_status: &SyncStatus,
    head: &Tip,
    genesis_height: BlockHeight,
) -> String {
    match sync_status {
        SyncStatus::AwaitingPeers => format!("#{:>8} Waiting for peers", head.height),
        SyncStatus::NoSync => format!("#{:>8} {:>44}", head.height, head.last_block_hash),
        SyncStatus::HeaderSync { current_height, highest_height } => {
            let percent = if *highest_height <= genesis_height {
                0
            } else {
                (min(current_height, highest_height) - genesis_height) * 100
                    / (highest_height - genesis_height)
            };
            format!("#{:>8} Downloading headers {}%", head.height, percent)
        }
        SyncStatus::BodySync { current_height, highest_height } => {
            let percent = if *highest_height <= genesis_height {
                0
            } else {
                (current_height - genesis_height) * 100 / (highest_height - genesis_height)
            };
            format!("#{:>8} Downloading blocks {}%", head.height, percent)
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
        format!("{:.2} Kgas/s", num as f64 / 1_000.0)
    } else if num < 1_000_000_000 {
        format!("{:.2} Mgas/s", num as f64 / 1_000_000.0)
    } else if num < 1_000_000_000_000 {
        format!("{:.2} Ggas/s", num as f64 / 1_000_000_000.0)
    } else {
        format!("{:.2} Tgas/s", num as f64 / 1_000_000_000_000.0)
    }
}
