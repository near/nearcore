use crate::{metrics, rocksdb_metrics, SyncStatus};
use actix::Addr;
use near_chain_configs::{ClientConfig, LogSummaryStyle};
use near_client_primitives::types::ShardSyncStatus;
use near_network::types::NetworkInfo;
use near_primitives::block::Tip;
use near_primitives::network::PeerId;
use near_primitives::serialize::to_base;
use near_primitives::telemetry::{
    TelemetryAgentInfo, TelemetryChainInfo, TelemetryInfo, TelemetrySystemInfo,
};
use near_primitives::time::{Clock, Instant};
use near_primitives::types::{AccountId, BlockHeight, EpochHeight, Gas, NumBlocks, ShardId};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::Version;
use near_primitives::views::{CurrentEpochValidatorInfo, EpochValidatorInfo, ValidatorKickoutView};
use near_store::db::StoreStatistics;
use near_telemetry::{telemetry, TelemetryActor};
use std::cmp::min;
use std::fmt::Write;
use std::sync::Arc;
use sysinfo::{get_current_pid, set_open_files_limit, Pid, ProcessExt, System, SystemExt};
use tracing::info;

const TERAGAS: f64 = 1_000_000_000_000_f64;

pub struct ValidatorInfoHelper {
    pub is_validator: bool,
    pub num_validators: usize,
}

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
    /// Total number of blocks processed.
    num_chunks_in_blocks_processed: u64,
    /// Total gas used during period.
    gas_used: u64,
    /// Sign telemetry with block producer key if available.
    validator_signer: Option<Arc<dyn ValidatorSigner>>,
    /// Telemetry actor.
    telemetry_actor: Addr<TelemetryActor>,
    /// Log coloring enabled
    log_summary_style: LogSummaryStyle,
}

impl InfoHelper {
    pub fn new(
        telemetry_actor: Addr<TelemetryActor>,
        client_config: &ClientConfig,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
    ) -> Self {
        set_open_files_limit(0);
        metrics::export_version(&client_config.version);
        InfoHelper {
            nearcore_version: client_config.version.clone(),
            sys: System::new(),
            pid: get_current_pid().ok(),
            started: Clock::instant(),
            num_blocks_processed: 0,
            num_chunks_in_blocks_processed: 0,
            gas_used: 0,
            telemetry_actor,
            validator_signer,
            log_summary_style: client_config.log_summary_style,
        }
    }

    pub fn chunk_processed(&mut self, shard_id: ShardId, gas_used: Gas) {
        metrics::TGAS_USAGE_HIST
            .with_label_values(&[&format!("{}", shard_id)])
            .observe(gas_used as f64 / TERAGAS);
    }

    pub fn chunk_skipped(&mut self, shard_id: ShardId) {
        metrics::CHUNK_SKIPPED_TOTAL.with_label_values(&[&format!("{}", shard_id)]).inc();
    }

    pub fn block_processed(&mut self, gas_used: Gas, num_chunks: u64) {
        self.num_blocks_processed += 1;
        self.num_chunks_in_blocks_processed += num_chunks;
        self.gas_used += gas_used;
    }

    pub fn info(
        &mut self,
        genesis_height: BlockHeight,
        head: &Tip,
        sync_status: &SyncStatus,
        node_id: &PeerId,
        network_info: &NetworkInfo,
        validator_info: Option<ValidatorInfoHelper>,
        validator_epoch_stats: Vec<ValidatorProductionStats>,
        epoch_height: EpochHeight,
        protocol_upgrade_block_height: BlockHeight,
        statistics: Option<StoreStatistics>,
    ) {
        let use_colour = matches!(self.log_summary_style, LogSummaryStyle::Colored);
        let paint = |colour: ansi_term::Colour, text: Option<String>| match text {
            None => ansi_term::Style::default().paint(""),
            Some(text) if use_colour => colour.bold().paint(text),
            Some(text) => ansi_term::Style::default().paint(text),
        };

        let s = |num| if num == 1 { "" } else { "s" };

        let sync_status_log = Some(display_sync_status(sync_status, head, genesis_height));

        let validator_info_log = validator_info.as_ref().map(|info| {
            format!(
                " {}{} validator{}",
                if info.is_validator { "Validator | " } else { "" },
                info.num_validators,
                s(info.num_validators)
            )
        });

        let network_info_log = Some(format!(
            " {} peer{} ⬇ {} ⬆ {}",
            network_info.num_connected_peers,
            s(network_info.num_connected_peers),
            PrettyNumber::bytes_per_sec(network_info.received_bytes_per_sec),
            PrettyNumber::bytes_per_sec(network_info.sent_bytes_per_sec)
        ));

        let avg_bls = (self.num_blocks_processed as f64)
            / (self.started.elapsed().as_millis() as f64)
            * 1000.0;
        let chunks_per_block = if self.num_blocks_processed > 0 {
            (self.num_chunks_in_blocks_processed as f64) / (self.num_blocks_processed as f64)
        } else {
            0.
        };
        let avg_gas_used =
            ((self.gas_used as f64) / (self.started.elapsed().as_millis() as f64) * 1000.0) as u64;
        let blocks_info_log =
            Some(format!(" {:.2} bps {}", avg_bls, PrettyNumber::gas_per_sec(avg_gas_used)));

        let proc_info = self.pid.filter(|pid| self.sys.refresh_process(*pid)).map(|pid| {
            let proc = self
                .sys
                .get_process(pid)
                .expect("refresh_process succeeds, this should be not None");
            (proc.cpu_usage(), proc.memory())
        });
        let machine_info_log = proc_info.as_ref().map(|(cpu, mem)| {
            format!(" CPU: {:.0}%, Mem: {}", cpu, PrettyNumber::bytes(mem * 1024))
        });

        info!(
            target: "stats", "{}{}{}{}{}",
            paint(ansi_term::Colour::Yellow, sync_status_log),
            paint(ansi_term::Colour::White, validator_info_log),
            paint(ansi_term::Colour::Cyan, network_info_log),
            paint(ansi_term::Colour::Green, blocks_info_log),
            paint(ansi_term::Colour::Blue, machine_info_log),
        );
        if let Some(statistics) = statistics {
            rocksdb_metrics::export_stats_as_metrics(statistics);
        }

        let (cpu_usage, memory_usage) = proc_info.unwrap_or_default();
        let is_validator = validator_info.map(|v| v.is_validator).unwrap_or_default();
        (metrics::IS_VALIDATOR.set(is_validator as i64));
        (metrics::RECEIVED_BYTES_PER_SECOND.set(network_info.received_bytes_per_sec as i64));
        (metrics::SENT_BYTES_PER_SECOND.set(network_info.sent_bytes_per_sec as i64));
        (metrics::BLOCKS_PER_MINUTE.set((avg_bls * (60 as f64)) as i64));
        (metrics::CHUNKS_PER_BLOCK_MILLIS.set((1000. * chunks_per_block) as i64));
        (metrics::CPU_USAGE.set(cpu_usage as i64));
        (metrics::MEMORY_USAGE.set((memory_usage * 1024) as i64));
        (metrics::AVG_TGAS_USAGE.set((avg_gas_used as f64 / TERAGAS).round() as i64));
        (metrics::EPOCH_HEIGHT.set(epoch_height as i64));
        (metrics::PROTOCOL_UPGRADE_BLOCK_HEIGHT.set(protocol_upgrade_block_height as i64));

        // In case we can't get the list of validators for the current and the previous epoch,
        // skip updating the per-validator metrics.
        // Note that the metrics are set to 0 for previous epoch validators who are no longer
        // validators.
        for stats in validator_epoch_stats {
            (metrics::VALIDATORS_BLOCKS_PRODUCED
                .with_label_values(&[stats.account_id.as_str()])
                .set(stats.num_produced_blocks as i64));
            (metrics::VALIDATORS_BLOCKS_EXPECTED
                .with_label_values(&[stats.account_id.as_str()])
                .set(stats.num_expected_blocks as i64));
            (metrics::VALIDATORS_CHUNKS_PRODUCED
                .with_label_values(&[stats.account_id.as_str()])
                .set(stats.num_produced_chunks as i64));
            (metrics::VALIDATORS_CHUNKS_EXPECTED
                .with_label_values(&[stats.account_id.as_str()])
                .set(stats.num_expected_chunks as i64));
        }

        self.started = Clock::instant();
        self.num_blocks_processed = 0;
        self.num_chunks_in_blocks_processed = 0;
        self.gas_used = 0;

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
                account_id: self.validator_signer.as_ref().map(|bp| bp.validator_id().clone()),
                is_validator,
                status: sync_status.as_variant_name().to_string(),
                latest_block_hash: to_base(&head.last_block_hash),
                latest_block_height: head.height,
                num_peers: network_info.num_connected_peers,
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

pub fn display_sync_status(
    sync_status: &SyncStatus,
    head: &Tip,
    genesis_height: BlockHeight,
) -> String {
    metrics::SYNC_STATUS.set(sync_status.repr() as i64);
    match sync_status {
        SyncStatus::AwaitingPeers => format!("#{:>8} Waiting for peers", head.height),
        SyncStatus::NoSync => format!("#{:>8} {:>44}", head.height, head.last_block_hash),
        SyncStatus::EpochSync { epoch_ord } => {
            format!("[EPOCH: {:>5}] Getting to a recent epoch", epoch_ord)
        }
        SyncStatus::HeaderSync { current_height, highest_height } => {
            let percent = if *highest_height <= genesis_height {
                0.0
            } else {
                (((min(current_height, highest_height) - genesis_height) * 100) as f64)
                    / ((highest_height - genesis_height) as f64)
            };
            format!(
                "#{:>8} Downloading headers {:.2}% ({} left; at {})",
                head.height,
                percent,
                highest_height - current_height,
                current_height
            )
        }
        SyncStatus::BodySync { current_height, highest_height } => {
            let percent = if *highest_height <= genesis_height {
                0.0
            } else {
                ((current_height - genesis_height) * 100) as f64
                    / ((highest_height - genesis_height) as f64)
            };
            format!(
                "#{:>8} Downloading blocks {:.2}% ({} left; at {})",
                head.height,
                percent,
                highest_height - current_height,
                current_height
            )
        }
        SyncStatus::StateSync(sync_hash, shard_statuses) => {
            let mut res = format!("State {:?}", sync_hash);
            let mut shard_statuses: Vec<_> = shard_statuses.iter().collect();
            shard_statuses.sort_by_key(|(shard_id, _)| *shard_id);
            for (shard_id, shard_status) in shard_statuses {
                write!(
                    res,
                    "[{}: {}]",
                    shard_id,
                    match shard_status.status {
                        ShardSyncStatus::StateDownloadHeader => "header",
                        ShardSyncStatus::StateDownloadParts => "parts",
                        ShardSyncStatus::StateDownloadScheduling => "scheduling",
                        ShardSyncStatus::StateDownloadApplying => "applying",
                        ShardSyncStatus::StateDownloadComplete => "download complete",
                        ShardSyncStatus::StateSplitScheduling => "split scheduling",
                        ShardSyncStatus::StateSplitApplying => "split applying",
                        ShardSyncStatus::StateSyncDone => "done",
                    }
                )
                .unwrap();
            }
            res
        }
        SyncStatus::StateSyncDone => format!("State sync done"),
    }
}

/// Format number using SI prefixes.
struct PrettyNumber(u64, &'static str);

impl PrettyNumber {
    fn bytes_per_sec(bps: u64) -> Self {
        Self(bps, "B/s")
    }

    fn bytes(bytes: u64) -> Self {
        Self(bytes, "B")
    }

    fn gas_per_sec(gps: u64) -> Self {
        Self(gps, "gas/s")
    }
}

impl std::fmt::Display for PrettyNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(mut num, unit) = self;
        if num < 1_000 {
            return write!(f, "{} {}", num, unit);
        }
        for prefix in b"kMGTPE" {
            if num < 1_000_000 {
                let precision = if num < 10_000 {
                    2
                } else if num < 100_000 {
                    1
                } else {
                    0
                };
                return write!(
                    f,
                    "{:.*} {}{}",
                    precision,
                    num as f64 / 1_000.0,
                    *prefix as char,
                    unit
                );
            }
            num /= 1000;
        }
        unreachable!()
    }
}

#[test]
fn test_pretty_number() {
    for (want, num) in [
        ("0 U", 0),
        ("1 U", 1),
        ("10 U", 10),
        ("100 U", 100),
        ("1.00 kU", 1_000),
        ("10.0 kU", 10_000),
        ("100 kU", 100_000),
        ("1.00 MU", 1_000_000),
        ("10.0 MU", 10_000_000),
        ("100 MU", 100_000_000),
        ("18.4 EU", u64::MAX),
    ] {
        let got = PrettyNumber(num, "U").to_string();
        assert_eq!(want, &got, "num={}", num);
    }
}

/// Number of blocks and chunks produced and expected by a certain validator.
pub struct ValidatorProductionStats {
    pub account_id: AccountId,
    pub num_produced_blocks: NumBlocks,
    pub num_expected_blocks: NumBlocks,
    pub num_produced_chunks: NumBlocks,
    pub num_expected_chunks: NumBlocks,
}

impl ValidatorProductionStats {
    pub fn kickout(kickout: ValidatorKickoutView) -> Self {
        Self {
            account_id: kickout.account_id,
            num_produced_blocks: 0,
            num_expected_blocks: 0,
            num_produced_chunks: 0,
            num_expected_chunks: 0,
        }
    }
    pub fn validator(info: CurrentEpochValidatorInfo) -> Self {
        Self {
            account_id: info.account_id,
            num_produced_blocks: info.num_produced_blocks,
            num_expected_blocks: info.num_expected_blocks,
            num_produced_chunks: info.num_produced_chunks,
            num_expected_chunks: info.num_expected_chunks,
        }
    }
}

/// Converts EpochValidatorInfo into a vector of ValidatorProductionStats.
pub fn get_validator_epoch_stats(
    current_validator_epoch_info: EpochValidatorInfo,
) -> Vec<ValidatorProductionStats> {
    let mut stats = vec![];
    // Record kickouts to replace latest stats of kicked out validators with zeros.
    for kickout in current_validator_epoch_info.prev_epoch_kickout {
        stats.push(ValidatorProductionStats::kickout(kickout));
    }
    for validator in current_validator_epoch_info.current_validators {
        stats.push(ValidatorProductionStats::validator(validator));
    }
    stats
}
