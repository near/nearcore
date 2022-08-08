use crate::{metrics, rocksdb_metrics, SyncStatus};
use actix::Addr;
use near_chain_configs::{ClientConfig, LogSummaryStyle};
use near_client_primitives::types::ShardSyncStatus;
use near_network::types::NetworkInfo;
use near_primitives::block::Tip;
use near_primitives::network::PeerId;
use near_primitives::telemetry::{
    TelemetryAgentInfo, TelemetryChainInfo, TelemetryInfo, TelemetrySystemInfo,
};
use near_primitives::time::{Clock, Instant};
use near_primitives::types::{
    AccountId, Balance, BlockHeight, EpochHeight, Gas, NumBlocks, ShardId,
};
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
    // The field can be None for testing. This allows avoiding running actix in tests.
    telemetry_actor: Option<Addr<TelemetryActor>>,
    /// Log coloring enabled
    log_summary_style: LogSummaryStyle,
    /// Timestamp of starting the client.
    pub boot_time_seconds: i64,
}

impl InfoHelper {
    pub fn new(
        telemetry_actor: Option<Addr<TelemetryActor>>,
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
            boot_time_seconds: Clock::utc().timestamp(),
        }
    }

    pub fn chunk_processed(&mut self, shard_id: ShardId, gas_used: Gas, balance_burnt: Balance) {
        metrics::TGAS_USAGE_HIST
            .with_label_values(&[&format!("{}", shard_id)])
            .observe(gas_used as f64 / TERAGAS);
        metrics::BALANCE_BURNT.inc_by(balance_burnt as f64);
    }

    pub fn chunk_skipped(&mut self, shard_id: ShardId) {
        metrics::CHUNK_SKIPPED_TOTAL.with_label_values(&[&format!("{}", shard_id)]).inc();
    }

    pub fn block_processed(
        &mut self,
        gas_used: Gas,
        num_chunks: u64,
        gas_price: Balance,
        total_supply: Balance,
        last_final_block_height: BlockHeight,
        last_final_ds_block_height: BlockHeight,
        epoch_height: EpochHeight,
    ) {
        self.num_blocks_processed += 1;
        self.num_chunks_in_blocks_processed += num_chunks;
        self.gas_used += gas_used;
        metrics::GAS_USED.inc_by(gas_used as f64);
        metrics::BLOCKS_PROCESSED.inc();
        metrics::CHUNKS_PROCESSED.inc_by(num_chunks);
        metrics::GAS_PRICE.set(gas_price as f64);
        metrics::TOTAL_SUPPLY.set(total_supply as f64);
        metrics::FINAL_BLOCK_HEIGHT.set(last_final_block_height as i64);
        metrics::FINAL_DOOMSLUG_BLOCK_HEIGHT.set(last_final_ds_block_height as i64);
        metrics::EPOCH_HEIGHT.set(epoch_height as i64);
    }

    pub fn info(
        &mut self,
        head: &Tip,
        sync_status: &SyncStatus,
        node_id: &PeerId,
        network_info: &NetworkInfo,
        validator_info: Option<ValidatorInfoHelper>,
        validator_epoch_stats: Vec<ValidatorProductionStats>,
        protocol_upgrade_block_height: BlockHeight,
        statistics: Option<StoreStatistics>,
        client_config: &ClientConfig,
    ) {
        let use_colour = matches!(self.log_summary_style, LogSummaryStyle::Colored);
        let paint = |colour: ansi_term::Colour, text: Option<String>| match text {
            None => ansi_term::Style::default().paint(""),
            Some(text) if use_colour => colour.bold().paint(text),
            Some(text) => ansi_term::Style::default().paint(text),
        };

        let s = |num| if num == 1 { "" } else { "s" };

        let sync_status_log = Some(display_sync_status(sync_status, head));

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
            let proc =
                self.sys.process(pid).expect("refresh_process succeeds, this should be not None");
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
        (metrics::CPU_USAGE.set(cpu_usage as i64));
        (metrics::MEMORY_USAGE.set((memory_usage * 1024) as i64));
        (metrics::PROTOCOL_UPGRADE_BLOCK_HEIGHT.set(protocol_upgrade_block_height as i64));

        // TODO: Deprecated.
        (metrics::BLOCKS_PER_MINUTE.set((avg_bls * (60 as f64)) as i64));
        // TODO: Deprecated.
        (metrics::CHUNKS_PER_BLOCK_MILLIS.set((1000. * chunks_per_block) as i64));
        // TODO: Deprecated.
        (metrics::AVG_TGAS_USAGE.set((avg_gas_used as f64 / TERAGAS).round() as i64));

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

        // In production `telemetry_actor` should always be available.
        if let Some(telemetry_actor) = &self.telemetry_actor {
            telemetry(
                telemetry_actor,
                self.telemetry_info(
                    head,
                    sync_status,
                    node_id,
                    network_info,
                    client_config,
                    cpu_usage,
                    memory_usage,
                    is_validator,
                ),
            );
        }
    }

    fn telemetry_info(
        &self,
        head: &Tip,
        sync_status: &SyncStatus,
        node_id: &PeerId,
        network_info: &NetworkInfo,
        client_config: &ClientConfig,
        cpu_usage: f32,
        memory_usage: u64,
        is_validator: bool,
    ) -> serde_json::Value {
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
                boot_time_seconds: self.boot_time_seconds,
            },
            chain: TelemetryChainInfo {
                node_id: node_id.to_string(),
                account_id: self.validator_signer.as_ref().map(|bp| bp.validator_id().clone()),
                is_validator,
                status: sync_status.as_variant_name().to_string(),
                latest_block_hash: head.last_block_hash.clone(),
                latest_block_height: head.height,
                num_peers: network_info.num_connected_peers,
                block_production_tracking_delay: client_config
                    .block_production_tracking_delay
                    .as_secs_f64(),
                min_block_production_delay: client_config.min_block_production_delay.as_secs_f64(),
                max_block_production_delay: client_config.max_block_production_delay.as_secs_f64(),
                max_block_wait_delay: client_config.max_block_wait_delay.as_secs_f64(),
            },
            extra_info: serde_json::to_string(&extra_telemetry_info(client_config)).unwrap(),
        };
        // Sign telemetry if there is a signer present.
        if let Some(vs) = self.validator_signer.as_ref() {
            vs.sign_telemetry(&info)
        } else {
            serde_json::to_value(&info).expect("Telemetry must serialize to json")
        }
    }
}

fn extra_telemetry_info(client_config: &ClientConfig) -> serde_json::Value {
    serde_json::json!({
        "block_production_tracking_delay":  client_config.block_production_tracking_delay.as_secs_f64(),
        "min_block_production_delay":  client_config.min_block_production_delay.as_secs_f64(),
        "max_block_production_delay": client_config.max_block_production_delay.as_secs_f64(),
        "max_block_wait_delay": client_config.max_block_wait_delay.as_secs_f64(),
    })
}

pub fn display_sync_status(sync_status: &SyncStatus, head: &Tip) -> String {
    metrics::SYNC_STATUS.set(sync_status.repr() as i64);
    match sync_status {
        SyncStatus::AwaitingPeers => format!("#{:>8} Waiting for peers", head.height),
        SyncStatus::NoSync => format!("#{:>8} {:>44}", head.height, head.last_block_hash),
        SyncStatus::EpochSync { epoch_ord } => {
            format!("[EPOCH: {:>5}] Getting to a recent epoch", epoch_ord)
        }
        SyncStatus::HeaderSync { start_height, current_height, highest_height } => {
            let percent = if highest_height <= start_height {
                0.0
            } else {
                (((min(current_height, highest_height) - start_height) * 100) as f64)
                    / ((highest_height - start_height) as f64)
            };
            format!(
                "#{:>8} Downloading headers {:.2}% ({} left; at {})",
                head.height,
                percent,
                highest_height - current_height,
                current_height
            )
        }
        SyncStatus::BodySync { start_height, current_height, highest_height } => {
            let percent = if highest_height <= start_height {
                0.0
            } else {
                ((current_height - start_height) * 100) as f64
                    / ((highest_height - start_height) as f64)
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

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use near_chain::test_utils::{KeyValueRuntime, ValidatorSchedule};
    use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
    use near_network::test_utils::peer_id_from_seed;
    use near_primitives::version::PROTOCOL_VERSION;
    use num_rational::Ratio;

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

    #[test]
    fn telemetry_info() {
        let config = ClientConfig::test(false, 1230, 2340, 50, false, true);
        let info_helper = InfoHelper::new(None, &config, None);

        let store = near_store::test_utils::create_test_store();
        let vs =
            ValidatorSchedule::new().block_producers_per_epoch(vec![vec!["test".parse().unwrap()]]);
        let runtime =
            Arc::new(KeyValueRuntime::new_with_validators_and_no_gc(store, vs, 123, false));
        let chain_genesis = ChainGenesis {
            time: Clock::utc(),
            height: 0,
            gas_limit: 1_000_000,
            min_gas_price: 100,
            max_gas_price: 1_000_000_000,
            total_supply: 3_000_000_000_000_000_000_000_000_000_000_000,
            gas_price_adjustment_rate: Ratio::from_integer(0),
            transaction_validity_period: 123123,
            epoch_length: 123,
            protocol_version: PROTOCOL_VERSION,
        };
        let doomslug_threshold_mode = DoomslugThresholdMode::TwoThirds;
        let chain =
            Chain::new(runtime.clone(), &chain_genesis, doomslug_threshold_mode, true).unwrap();

        let telemetry = info_helper.telemetry_info(
            &chain.head().unwrap(),
            &SyncStatus::AwaitingPeers,
            &peer_id_from_seed("zxc"),
            &NetworkInfo {
                connected_peers: vec![],
                num_connected_peers: 0,
                peer_max_count: 0,
                highest_height_peers: vec![],
                sent_bytes_per_sec: 0,
                received_bytes_per_sec: 0,
                known_producers: vec![],
                peer_counter: 0,
                tier1_accounts: vec![],
            },
            &config,
            0.0,
            0,
            false,
        );
        println!("Got telemetry info: {:?}", telemetry);
        assert_matches!(
            telemetry["extra_info"].as_str().unwrap().find("\"max_block_production_delay\":2.34,"),
            Some(_)
        );
    }
}
