use crate::config_updater::ConfigUpdater;
use crate::{metrics, SyncStatus};
use actix::Addr;
use itertools::Itertools;
use near_chain_configs::{ClientConfig, LogSummaryStyle, SyncConfig};
use near_client_primitives::types::StateSyncStatus;
use near_network::types::NetworkInfo;
use near_primitives::block::Tip;
use near_primitives::network::PeerId;
use near_primitives::static_clock::StaticClock;
use near_primitives::telemetry::{
    TelemetryAgentInfo, TelemetryChainInfo, TelemetryInfo, TelemetrySystemInfo,
};
use near_primitives::types::{
    AccountId, Balance, BlockHeight, EpochHeight, EpochId, Gas, NumBlocks, ShardId, ValidatorId,
    ValidatorInfoIdentifier,
};
use near_primitives::unwrap_or_return;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::Version;
use near_primitives::views::{
    CatchupStatusView, ChunkProcessingStatus, CurrentEpochValidatorInfo, EpochValidatorInfo,
    ValidatorKickoutView,
};
use near_telemetry::{telemetry, TelemetryActor};
use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;
use std::time::Instant;
use sysinfo::{get_current_pid, set_open_files_limit, Pid, ProcessExt, System, SystemExt};
use tracing::info;

const TERAGAS: f64 = 1_000_000_000_000_f64;

struct ValidatorInfoHelper {
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
    /// Log coloring enabled.
    log_summary_style: LogSummaryStyle,
    /// Epoch id.
    epoch_id: Option<EpochId>,
    /// Timestamp of starting the client.
    pub boot_time_seconds: i64,
    // Allows more detailed logging, for example a list of orphaned blocks.
    enable_multiline_logging: bool,
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
            started: StaticClock::instant(),
            num_blocks_processed: 0,
            num_chunks_in_blocks_processed: 0,
            gas_used: 0,
            telemetry_actor,
            validator_signer,
            log_summary_style: client_config.log_summary_style,
            boot_time_seconds: StaticClock::utc().timestamp(),
            epoch_id: None,
            enable_multiline_logging: client_config.enable_multiline_logging,
        }
    }

    pub fn chunk_processed(&mut self, shard_id: ShardId, gas_used: Gas, balance_burnt: Balance) {
        metrics::TGAS_USAGE_HIST
            .with_label_values(&[&shard_id.to_string()])
            .observe(gas_used as f64 / TERAGAS);
        metrics::BALANCE_BURNT.inc_by(balance_burnt as f64);
    }

    pub fn chunk_skipped(&mut self, shard_id: ShardId) {
        metrics::CHUNK_SKIPPED_TOTAL.with_label_values(&[&shard_id.to_string()]).inc();
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
        last_final_block_height_in_epoch: Option<BlockHeight>,
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
        if let Some(last_final_block_height_in_epoch) = last_final_block_height_in_epoch {
            // In rare cases cases the final height isn't updated, for example right after a state sync.
            // Don't update the metric in such cases.
            metrics::FINAL_BLOCK_HEIGHT_IN_EPOCH.set(last_final_block_height_in_epoch as i64);
        }
    }

    /// Count which shards are tracked by the node in the epoch indicated by head parameter.
    fn record_tracked_shards(head: &Tip, client: &crate::client::Client) {
        let me = client.validator_signer.as_ref().map(|x| x.validator_id());
        if let Ok(num_shards) = client.epoch_manager.num_shards(&head.epoch_id) {
            for shard_id in 0..num_shards {
                let tracked = client.shard_tracker.care_about_shard(
                    me,
                    &head.last_block_hash,
                    shard_id,
                    true,
                );
                metrics::TRACKED_SHARDS
                    .with_label_values(&[&shard_id.to_string()])
                    .set(if tracked { 1 } else { 0 });
            }
        }
    }

    fn record_block_producers(head: &Tip, client: &crate::client::Client) {
        let me = client.validator_signer.as_ref().map(|x| x.validator_id().clone());
        if let Some(is_bp) = me.map_or(Some(false), |account_id| {
            // In rare cases block producer information isn't available.
            // Don't set the metric in this case.
            client
                .epoch_manager
                .get_epoch_block_producers_ordered(&head.epoch_id, &head.last_block_hash)
                .map_or(None, |bp| Some(bp.iter().any(|bp| bp.0.account_id() == &account_id)))
        }) {
            metrics::IS_BLOCK_PRODUCER.set(if is_bp { 1 } else { 0 });
        }
    }

    fn record_chunk_producers(head: &Tip, client: &crate::client::Client) {
        if let (Some(account_id), Ok(epoch_info)) = (
            client.validator_signer.as_ref().map(|x| x.validator_id().clone()),
            client.epoch_manager.get_epoch_info(&head.epoch_id),
        ) {
            for (shard_id, validators) in epoch_info.chunk_producers_settlement().iter().enumerate()
            {
                let is_chunk_producer_for_shard = validators.iter().any(|&validator_id| {
                    *epoch_info.validator_account_id(validator_id) == account_id
                });
                metrics::IS_CHUNK_PRODUCER_FOR_SHARD
                    .with_label_values(&[&shard_id.to_string()])
                    .set(if is_chunk_producer_for_shard { 1 } else { 0 });
            }
        } else if let Ok(num_shards) = client.epoch_manager.num_shards(&head.epoch_id) {
            for shard_id in 0..num_shards {
                metrics::IS_CHUNK_PRODUCER_FOR_SHARD
                    .with_label_values(&[&shard_id.to_string()])
                    .set(0);
            }
        }
    }

    /// The value obtained by multiplying the stake fraction with the expected number of blocks in an epoch
    /// is an estimation, and not an exact value. To obtain a more precise result, it is necessary to examine
    /// all the blocks in the epoch. However, even this method may not be completely accurate because additional
    /// blocks could potentially be added at the end of the epoch.
    fn record_epoch_settlement_info(head: &Tip, client: &crate::client::Client) {
        let epoch_info = client.epoch_manager.get_epoch_info(&head.epoch_id);
        let blocks_in_epoch = client.config.epoch_length;
        let number_of_shards = client.epoch_manager.num_shards(&head.epoch_id).unwrap_or_default();
        if let Ok(epoch_info) = epoch_info {
            metrics::VALIDATORS_CHUNKS_EXPECTED_IN_EPOCH.reset();
            metrics::VALIDATORS_BLOCKS_EXPECTED_IN_EPOCH.reset();
            metrics::BLOCK_PRODUCER_STAKE.reset();

            let epoch_height = epoch_info.epoch_height().to_string();

            let mut stake_per_bp = HashMap::<ValidatorId, Balance>::new();

            let stake_to_blocks = |stake: Balance, stake_sum: Balance| -> i64 {
                if stake == 0 {
                    0
                } else {
                    (((stake as f64) / (stake_sum as f64)) * (blocks_in_epoch as f64)) as i64
                }
            };

            let mut stake_sum = 0;
            for &id in epoch_info.block_producers_settlement() {
                let stake = epoch_info.validator_stake(id);
                stake_per_bp.insert(id, stake);
                stake_sum += stake;
            }

            stake_per_bp.iter().for_each(|(&id, &stake)| {
                metrics::BLOCK_PRODUCER_STAKE
                    .with_label_values(&[
                        epoch_info.get_validator(id).account_id().as_str(),
                        &epoch_height,
                    ])
                    .set((stake / 1e24 as u128) as i64);
                metrics::VALIDATORS_BLOCKS_EXPECTED_IN_EPOCH
                    .with_label_values(&[
                        epoch_info.get_validator(id).account_id().as_str(),
                        &epoch_height,
                    ])
                    .set(stake_to_blocks(stake, stake_sum))
            });

            for shard_id in 0..number_of_shards {
                let mut stake_per_cp = HashMap::<ValidatorId, Balance>::new();
                stake_sum = 0;
                for &id in &epoch_info.chunk_producers_settlement()[shard_id as usize] {
                    let stake = epoch_info.validator_stake(id);
                    stake_per_cp.insert(id, stake);
                    stake_sum += stake;
                }

                stake_per_cp.iter().for_each(|(&id, &stake)| {
                    metrics::VALIDATORS_CHUNKS_EXPECTED_IN_EPOCH
                        .with_label_values(&[
                            epoch_info.get_validator(id).account_id().as_str(),
                            &shard_id.to_string(),
                            &epoch_height,
                        ])
                        .set(stake_to_blocks(stake, stake_sum))
                });
            }
        }
    }

    /// Records protocol version of the current epoch.
    fn record_protocol_version(head: &Tip, client: &crate::client::Client) {
        if let Ok(version) = client.epoch_manager.get_epoch_protocol_version(&head.epoch_id) {
            metrics::CURRENT_PROTOCOL_VERSION.set(version as i64);
        }
    }

    /// Print current summary.
    pub fn log_summary(
        &mut self,
        client: &crate::client::Client,
        node_id: &PeerId,
        network_info: &NetworkInfo,
        config_updater: &Option<ConfigUpdater>,
    ) {
        let is_syncing = client.sync_status.is_syncing();
        let head = unwrap_or_return!(client.chain.head());
        let validator_info = if !is_syncing {
            let validators = unwrap_or_return!(client
                .epoch_manager
                .get_epoch_block_producers_ordered(&head.epoch_id, &head.last_block_hash));
            let num_validators = validators.len();
            let account_id = client.validator_signer.as_ref().map(|x| x.validator_id());
            let is_validator = if let Some(account_id) = account_id {
                match client.epoch_manager.get_validator_by_account_id(
                    &head.epoch_id,
                    &head.last_block_hash,
                    account_id,
                ) {
                    Ok((_, is_slashed)) => !is_slashed,
                    Err(_) => false,
                }
            } else {
                false
            };
            Some(ValidatorInfoHelper { is_validator, num_validators })
        } else {
            None
        };

        let header_head = unwrap_or_return!(client.chain.header_head());
        let validator_epoch_stats = if is_syncing {
            // EpochManager::get_validator_info method (which is what runtime
            // adapter calls) is expensive when node is syncing so we’re simply
            // not collecting the statistics.  The statistics are used to update
            // a few Prometheus metrics only so we prefer to leave the metrics
            // unset until node finishes synchronising.  TODO(#6763): If we
            // manage to get get_validator_info fasts again (or return an error
            // if computation would be too slow), remove the ‘if is_syncing’
            // check.
            Default::default()
        } else {
            let epoch_identifier = ValidatorInfoIdentifier::BlockHash(header_head.last_block_hash);
            client
                .epoch_manager
                .get_validator_info(epoch_identifier)
                .map(get_validator_epoch_stats)
                .unwrap_or_default()
        };

        InfoHelper::record_tracked_shards(&head, &client);
        InfoHelper::record_block_producers(&head, &client);
        InfoHelper::record_chunk_producers(&head, &client);
        let next_epoch_id = Some(head.epoch_id.clone());
        if self.epoch_id.ne(&next_epoch_id) {
            // We only want to compute this once per epoch to avoid heavy computational work, that can last up to 100ms.
            InfoHelper::record_epoch_settlement_info(&head, &client);
            // This isn't heavy computationally.
            InfoHelper::record_protocol_version(&head, &client);

            self.epoch_id = next_epoch_id;
        }

        self.info(
            &head,
            &client.sync_status,
            client.get_catchup_status().unwrap_or_default(),
            node_id,
            network_info,
            validator_info,
            validator_epoch_stats,
            client
                .epoch_manager
                .get_estimated_protocol_upgrade_block_height(head.last_block_hash)
                .unwrap_or(None)
                .unwrap_or(0),
            &client.config,
            config_updater,
        );
        self.log_chain_processing_info(client, &head.epoch_id);
    }

    fn info(
        &mut self,
        head: &Tip,
        sync_status: &SyncStatus,
        catchup_status: Vec<CatchupStatusView>,
        node_id: &PeerId,
        network_info: &NetworkInfo,
        validator_info: Option<ValidatorInfoHelper>,
        validator_epoch_stats: Vec<ValidatorProductionStats>,
        protocol_upgrade_block_height: BlockHeight,
        client_config: &ClientConfig,
        config_updater: &Option<ConfigUpdater>,
    ) {
        let use_colour = matches!(self.log_summary_style, LogSummaryStyle::Colored);
        let paint = |colour: ansi_term::Colour, text: Option<String>| match text {
            None => ansi_term::Style::default().paint(""),
            Some(text) if use_colour => colour.bold().paint(text),
            Some(text) => ansi_term::Style::default().paint(text),
        };

        let s = |num| if num == 1 { "" } else { "s" };

        let sync_status_log =
            Some(display_sync_status(sync_status, head, &client_config.state_sync.sync));
        let catchup_status_log = display_catchup_status(catchup_status);
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
        if !catchup_status_log.is_empty() {
            info!(target: "stats", "Catchups\n{}", catchup_status_log);
        }
        if let Some(config_updater) = &config_updater {
            config_updater.report_status();
        }
        let (cpu_usage, memory_usage) = proc_info.unwrap_or_default();
        let is_validator = validator_info.map(|v| v.is_validator).unwrap_or_default();
        (metrics::IS_VALIDATOR.set(is_validator as i64));
        (metrics::RECEIVED_BYTES_PER_SECOND.set(network_info.received_bytes_per_sec as i64));
        (metrics::SENT_BYTES_PER_SECOND.set(network_info.sent_bytes_per_sec as i64));
        (metrics::CPU_USAGE.set(cpu_usage as i64));
        (metrics::MEMORY_USAGE.set((memory_usage * 1024) as i64));
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
            for ((shard, expected), produced) in stats
                .shards
                .iter()
                .zip(stats.num_expected_chunks_per_shard.iter())
                .zip(stats.num_produced_chunks_per_shard.iter())
            {
                (metrics::VALIDATORS_CHUNKS_EXPECTED_BY_SHARD
                    .with_label_values(&[stats.account_id.as_str(), &shard.to_string()])
                    .set(*expected as i64));
                (metrics::VALIDATORS_CHUNKS_PRODUCED_BY_SHARD
                    .with_label_values(&[stats.account_id.as_str(), &shard.to_string()])
                    .set(*produced as i64));
            }
        }

        self.started = StaticClock::instant();
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
                latest_block_hash: head.last_block_hash,
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

    fn log_chain_processing_info(&mut self, client: &crate::Client, epoch_id: &EpochId) {
        let chain = &client.chain;
        let use_colour = matches!(self.log_summary_style, LogSummaryStyle::Colored);
        let info = chain.get_chain_processing_info();
        let blocks_info = BlocksInfo { blocks_info: info.blocks_info, use_colour };
        tracing::debug!(
            target: "stats",
            "{:?} Orphans: {} With missing chunks: {} In processing {}{}",
            epoch_id,
            info.num_orphans,
            info.num_blocks_missing_chunks,
            info.num_blocks_in_processing,
            if self.enable_multiline_logging { blocks_info.to_string() } else { "".to_owned() },
        );
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

pub fn display_catchup_status(catchup_status: Vec<CatchupStatusView>) -> String {
    catchup_status
        .into_iter()
        .map(|catchup_status| {
            let shard_sync_string = catchup_status
                .shard_sync_status
                .iter()
                .sorted_by_key(|x| x.0)
                .map(|(shard_id, status_string)| format!("Shard {} {}", shard_id, status_string))
                .join(", ");
            let block_catchup_string = if !catchup_status.blocks_to_catchup.is_empty() {
                "done".to_string()
            } else {
                catchup_status
                    .blocks_to_catchup
                    .iter()
                    .map(|block_view| format!("{:?}@{:?}", block_view.hash, block_view.height))
                    .join(", ")
            };
            format!(
                "Sync block {:?}@{:?} \nShard sync status: {}\nNext blocks to catch up: {}",
                catchup_status.sync_block_hash,
                catchup_status.sync_block_height,
                shard_sync_string,
                block_catchup_string,
            )
        })
        .join("\n")
}

pub fn display_sync_status(
    sync_status: &SyncStatus,
    head: &Tip,
    state_sync_config: &SyncConfig,
) -> String {
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
        SyncStatus::StateSync(StateSyncStatus { sync_hash, sync_status: shard_statuses }) => {
            let mut res = format!("State {:?}", sync_hash);
            let mut shard_statuses: Vec<_> = shard_statuses.iter().collect();
            shard_statuses.sort_by_key(|(shard_id, _)| *shard_id);
            for (shard_id, shard_status) in shard_statuses {
                write!(res, "[{}: {}]", shard_id, shard_status.status.to_string(),).unwrap();
            }
            if matches!(state_sync_config, SyncConfig::Peers) {
                // TODO #8719
                tracing::warn!(
                    target: "stats",
                    "The node is syncing its State. The current implementation of this mechanism is known to be unreliable. It may never complete, or fail randomly and corrupt the DB.\n\
                     Suggestions:\n\
                     * Download a recent data snapshot and restart the node.\n\
                     * Disable state sync in the config. Add `\"state_sync_enabled\": false` to `config.json`.\n\
                     \n\
                     A better implementation of State Sync is work in progress.");
            }
            res
        }
        SyncStatus::StateSyncDone => "State sync done".to_string(),
    }
}

/// Displays ` {} for {}ms` if second item is `Some`.
struct FormatMillis(&'static str, Option<u128>);

impl std::fmt::Display for FormatMillis {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.1.map_or(Ok(()), |ms| write!(fmt, " {} for {ms}ms", self.0))
    }
}

/// Formats information about each block.  Each information line is *preceded*
/// by a new line character.  There’s no final new line character.  This is
/// meant to be used in logging where final new line is not desired.
struct BlocksInfo {
    blocks_info: Vec<near_primitives::views::BlockProcessingInfo>,
    use_colour: bool,
}

impl std::fmt::Display for BlocksInfo {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let paint = |colour: ansi_term::Colour, text: String| {
            if self.use_colour {
                colour.bold().paint(text)
            } else {
                ansi_term::Style::default().paint(text)
            }
        };

        for block_info in self.blocks_info.iter() {
            let mut all_chunks_received = true;
            let chunk_status = block_info
                .chunks_info
                .iter()
                .map(|chunk_info| {
                    if let Some(chunk_info) = chunk_info {
                        all_chunks_received &=
                            matches!(chunk_info.status, ChunkProcessingStatus::Completed);
                        match chunk_info.status {
                            ChunkProcessingStatus::Completed => '✔',
                            ChunkProcessingStatus::Requested => '⬇',
                            ChunkProcessingStatus::NeedToRequest => '.',
                        }
                    } else {
                        'X'
                    }
                })
                .collect::<String>();

            let chunk_status_color = if all_chunks_received {
                ansi_term::Colour::Green
            } else {
                ansi_term::Colour::White
            };

            let chunk_status = paint(chunk_status_color, chunk_status);
            let in_progress = FormatMillis("in progress", Some(block_info.in_progress_ms));
            let in_orphan = FormatMillis("orphan", block_info.orphaned_ms);
            let missing_chunks = FormatMillis("missing chunks", block_info.missing_chunks_ms);

            write!(
                fmt,
                "\n  {} {} {:?}{in_progress}{in_orphan}{missing_chunks} Chunks:({chunk_status}))",
                block_info.height, block_info.hash, block_info.block_status,
            )?;
        }

        Ok(())
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
    pub shards: Vec<ShardId>,
    pub num_produced_chunks_per_shard: Vec<NumBlocks>,
    pub num_expected_chunks_per_shard: Vec<NumBlocks>,
}

impl ValidatorProductionStats {
    pub fn kickout(kickout: ValidatorKickoutView) -> Self {
        Self {
            account_id: kickout.account_id,
            num_produced_blocks: 0,
            num_expected_blocks: 0,
            num_produced_chunks: 0,
            num_expected_chunks: 0,
            shards: vec![],
            num_produced_chunks_per_shard: vec![],
            num_expected_chunks_per_shard: vec![],
        }
    }
    pub fn validator(info: CurrentEpochValidatorInfo) -> Self {
        Self {
            account_id: info.account_id,
            num_produced_blocks: info.num_produced_blocks,
            num_expected_blocks: info.num_expected_blocks,
            num_produced_chunks: info.num_produced_chunks,
            num_expected_chunks: info.num_expected_chunks,
            shards: info.shards,
            num_produced_chunks_per_shard: info.num_produced_chunks_per_shard,
            num_expected_chunks_per_shard: info.num_expected_chunks_per_shard,
        }
    }
}

/// Converts EpochValidatorInfo into a vector of ValidatorProductionStats.
fn get_validator_epoch_stats(
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
    use near_chain::test_utils::{KeyValueRuntime, MockEpochManager, ValidatorSchedule};
    use near_chain::types::ChainConfig;
    use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
    use near_epoch_manager::shard_tracker::ShardTracker;
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
        let config = ClientConfig::test(false, 1230, 2340, 50, false, true, true, true);
        let info_helper = InfoHelper::new(None, &config, None);

        let store = near_store::test_utils::create_test_store();
        let vs =
            ValidatorSchedule::new().block_producers_per_epoch(vec![vec!["test".parse().unwrap()]]);
        let epoch_manager = MockEpochManager::new_with_validators(store.clone(), vs, 123);
        let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
        let runtime = KeyValueRuntime::new(store, epoch_manager.as_ref());
        let chain_genesis = ChainGenesis {
            time: StaticClock::utc(),
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
        let chain = Chain::new(
            epoch_manager,
            shard_tracker,
            runtime,
            &chain_genesis,
            doomslug_threshold_mode,
            ChainConfig::test(),
            None,
        )
        .unwrap();

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
                tier1_connections: vec![],
                tier1_accounts_keys: vec![],
                tier1_accounts_data: vec![],
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
