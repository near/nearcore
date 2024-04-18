use bytesize::ByteSize;
use chrono::Utc;
use clap::Parser;
use congestion_model::strategy::{
    FancyGlobalTransactionStop, GlobalTxStopShard, NepStrategy, NewTxLast, NoQueueShard,
    SimpleBackpressure, SmoothTrafficLight, TrafficLight,
};
use congestion_model::workload::{
    AllForOneProducer, BalancedProducer, FairnessBenchmarkProducer, LinearImbalanceProducer,
    Producer,
};
use congestion_model::{
    summary_table, CongestionStrategy, Model, ShardQueueLengths, StatsWriter, PGAS, TGAS,
};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{self, Layer};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value = "4")]
    shards: usize,
    #[clap(short, long, default_value = "1000")]
    rounds: usize,

    /// Warmup rounds do not count towards total gas in the summary table. CSV
    /// writer output is not affected.
    #[clap(short, long, default_value_t = 0)]
    warmup: usize,

    /// Can be used to select a single workload or "all" to run all workload.
    /// It's case insensitive and spaces are stripped.
    /// Example: "all", "balanced", "all to one", "AllToOne".
    #[clap(long, default_value = "all")]
    workload: String,

    /// Can be used to select a single strategy or "all" to run all strategies.
    #[clap(long, default_value = "all")]
    strategy: String,

    /// If enabled the model will write stats into a csv file that can be used
    /// to visualize the evaluation of the model over time.
    #[clap(long, default_value = "false")]
    write_stats: bool,

    /// Optional path the file where the stats should be saved. By default
    /// the stats will be saved to a file name with prefix "stats", the strategy
    /// and workload name concatenated and ".csv" extension. This option can
    /// only be used when a single strategy and a single workflow are selected
    /// otherwise the stats from different evaluations would overwrite each
    /// other.
    #[clap(long)]
    write_stats_filepath: Option<String>,
    /// At most N transactions can stay in the transaction pool and the remainder is rejected.
    ///
    /// This can be useful to look at transaction delays.
    #[clap(long, default_value_t = usize::MAX)]
    tx_pool_size: usize,
}

fn main() {
    let args = Args::parse();

    let filter = tracing_subscriber::EnvFilter::from_default_env();
    let layer = tracing_subscriber::fmt::layer().with_filter(filter);
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::set_global_default(subscriber).expect("could not set a global subscriber");

    summary_table::print_summary_header();

    let workload_names = parse_workload_names(args.workload.as_ref());
    let strategy_names = parse_strategy_names(args.strategy.as_ref());

    if args.write_stats_filepath.is_some()
        && (workload_names.len() != 1 || strategy_names.len() != 1)
    {
        panic!("write_stats_filepath can only be used with single workload and strategy. Parsed {:?} workloads and {:?} strategies. ", workload_names, strategy_names);
    }

    for workload_name in &workload_names {
        for strategy_name in &strategy_names {
            let stats_writer = parse_stats_writer(
                args.write_stats,
                args.write_stats_filepath.clone(),
                workload_name,
                strategy_name,
            );

            run_model(
                &strategy_name,
                &workload_name,
                args.shards,
                args.rounds,
                args.warmup,
                stats_writer,
                args.tx_pool_size,
            );
        }
    }
}

fn parse_stats_writer(
    write_stats: bool,
    write_stats_filepath: Option<String>,
    workload_name: &String,
    strategy_name: &String,
) -> StatsWriter {
    if !write_stats {
        return None;
    }

    let default_path = format!("stats_{}_{}.csv", workload_name, strategy_name);
    let path = write_stats_filepath.unwrap_or(default_path);
    let stats_writer = Box::new(csv::Writer::from_path(path).unwrap());
    Some(stats_writer)
}

fn run_model(
    strategy_name: &str,
    workload_name: &str,
    num_shards: usize,
    num_rounds: usize,
    num_warmup_rounds: usize,
    mut stats_writer: StatsWriter,
    tx_pool_size: usize,
) {
    let strategy = strategy(strategy_name, num_shards);
    let workload = workload(workload_name);
    let mut model = Model::new(strategy, workload);
    let mut max_queues = ShardQueueLengths::default();

    // Set the start time to an half hour ago to make it visible by default in
    // grafana. Each round is 1 virtual second so hald an hour is good for
    // looking at a maximum of 1800 rounds, beyond that you'll need to customize
    // the grafana time range.
    let start_time = Utc::now() - Duration::from_secs(1 * 60 * 60);
    let mut warmup_gas_usage = model.gas_throughput();

    model.write_stats_header(&mut stats_writer);

    for round in 0..num_rounds {
        if round == num_warmup_rounds {
            warmup_gas_usage = model.gas_throughput();
        }
        model.write_stats_values(&mut stats_writer, start_time, round);
        model.step();
        model.trim_transaction_pools(tx_pool_size);
        max_queues = max_queues.max_component_wise(&model.max_queue_length());
    }
    summary_table::print_summary_row(
        workload_name,
        strategy_name,
        &model.progress(),
        &((model.gas_throughput() - warmup_gas_usage)
            / (num_rounds - num_warmup_rounds)
            / num_shards),
        &max_queues,
        &model.user_experience(),
    );
}

fn normalize_cmdline_arg(value: &str) -> String {
    value.to_lowercase().replace(" ", "")
}

// Add workloads here to simulate them with `cargo run`.
fn workload(workload_name: &str) -> Box<dyn Producer> {
    match workload_name {
        "Balanced" => Box::<BalancedProducer>::default(),
        "Increasing Size" => {
            // Transform the tx to a small local receipt which produces 3 large receipts to another shard.
            Box::new(BalancedProducer::with_sizes_and_fan_out(vec![100, 1_000_000], 3))
        }
        "Extreme Increasing Size" => {
            // Produce 50 big receipts instead of 3 as in "Increasing Size"
            Box::new(BalancedProducer::with_sizes_and_fan_out(vec![100, 2_000_000], 10))
        }
        "Shard War" => {
            // Each shard transforms one local tx into 4^3 = 64 receipts of 100kB to another shard
            Box::new(BalancedProducer::with_sizes_and_fan_out(vec![100, 100, 100, 100_000], 4))
        }
        "Mixed All To One" => Box::<AllForOneProducer>::default(),
        "Indirect All To One" => Box::new(AllForOneProducer::new(false, true, true)),
        "One Hop All To One" => Box::new(AllForOneProducer::new(true, false, false)),
        "Two Hop All To One" => Box::new(AllForOneProducer::new(false, true, false)),
        "Three Hop All To One" => Box::new(AllForOneProducer::new(false, false, true)),
        "Relayed Hot" => Box::new(AllForOneProducer::hot_tg()),
        "Linear Imbalance" => Box::<LinearImbalanceProducer>::default(),
        "Big Linear Imbalance" => Box::new(LinearImbalanceProducer::big_receipts()),
        "Fairness Test" => Box::<FairnessBenchmarkProducer>::default(),
        _ => panic!("unknown workload: {}", workload_name),
    }
}

// Add strategies here to simulate them with `cargo run`.
// Returns a vector of strategies, one for each shard.
fn strategy(strategy_name: &str, num_shards: usize) -> Vec<Box<dyn CongestionStrategy>> {
    let mut result = vec![];
    for _ in 0..num_shards {
        let strategy = match strategy_name {
            "No queues" => Box::new(NoQueueShard {}) as Box<dyn CongestionStrategy>,
            "Global TX stop" => Box::<GlobalTxStopShard>::default(),
            "Simple backpressure" => Box::<SimpleBackpressure>::default(),
            "Fancy Stop" => Box::<FancyGlobalTransactionStop>::default(),
            "New TX last" => Box::<NewTxLast>::default(),
            "Traffic Light" => Box::<TrafficLight>::default(),
            "Smooth Traffic Light" => Box::<SmoothTrafficLight>::default(),
            // Trade essentially unbounded outgoing delays for higher
            // utilization. If run for long enough, it will still fill the
            // buffer and hit a memory limit, but full throughput can be
            // sustained for a long time this way.
            "STL_MAX_UTIL" => Box::new(
                SmoothTrafficLight::default()
                    .with_smooth_slow_down(false)
                    .with_gas_limits(50 * PGAS, u64::MAX)
                    .with_tx_reject_threshold(0.125),
            ),
            "STL_HIGH_UTIL" => Box::new(
                SmoothTrafficLight::default()
                    .with_smooth_slow_down(false)
                    .with_gas_limits(50 * PGAS, 50 * PGAS)
                    .with_tx_reject_threshold(0.125),
            ),
            // Keep queues short enough that the can be emptied in one round.
            "STL_MIN_DELAY" => Box::new(
                SmoothTrafficLight::default()
                    .with_gas_limits(1300 * TGAS, 1 * PGAS)
                    .with_tx_reject_threshold(0.95),
            ),
            "STL_LOW_DELAY" => Box::new(
                SmoothTrafficLight::default()
                    .with_gas_limits(5 * PGAS, 1 * PGAS)
                    .with_tx_reject_threshold(0.5),
            ),
            "NEP" => Box::<NepStrategy>::default(),
            "NEP 200MB" => Box::new(
                NepStrategy::default().with_memory_limits(ByteSize::mb(100), ByteSize::mb(100)),
            ),
            "NEP 450/50MB" => Box::new(
                // keep outgoing limit small
                // (1) if we hit this, it's due to another shard's incoming congestion,
                //     so we are already in a second stage of congestion and should be more aggressive
                // (2) this soft limit will be breached quite a bit anyway
                //     as we don't stop executing receipts
                NepStrategy::default().with_memory_limits(ByteSize::mb(450), ByteSize::mb(50)),
            ),
            "NEP 1GB" => Box::new(
                NepStrategy::default().with_memory_limits(ByteSize::mb(500), ByteSize::mb(500)),
            ),
            "NEP 10 Pgas" => Box::new(NepStrategy::default().with_gas_limits(10 * PGAS, 10 * PGAS)),
            "NEP 1 Pgas" => Box::new(NepStrategy::default().with_gas_limits(10 * PGAS, 10 * PGAS)),
            "NEP 10/1 Pgas" => {
                Box::new(NepStrategy::default().with_gas_limits(10 * PGAS, 1 * PGAS))
            }
            // NEP v2 takes results from memory and gas limits into account and fixes those
            "NEPv2" => Box::new(
                NepStrategy::default()
                    .with_gas_limits(10 * PGAS, 1 * PGAS)
                    .with_memory_limits(ByteSize::mb(450), ByteSize::mb(50)),
            ),
            "NEPv2 1GB" => Box::new(
                NepStrategy::default()
                    .with_gas_limits(10 * PGAS, 1 * PGAS)
                    .with_memory_limits(ByteSize::mb(900), ByteSize::mb(100)),
            ),
            "NEPv2 early global stop" => Box::new(
                NepStrategy::default()
                    .with_gas_limits(10 * PGAS, 1 * PGAS)
                    .with_memory_limits(ByteSize::mb(450), ByteSize::mb(50))
                    .with_global_stop_limit(0.5),
            ),
            "NEPv2 late global stop" => Box::new(
                NepStrategy::default()
                    .with_gas_limits(10 * PGAS, 1 * PGAS)
                    .with_memory_limits(ByteSize::mb(450), ByteSize::mb(50))
                    .with_global_stop_limit(1.0),
            ),
            "NEPv2 less forwarding" => Box::new(
                NepStrategy::default()
                    .with_gas_limits(10 * PGAS, 1 * PGAS)
                    .with_memory_limits(ByteSize::mb(450), ByteSize::mb(50))
                    .with_send_gas_limit_range(PGAS / 2, 2 * PGAS),
            ),
            "NEPv2 more forwarding" => Box::new(
                NepStrategy::default()
                    .with_gas_limits(10 * PGAS, 1 * PGAS)
                    .with_memory_limits(ByteSize::mb(450), ByteSize::mb(50))
                    .with_send_gas_limit_range(PGAS / 2, 100 * PGAS),
            ),
            "NEPv2 less tx" => Box::new(
                NepStrategy::default()
                    .with_gas_limits(10 * PGAS, 1 * PGAS)
                    .with_memory_limits(ByteSize::mb(450), ByteSize::mb(50))
                    .with_tx_gas_limit_range(0, 100 * TGAS),
            ),
            "NEPv2 more tx" => Box::new(
                NepStrategy::default()
                    .with_gas_limits(10 * PGAS, 1 * PGAS)
                    .with_memory_limits(ByteSize::mb(450), ByteSize::mb(50))
                    .with_tx_gas_limit_range(5 * TGAS, 900 * TGAS),
            ),
            // NEP v3 takes results from v2 into account, and lots of further fine-tuning.
            // Unfortunately, no configuration can pass the fairness test in a satisfying way.
            "NEPv3" => Box::new(
                NepStrategy::default()
                    // small outgoing buffers is great for low latency
                    .with_gas_limits(10 * PGAS, 1 * PGAS)
                    .with_memory_limits(ByteSize::mb(500), ByteSize::mb(50))
                    // going to zero is generally better in this strategy
                    .with_tx_gas_limit_range(0, 500 * TGAS)
                    .with_send_gas_limit_range(0, 5 * PGAS)
                    .with_global_stop_limit(0.95),
            ),
            _ => panic!("unknown strategy: {}", strategy_name),
        };

        result.push(strategy);
    }
    result
}

fn parse_workload_names(workload_name: &str) -> Vec<String> {
    let available: Vec<String> = vec![
        "Balanced".to_string(),
        "Increasing Size".to_string(),
        "Extreme Increasing Size".to_string(),
        "Shard War".to_string(),
        "Mixed All To One".to_string(),
        "Indirect All To One".to_string(),
        "One Hop All To One".to_string(),
        "Two Hop All To One".to_string(),
        "Three Hop All To One".to_string(),
        "Relayed Hot".to_string(),
        "Linear Imbalance".to_string(),
        "Big Linear Imbalance".to_string(),
        "Fairness Test".to_string(),
    ];

    if workload_name == "all" {
        return available;
    }

    for name in &available {
        if normalize_cmdline_arg(name.as_ref()) == normalize_cmdline_arg(workload_name) {
            return vec![name.to_string()];
        }
    }
    panic!("The requested workload name did not match any available workloads. Requested workload name {:?}, The available workloads are: {:?}", workload_name, available);
}

fn parse_strategy_names(strategy_name: &str) -> Vec<String> {
    let available: Vec<String> = vec![
        "No queues".to_string(),
        "Global TX stop".to_string(),
        "Simple backpressure".to_string(),
        "Fancy Stop".to_string(),
        "New TX last".to_string(),
        "Traffic Light".to_string(),
        "Smooth Traffic Light".to_string(),
        "STL_MAX_UTIL".to_string(),
        "STL_HIGH_UTIL".to_string(),
        "STL_MIN_DELAY".to_string(),
        "STL_LOW_DELAY".to_string(),
        "NEP".to_string(),
        "NEP 200MB".to_string(),
        "NEP 450/50MB".to_string(),
        "NEP 1GB".to_string(),
        "NEP 10 Pgas".to_string(),
        "NEP 1 Pgas".to_string(),
        "NEP 10/1 Pgas".to_string(),
        "NEPv2".to_string(),
        "NEPv2 1GB".to_string(),
        "NEPv2 early global stop".to_string(),
        "NEPv2 late global stop".to_string(),
        "NEPv2 less forwarding".to_string(),
        "NEPv2 more forwarding".to_string(),
        "NEPv2 less tx".to_string(),
        "NEPv2 more tx".to_string(),
        "NEPv3".to_string(),
    ];

    if strategy_name == "all" {
        return available;
    }

    for name in &available {
        if normalize_cmdline_arg(name.as_ref()) == normalize_cmdline_arg(strategy_name) {
            return vec![name.to_string()];
        }
    }
    panic!("The requested strategy name did not match any available strategies. Requested strategy name {:?}, The available strategies are: {:?}", strategy_name, available);
}

// for looking at more details during execution, call print_report
#[allow(dead_code)]
fn print_report(model: &Model) {
    let queues = model.queue_lengths();
    let throughput = model.gas_throughput();
    let progress = model.progress();

    println!("burnt {} PGas", throughput.total / PGAS,);
    println!("{:>6} transactions finished", progress.finished_transactions);
    println!("{:>6} transactions waiting", progress.waiting_transactions);
    println!("{:>6} transactions pending", progress.pending_transactions);
    println!("{:>6} transactions failed", progress.failed_transactions);
    for shard_id in model.shard_ids() {
        println!("SHARD {shard_id}");
        println!("    {:>6} receipts incoming", queues[shard_id].incoming_receipts.num);
        println!("    {:>6} receipts queued", queues[shard_id].queued_receipts.num);
    }
}
