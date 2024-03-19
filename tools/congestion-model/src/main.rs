use chrono::Utc;
use congestion_model::strategy::{GlobalTxStopShard, NewTxLast, NoQueueShard, SimpleBackpressure};
use congestion_model::workload::{
    AllForOneProducer, BalancedProducer, LinearImbalanceProducer, Producer,
};
use congestion_model::{
    summary_table, CongestionStrategy, Model, ShardQueueLengths, StatsWriter, PGAS,
};
use std::time::Duration;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value = "4")]
    shards: usize,
    #[clap(short, long, default_value = "1000")]
    rounds: usize,

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
}

fn main() {
    let args = Args::parse();

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

            run_model(&strategy_name, &workload_name, args.shards, args.rounds, stats_writer);
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
    mut stats_writer: StatsWriter,
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

    model.write_stats_header(&mut stats_writer);

    for round in 0..num_rounds {
        model.write_stats_values(&mut stats_writer, start_time, round);
        model.step();
        max_queues = max_queues.max_component_wise(&model.max_queue_length());
    }
    summary_table::print_summary_row(
        workload_name,
        strategy_name,
        &model.progress(),
        &model.gas_throughput(),
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
        "All To One" => Box::new(AllForOneProducer::one_hop_only()),
        "Indirect All To One" => Box::<AllForOneProducer>::default(),
        "Linear Imbalance" => Box::<LinearImbalanceProducer>::default(),
        "Big Linear Imbalance" => Box::new(LinearImbalanceProducer::big_receipts()),
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
            "New TX last" => Box::<NewTxLast>::default(),
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
        "All To One".to_string(),
        "Indirect All To One".to_string(),
        "Linear Imbalance".to_string(),
        "Big Linear Imbalance".to_string(),
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
        "New TX last".to_string(),
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
