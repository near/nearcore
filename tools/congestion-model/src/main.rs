use congestion_model::strategy::{GlobalTxStopShard, NoQueueShard, SimpleBackpressure};
use congestion_model::workload::{
    AllForOneProducer, BalancedProducer, LinearImbalanceProducer, Producer,
};
use congestion_model::{summary_table, CongestionStrategy, Model, PGAS};

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
}

fn main() {
    let args = Args::parse();

    summary_table::print_summary_header();

    let workload_names = parse_workflow_names(args.workload.as_ref());
    let strategy_names = parse_strategy_names(args.strategy.as_ref());

    for workload_name in &workload_names {
        for strategy_name in &strategy_names {
            run_model(&strategy_name, &workload_name, args.shards, args.rounds);
        }
    }
}

fn run_model(strategy_name: &str, workload_name: &str, num_shards: usize, num_rounds: usize) {
    let strategy = strategy(strategy_name, num_shards);
    let workload = workload(workload_name);
    let mut model = Model::new(strategy, workload);
    for _ in 0..num_rounds {
        model.step();
    }
    summary_table::print_summary_row(&model, workload_name, strategy_name);
}

fn normalize_cmdline_arg(value: &str) -> String {
    value.to_lowercase().replace(" ", "")
}

// Add workloads here to simulate them with `cargo run`.
fn workload(workload_name: &str) -> Box<dyn Producer> {
    match workload_name {
        "Balanced" => Box::<BalancedProducer>::default(),
        "All To One" => Box::<AllForOneProducer>::default(),
        "Linear Imbalance" => Box::<LinearImbalanceProducer>::default(),
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
            "Global TX stop" => Box::<GlobalTxStopShard>::default() as Box<dyn CongestionStrategy>,
            "Simple backpressure" => {
                Box::<SimpleBackpressure>::default() as Box<dyn CongestionStrategy>
            }
            _ => panic!("unknown strategy: {}", strategy_name),
        };

        result.push(strategy);
    }
    result
}

fn parse_workflow_names(workflow_name: &str) -> Vec<String> {
    let available: Vec<String> =
        vec!["Balanced".to_string(), "All To One".to_string(), "Linear Imbalance".to_string()];

    if workflow_name == "all" {
        return available;
    }

    for name in &available {
        if normalize_cmdline_arg(name.as_ref()) == normalize_cmdline_arg(workflow_name) {
            return vec![name.to_string()];
        }
    }
    panic!("The requested workflow name did not match any available workflows. Requested workflow name {:?}, The available workflows are: {:?}", workflow_name, available);
}

fn parse_strategy_names(strategy_name: &str) -> Vec<String> {
    let available: Vec<String> = vec![
        "No queues".to_string(),
        "Global TX stop".to_string(),
        "Simple backpressure".to_string(),
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
        println!("    {:>6} receipts incoming", queues[shard_id].incoming_receipts);
        println!("    {:>6} receipts queued", queues[shard_id].queued_receipts);
    }
}
