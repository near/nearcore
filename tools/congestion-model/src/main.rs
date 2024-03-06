use congestion_model::strategy::{GlobalTxStopShard, NoQueueShard, SimpleBackpressure};
use congestion_model::workload::{AllForOneProducer, BalancedProducer, Producer};
use congestion_model::{summary_table, CongestionStrategy, Model, PGAS};

fn main() {
    let num_shards = 4;
    let num_rounds = 1000;

    summary_table::print_summary_header();
    for workload_index in 0..NUM_WORKLOADS {
        for design_index in 0..NUM_STRATEGIES {
            let (design_name, design) = strategy(num_shards, design_index);
            let (workload_name, workload) = workload(workload_index);
            let mut model = Model::new(design, workload);
            for _ in 0..num_rounds {
                model.step();
            }
            summary_table::print_summary_row(&model, workload_name, design_name);
        }
    }
}

// Add workloads here to simulate them with `cargo run`.
const NUM_WORKLOADS: usize = 2;
fn workload(i: usize) -> (&'static str, Box<dyn Producer>) {
    match i {
        NUM_WORKLOADS.. => panic!(),
        0 => ("Balanced", Box::<BalancedProducer>::default()),
        1 => ("All to one", Box::<AllForOneProducer>::default()),
    }
}

// Add strategies here to simulate them with `cargo run`.
const NUM_STRATEGIES: usize = 3;
fn strategy(num_shards: usize, i: usize) -> (&'static str, Vec<Box<dyn CongestionStrategy>>) {
    let mut shards = vec![];
    for _ in 0..num_shards {
        let shard = match i {
            NUM_STRATEGIES.. => panic!(),
            0 => Box::new(NoQueueShard {}) as Box<dyn CongestionStrategy>,
            1 => Box::<GlobalTxStopShard>::default() as Box<dyn CongestionStrategy>,
            2 => Box::<SimpleBackpressure>::default() as Box<dyn CongestionStrategy>,
        };
        shards.push(shard);
    }
    let name = match i {
        NUM_STRATEGIES.. => panic!(),
        0 => "No queues",
        1 => "Global TX stop",
        2 => "Simple backpressure",
    };
    (name, shards)
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
