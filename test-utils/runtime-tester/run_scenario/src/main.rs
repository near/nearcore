use clap::{App, Arg};
use runtime_tester::Scenario;

use std::path::Path;
use std::time::{Duration, Instant};

fn main() {
    let matches = App::new("Run scenario")
        .arg(
            Arg::with_name("scenario")
                .long("scenario")
                .required(true)
                .takes_value(true)
                .help("Scenario to run"),
        )
        .get_matches();
    let path = Path::new(matches.value_of("scenario").unwrap());
    let scenario = Scenario::from_file(path).expect("Failed to deserialize the scenario file.");
    let starting_time = Instant::now();
    let runtime_stats = scenario.run().expect("Error while running scenario");
    println!("Time to run: {:?}", starting_time.elapsed());
    for block_stats in runtime_stats.blocks_stats {
        if block_stats.block_production_time > Duration::from_secs(1) {
            println!(
                "Time to produce block {} is {:?}",
                block_stats.height, block_stats.block_production_time
            );
        }
    }
}
