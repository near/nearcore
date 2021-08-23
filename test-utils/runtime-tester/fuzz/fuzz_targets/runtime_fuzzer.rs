#![no_main]
use libfuzzer_sys::fuzz_target;
use runtime_tester::Scenario;
use std::fs::File;
use std::time::Duration;

fuzz_target!(|scenario: Scenario| {
    let filename = "failed_scenario.json";
    let runtime_stats = scenario.run();
    if let Err(e) = runtime_stats {
        serde_json::to_writer(&File::create(filename).unwrap(), &scenario).unwrap();
        panic!("Bad scenario: {}, Error: {}", filename, e);
    } else {
        let stats = runtime_stats.unwrap();
        for block_stats in stats.blocks_stats {
            if block_stats.block_production_time > Duration::from_secs(1) {
                serde_json::to_writer(&File::create(filename).unwrap(), &scenario).unwrap();
                panic!(
                    "Bad scenario: {}, block at height {} was produced in {:?}",
                    filename, block_stats.height, block_stats.block_production_time
                );
            }
        }
    }
});
