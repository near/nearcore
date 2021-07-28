#![no_main]
use libfuzzer_sys::fuzz_target;
use runtime_tester::Scenario;
use std::fs::File;

fuzz_target!(|scenario: Scenario| {
    let filename = "failed_scenario.json";
    let runtime_stats = scenario.run();
    if let Err(e) = runtime_stats {
        serde_json::to_writer(&File::create(filename).unwrap(), &scenario).unwrap();
        panic!("Bad scenario: {}, Error: {}", filename, e);
    }
});
