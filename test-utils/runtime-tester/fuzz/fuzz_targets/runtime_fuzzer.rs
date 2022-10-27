#![no_main]
use libfuzzer_sys::fuzz_target;
use runtime_tester::Scenario;
use std::fs::File;
use std::time::Duration;

fn do_fuzz(scenario: &Scenario) -> Result<(), String> {
    let stats = scenario.run().result.map_err(|e| e.to_string())?;
    for block_stats in stats.blocks_stats {
        if block_stats.block_production_time > Duration::from_secs(2) {
            return Err(format!(
                "block at height {} was produced in {:?}",
                block_stats.height, block_stats.block_production_time
            ));
        }
    }
    Ok(())
}

fn fuzz(scenario: Scenario) {
    if let Err(err) = do_fuzz(&scenario) {
        let file = "failed_scenario.json";
        serde_json::to_writer(&File::create(file).unwrap(), &scenario).unwrap();
        panic!("Bad scenario: {}, {}", file, err);
    }
}

fuzz_target!(|scenario: Scenario| { fuzz(scenario) });
